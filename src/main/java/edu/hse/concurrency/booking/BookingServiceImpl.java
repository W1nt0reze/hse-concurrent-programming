package edu.hse.concurrency.booking;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class BookingServiceImpl implements BookingService {
    private final Hall hall;
    private final Duration holdDuration;
    private final SeatState[] states;
    private final Object lifecycle = new Object();
    private final Set<Waiter> waiters = new HashSet<>();
    private volatile boolean stopped;
    private int activeCalls;
    private long nextId;

    public BookingServiceImpl(Hall hall, Duration holdDuration) {
        this.hall = Objects.requireNonNull(hall, "hall");
        this.holdDuration = Objects.requireNonNull(holdDuration, "holdDuration");
        if (holdDuration.isZero() || holdDuration.isNegative()) {
            throw new IllegalArgumentException("holdDuration must be positive");
        }
        states = new SeatState[hall.capacity()];
        Arrays.setAll(states, i -> new SeatState());
    }

    @Override
    public Optional<Hold> hold(String clientId, Set<Seat> seats) {
        Set<Seat> copy = Set.copyOf(Objects.requireNonNull(seats, "seats"));
        int[] indices = prepareSeats(clientId, copy);
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Booking service is stopped");
            }
            activeCalls++;
        }
        try {
            return withSeats(indices, 0, () -> {
                Instant now = Instant.now();
                for (int index : indices) {
                    if (states[index].status(now) != SeatStatus.FREE) {
                        return Optional.empty();
                    }
                }
                return Optional.of(createHold(clientId, copy, indices, now));
            });
        } finally {
            synchronized (lifecycle) {
                if (--activeCalls == 0) {
                    lifecycle.notifyAll();
                }
            }
        }
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Set<Seat> copy = Set.copyOf(Objects.requireNonNull(seats, "seats"));
        int[] indices = prepareSeats(clientId, copy);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        Waiter waiter = new Waiter();
        long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException e) {
            timeoutNanos = Long.MAX_VALUE;
        }
        final long budget = timeoutNanos;
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Booking service is stopped");
            }
            activeCalls++;
            waiters.add(waiter);
        }
        long started = System.nanoTime();
        try {
            boolean firstAttempt = true;
            while (true) {
                boolean first = firstAttempt;
                Attempt attempt = withSeats(indices, 0, () -> {
                    synchronized (waiter) {
                        waiter.signalled = false;
                    }
                    long checkedAt = System.nanoTime();
                    long remaining = budget - (checkedAt - started);
                    if (stopped || Thread.currentThread().isInterrupted()
                            || (remaining <= 0 && (!first || budget > 0))) {
                        return new Attempt(null, true, 0);
                    }
                    Instant now = Instant.now();
                    boolean free = true;
                    long delay = Math.max(0, remaining);
                    for (int index : indices) {
                        SeatState state = states[index];
                        SeatStatus status = state.status(now);
                        if (status == SeatStatus.SOLD) {
                            return new Attempt(null, true, 0);
                        }
                        if (status == SeatStatus.HELD) {
                            free = false;
                            long untilExpiration;
                            try {
                                untilExpiration = Duration.between(now, state.hold.expiresAt()).toNanos();
                            } catch (ArithmeticException e) {
                                untilExpiration = Long.MAX_VALUE;
                            }
                            delay = Math.min(delay, untilExpiration);
                        }
                    }
                    if (free) {
                        if (stopped || Thread.currentThread().isInterrupted()) {
                            return new Attempt(null, true, 0);
                        }
                        return new Attempt(createHold(clientId, copy, indices, now), true, 0);
                    }
                    if (remaining <= 0) {
                        return new Attempt(null, true, 0);
                    }
                    for (int index : indices) {
                        SeatState state = states[index];
                        if (state.waiters == null) {
                            state.waiters = new HashSet<>();
                        }
                        state.waiters.add(waiter);
                    }
                    return new Attempt(null, false, checkedAt + delay);
                });
                // Once committed, an interrupt belongs to the caller's next operation.
                if (attempt.hold != null) {
                    return Optional.of(attempt.hold);
                }
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (attempt.done) {
                    return Optional.empty();
                }
                synchronized (waiter) {
                    if (!waiter.signalled && !stopped) {
                        long remaining = attempt.wakeAt - System.nanoTime();
                        if (remaining > 0) {
                            waiter.wait(remaining / 1_000_000, (int) (remaining % 1_000_000));
                        }
                    }
                }
                firstAttempt = false;
            }
        } finally {
            withSeats(indices, 0, () -> {
                for (int index : indices) {
                    Set<Waiter> registered = states[index].waiters;
                    if (registered != null) {
                        registered.remove(waiter);
                        if (registered.isEmpty()) {
                            states[index].waiters = null;
                        }
                    }
                }
                return null;
            });
            synchronized (lifecycle) {
                waiters.remove(waiter);
                if (--activeCalls == 0) {
                    lifecycle.notifyAll();
                }
            }
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        Objects.requireNonNull(hold, "hold");
        int[] indices = prepareSeats(hold.clientId(), hold.seats());
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Booking service is stopped");
            }
            activeCalls++;
        }
        try {
            return withSeats(indices, 0, () -> {
                if (!Instant.now().isBefore(hold.expiresAt())) {
                    return false;
                }
                for (int index : indices) {
                    if (states[index].hold != hold || states[index].sold) {
                        return false;
                    }
                }
                for (int index : indices) {
                    SeatState state = states[index];
                    state.hold = null;
                    state.sold = true;
                    if (state.waiters != null) {
                        for (Waiter waiter : state.waiters) {
                            waiter.signal();
                        }
                    }
                }
                return true;
            });
        } finally {
            synchronized (lifecycle) {
                if (--activeCalls == 0) {
                    lifecycle.notifyAll();
                }
            }
        }
    }

    @Override
    public boolean release(Hold hold) {
        Objects.requireNonNull(hold, "hold");
        int[] indices = prepareSeats(hold.clientId(), hold.seats());
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Booking service is stopped");
            }
            activeCalls++;
        }
        try {
            return withSeats(indices, 0, () -> {
                if (!Instant.now().isBefore(hold.expiresAt())) {
                    return false;
                }
                for (int index : indices) {
                    if (states[index].hold != hold || states[index].sold) {
                        return false;
                    }
                }
                for (int index : indices) {
                    SeatState state = states[index];
                    state.hold = null;
                    if (state.waiters != null) {
                        for (Waiter waiter : state.waiters) {
                            waiter.signal();
                        }
                    }
                }
                return true;
            });
        } finally {
            synchronized (lifecycle) {
                if (--activeCalls == 0) {
                    lifecycle.notifyAll();
                }
            }
        }
    }

    @Override
    public SeatStatus status(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall: " + seat);
        }
        int index = (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
        synchronized (states[index]) {
            return states[index].status(Instant.now());
        }
    }

    @Override
    public void shutdown() {
        boolean interrupted = false;
        synchronized (lifecycle) {
            stopped = true;
            for (Waiter waiter : waiters) {
                waiter.signal();
            }
            while (activeCalls != 0) {
                try {
                    lifecycle.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Hold createHold(String clientId, Set<Seat> seats, int[] indices, Instant now) {
        Instant expiresAt;
        try {
            expiresAt = now.plus(holdDuration);
        } catch (DateTimeException | ArithmeticException e) {
            expiresAt = Instant.MAX;
        }
        long id;
        synchronized (lifecycle) {
            id = ++nextId;
        }
        Hold hold = new Hold(id, clientId, seats, expiresAt);
        for (int index : indices) {
            states[index].hold = hold;
        }
        return hold;
    }

    private int[] prepareSeats(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        if (clientId.isBlank() || seats.isEmpty() || seats.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("Expected a client and 1-10 seats");
        }
        int[] result = new int[seats.size()];
        int offset = 0;
        for (Seat seat : seats) {
            if (!hall.contains(seat)) {
                throw new IllegalArgumentException("Seat is outside the hall: " + seat);
            }
            result[offset++] = (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
        }
        Arrays.sort(result);
        return result;
    }

    private <T> T withSeats(int[] indices, int offset, Action<T> action) {
        if (offset == indices.length) {
            return action.run();
        }
        synchronized (states[indices[offset]]) {
            return withSeats(indices, offset + 1, action);
        }
    }

    private interface Action<T> {
        T run();
    }

    private record Attempt(Hold hold, boolean done, long wakeAt) { }

    private static final class Waiter {
        private boolean signalled;

        private synchronized void signal() {
            signalled = true;
            notifyAll();
        }
    }

    private static final class SeatState {
        private Hold hold;
        private boolean sold;
        private Set<Waiter> waiters;

        private SeatStatus status(Instant now) {
            if (sold) {
                return SeatStatus.SOLD;
            }
            return hold != null && now.isBefore(hold.expiresAt()) ? SeatStatus.HELD : SeatStatus.FREE;
        }
    }
}
