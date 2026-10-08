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
        Set<Seat> copy = validateRequest(clientId, seats);
        int[] indices = indices(copy);
        enter(null);
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
            leave(null);
        }
    }

    @Override
    public Optional<Hold> holdOrWait(String clientId, Set<Seat> seats, Duration timeout)
            throws InterruptedException {
        Set<Seat> copy = validateRequest(clientId, seats);
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        int[] indices = indices(copy);
        Waiter waiter = new Waiter();
        enter(waiter);
        long started = System.nanoTime();
        long budget = nanos(timeout);
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
                            delay = Math.min(delay, nanos(Duration.between(now, state.hold.expiresAt())));
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
                        timedWait(waiter, attempt.wakeAt - System.nanoTime());
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
            leave(waiter);
        }
    }

    @Override
    public boolean confirm(Hold hold) {
        return finish(hold, true);
    }

    @Override
    public boolean release(Hold hold) {
        return finish(hold, false);
    }

    private boolean finish(Hold hold, boolean sell) {
        Objects.requireNonNull(hold, "hold");
        Set<Seat> copy = validateRequest(hold.clientId(), hold.seats());
        int[] indices = indices(copy);
        enter(null);
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
                    state.sold = sell;
                    state.signalWaiters();
                }
                return true;
            });
        } finally {
            leave(null);
        }
    }

    @Override
    public SeatStatus status(Seat seat) {
        int index = index(seat);
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

    private Set<Seat> validateRequest(String clientId, Set<Seat> seats) {
        Objects.requireNonNull(clientId, "clientId");
        Set<Seat> copy = Set.copyOf(Objects.requireNonNull(seats, "seats"));
        if (clientId.isBlank() || copy.isEmpty() || copy.size() > MAX_SEATS_PER_HOLD) {
            throw new IllegalArgumentException("Expected a client and 1-10 seats");
        }
        for (Seat seat : copy) {
            index(seat);
        }
        return copy;
    }

    private int index(Seat seat) {
        Objects.requireNonNull(seat, "seat");
        if (!hall.contains(seat)) {
            throw new IllegalArgumentException("Seat is outside the hall: " + seat);
        }
        return (seat.row() - 1) * hall.seatsPerRow() + seat.number() - 1;
    }

    private int[] indices(Set<Seat> seats) {
        int[] result = new int[seats.size()];
        int offset = 0;
        for (Seat seat : seats) {
            result[offset++] = index(seat);
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

    private void enter(Waiter waiter) {
        synchronized (lifecycle) {
            if (stopped) {
                throw new IllegalStateException("Booking service is stopped");
            }
            activeCalls++;
            if (waiter != null) {
                waiters.add(waiter);
            }
        }
    }

    private void leave(Waiter waiter) {
        synchronized (lifecycle) {
            waiters.remove(waiter);
            if (--activeCalls == 0) {
                lifecycle.notifyAll();
            }
        }
    }

    private static long nanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    private static void timedWait(Object monitor, long nanos) throws InterruptedException {
        if (nanos > 0) {
            monitor.wait(nanos / 1_000_000, (int) (nanos % 1_000_000));
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

        private void signalWaiters() {
            if (waiters != null) {
                for (Waiter waiter : waiters) {
                    waiter.signal();
                }
            }
        }
    }
}
