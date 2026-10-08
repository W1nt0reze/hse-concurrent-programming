package edu.hse.concurrency.booking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class BookingConcurrencyTest {
    private final List<BookingService> services = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private static final Seat A = new Seat(1, 1);
    private static final Seat B = new Seat(1, 2);
    private static final Seat C = new Seat(1, 3);

    private BookingService service(Duration duration) {
        BookingService result = new BookingServiceImpl(new Hall(4, 10), duration);
        services.add(result);
        return result;
    }

    private BookingService service() {
        return service(Duration.ofMinutes(1));
    }

    @AfterEach
    void cleanup() throws InterruptedException {
        for (BookingService service : services) {
            service.shutdown();
        }
        for (Thread thread : threads) {
            thread.interrupt();
            thread.join(2_000);
            assertFalse(thread.isAlive());
        }
    }

    private <T> Pending<T> start(Callable<T> action) {
        FutureTask<T> task = new FutureTask<>(action);
        Thread thread = new Thread(task);
        threads.add(thread);
        thread.start();
        return new Pending<>(thread, task);
    }

    private static void awaitState(Thread thread, Thread.State state) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (thread.getState() != state && thread.isAlive() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(state, thread.getState());
    }

    @Test
    void onlyOneBuyerCanHoldTheSameSeats() throws Exception {
        BookingService service = service();
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(32)) {
            List<Future<Optional<Hold>>> results = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                int client = i;
                results.add(pool.submit(() -> {
                    start.await();
                    return service.hold("buyer-" + client, Set.of(new Seat(1, 1), new Seat(1, 2)));
                }));
            }
            start.countDown();
            int winners = 0;
            for (var result : results) {
                if (result.get(5, TimeUnit.SECONDS).isPresent()) {
                    winners++;
                }
            }
            assertEquals(1, winners);
        }
    }

    @Test
    void confirmationWinsExactlyOnce() throws Exception {
        BookingService service = service();
        Hold hold = service.hold("buyer", Set.of(A, B)).orElseThrow();
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(32)) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 128; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return service.confirm(hold);
                }));
            }
            start.countDown();
            int wins = 0;
            for (var result : results) {
                if (result.get(5, TimeUnit.SECONDS)) {
                    wins++;
                }
            }
            assertEquals(1, wins);
            assertEquals(SeatStatus.SOLD, service.status(A));
            assertEquals(SeatStatus.SOLD, service.status(B));
        }
    }

    @Test
    void confirmAndReleaseCannotBothSucceed() throws Exception {
        BookingService service = service();
        for (int round = 0; round < 100; round++) {
            BookingService current = round == 0 ? service : service();
            Hold hold = current.hold("buyer", Set.of(A, B)).orElseThrow();
            CountDownLatch start = new CountDownLatch(1);
            var confirm = start(() -> { start.await(); return current.confirm(hold); });
            var release = start(() -> { start.await(); return current.release(hold); });
            start.countDown();
            assertNotEquals(confirm.get(), release.get());
            assertEquals(current.status(A), current.status(B));
        }
    }

    @Test
    void overlappingSetsCompleteUnderLoad() throws Exception {
        BookingService service = service();
        try (var pool = Executors.newFixedThreadPool(24)) {
            List<Future<?>> results = new ArrayList<>();
            for (int worker = 0; worker < 24; worker++) {
                int id = worker;
                results.add(pool.submit(() -> {
                    for (int round = 0; round < 200; round++) {
                        Seat left = new Seat(1, id % 9 + 1);
                        Seat right = new Seat(1, id % 9 + 2);
                        Hold hold = service.holdOrWait("buyer-" + id,
                                Set.of(left, right), Duration.ofSeconds(5)).orElseThrow();
                        assertEquals(SeatStatus.HELD, service.status(left));
                        assertTrue(service.release(hold));
                    }
                    return null;
                }));
            }
            for (Future<?> result : results) {
                result.get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void releaseWakesTheBuyer() throws Exception {
        BookingService service = service();
        Hold original = service.hold("first", Set.of(A, B)).orElseThrow();
        var waiting = start(() -> service.holdOrWait("second", Set.of(A, B), Duration.ofSeconds(5)));
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        assertTrue(service.release(original));
        assertEquals(Set.of(A, B), waiting.get().orElseThrow().seats());
    }

    @Test
    void expirationWakesBuyerWithoutOtherServiceCalls() throws Exception {
        BookingService service = service(Duration.ofMillis(250));
        Hold original = service.hold("first", Set.of(A, B)).orElseThrow();
        var waiting = start(() -> service.holdOrWait("second", Set.of(A, B), Duration.ofSeconds(5)));
        Hold next = waiting.get().orElseThrow();
        assertFalse(Instant.now().isBefore(original.expiresAt()));
        assertNotEquals(original.id(), next.id());
        assertFalse(service.confirm(original));
        assertFalse(service.release(original));
    }

    @Test
    void waitsUntilEveryRequestedSeatIsFree() throws Exception {
        BookingService service = service();
        Hold first = service.hold("first", Set.of(A)).orElseThrow();
        Hold second = service.hold("second", Set.of(B)).orElseThrow();
        var waiting = start(() -> service.holdOrWait("third", Set.of(A, B), Duration.ofSeconds(5)));
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        assertTrue(service.release(first));
        assertFalse(waiting.task.isDone());
        assertEquals(SeatStatus.FREE, service.status(A));
        assertTrue(service.release(second));
        assertTrue(waiting.get().isPresent());
    }

    @Test
    void saleEndsAnExistingWait() throws Exception {
        BookingService service = service();
        Hold original = service.hold("first", Set.of(A)).orElseThrow();
        var waiting = start(() -> service.holdOrWait("second", Set.of(A, B), Duration.ofSeconds(30)));
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        assertTrue(service.confirm(original));
        assertTrue(waiting.get().isEmpty());
        assertEquals(SeatStatus.FREE, service.status(B));
    }

    @Test
    void soldSeatIsRejectedEvenIfAnotherSeatIsHeld() throws Exception {
        BookingService service = service();
        service.hold("first", Set.of(A));
        service.confirm(service.hold("second", Set.of(B)).orElseThrow());
        assertTrue(start(() -> service.holdOrWait("third", Set.of(A, B), Duration.ofDays(1)))
                .get().isEmpty());
    }

    @Test
    void timeoutDoesNotReserveTheFreePartOfASet() throws Exception {
        BookingService service = service();
        service.hold("first", Set.of(A));
        long start = System.nanoTime();
        assertTrue(service.holdOrWait("second", Set.of(A, B), Duration.ofMillis(150)).isEmpty());
        long elapsed = System.nanoTime() - start;
        assertTrue(elapsed >= TimeUnit.MILLISECONDS.toNanos(150));
        assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2));
        assertEquals(SeatStatus.FREE, service.status(B));
    }

    @Test
    void interruptLeavesNoReservation() throws Exception {
        BookingService service = service();
        Hold original = service.hold("first", Set.of(A)).orElseThrow();
        var waiting = start(() -> {
            assertThrows(InterruptedException.class,
                    () -> service.holdOrWait("second", Set.of(A, B), Duration.ofDays(1)));
            return null;
        });
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        waiting.thread.interrupt();
        waiting.get();
        assertEquals(SeatStatus.FREE, service.status(B));
        assertTrue(service.release(original));
        assertTrue(service.hold("third", Set.of(A, B)).isPresent());
    }

    @Test
    void alreadyInterruptedThreadCannotReserveFreeSeats() throws Exception {
        BookingService service = service();
        start(() -> {
            Thread.currentThread().interrupt();
            assertThrows(InterruptedException.class,
                    () -> service.holdOrWait("buyer", Set.of(A), Duration.ZERO));
            return null;
        }).get();
        assertEquals(SeatStatus.FREE, service.status(A));
    }

    @Test
    void shutdownWakesAllBuyersAndIsSafeToCallConcurrently() throws Exception {
        BookingService service = service();
        Hold original = service.hold("first", Set.of(A)).orElseThrow();
        List<Pending<Optional<Hold>>> buyers = new ArrayList<>();
        for (int i = 0; i < 24; i++) {
            buyers.add(start(() -> service.holdOrWait("buyer", Set.of(A), Duration.ofDays(1))));
        }
        for (var buyer : buyers) {
            awaitState(buyer.thread, Thread.State.TIMED_WAITING);
        }
        var stop1 = start(() -> { service.shutdown(); return null; });
        var stop2 = start(() -> { service.shutdown(); return null; });
        stop1.get();
        stop2.get();
        for (var buyer : buyers) {
            assertTrue(buyer.get().isEmpty());
        }
        assertThrows(IllegalStateException.class, () -> service.confirm(original));
        assertThrows(IllegalStateException.class, () -> service.release(original));
        assertEquals(SeatStatus.HELD, service.status(A));
    }

    @Test
    void statusStillAccountsForExpirationAfterShutdown() throws Exception {
        BookingService service = service(Duration.ofMillis(100));
        service.hold("buyer", Set.of(A));
        service.shutdown();
        Thread.sleep(150);
        assertEquals(SeatStatus.FREE, service.status(A));
    }

    @Test
    void unrelatedReleaseDoesNotWakeBuyer() throws Exception {
        BookingService service = service();
        Hold needed = service.hold("first", Set.of(A)).orElseThrow();
        var waiting = start(() -> service.holdOrWait("buyer", Set.of(A), Duration.ofSeconds(30)));
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        var bean = ManagementFactory.getThreadMXBean();
        long before = bean.getThreadInfo(waiting.thread.threadId()).getWaitedCount();
        for (int i = 0; i < 100; i++) {
            assertTrue(service.release(service.hold("other", Set.of(B)).orElseThrow()));
        }
        Thread.sleep(100);
        assertEquals(before, bean.getThreadInfo(waiting.thread.threadId()).getWaitedCount());
        service.release(needed);
        assertTrue(waiting.get().isPresent());
    }

    @Test
    void blockedSeatDoesNotBlockAnIndependentOperation() throws Exception {
        BookingService service = service();
        Field field = BookingServiceImpl.class.getDeclaredField("states");
        field.setAccessible(true);
        Object[] states = (Object[]) field.get(service);
        Pending<Optional<Hold>> blocked;
        synchronized (states[0]) {
            blocked = start(() -> service.hold("first", Set.of(A)));
            awaitState(blocked.thread, Thread.State.BLOCKED);
            Hold independent = start(() -> service.hold("second", Set.of(B, C))).get().orElseThrow();
            assertTrue(start(() -> service.confirm(independent)).get());
            assertEquals(SeatStatus.SOLD, start(() -> service.status(B)).get());
        }
        assertTrue(blocked.get().isPresent());
    }

    @Test
    void noPartialReservationWhileAcquiringTheSet() throws Exception {
        BookingService service = service();
        Field field = BookingServiceImpl.class.getDeclaredField("states");
        field.setAccessible(true);
        Object[] states = (Object[]) field.get(service);
        Pending<Optional<Hold>> booking;
        Pending<SeatStatus> status;
        synchronized (states[1]) {
            booking = start(() -> service.hold("buyer", Set.of(A, B)));
            awaitState(booking.thread, Thread.State.BLOCKED);
            assertEquals(SeatStatus.FREE, service.status(B));
            status = start(() -> service.status(A));
            awaitState(status.thread, Thread.State.BLOCKED);
        }
        assertTrue(booking.get().isPresent());
        assertEquals(SeatStatus.HELD, status.get());
        assertEquals(SeatStatus.HELD, service.status(B));
    }

    @Test
    void forgedAndForeignHoldsCannotModifySeats() {
        BookingService service = service();
        Hold original = service.hold("buyer", Set.of(A)).orElseThrow();
        Hold forged = new Hold(original.id(), original.clientId(), original.seats(), original.expiresAt());
        assertFalse(service.confirm(forged));
        assertFalse(service.release(forged));
        BookingService other = service();
        Hold foreign = other.hold("buyer", Set.of(A)).orElseThrow();
        assertFalse(service.confirm(foreign));
        assertTrue(service.confirm(original));
    }

    @Test
    void seatSetIsCopiedAndEqualSeatsReferToTheSamePlace() {
        BookingService service = service();
        Set<Seat> seats = new HashSet<>(Set.of(A, B));
        Hold hold = service.hold("buyer", seats).orElseThrow();
        seats.clear();
        assertEquals(Set.of(A, B), hold.seats());
        assertTrue(service.hold("other", Set.of(new Seat(1, 1))).isEmpty());
        assertTrue(service.release(hold));
    }

    @Test
    void validationPrecedesShutdownCheck() {
        BookingService service = service();
        service.shutdown();
        assertThrows(NullPointerException.class, () -> service.status(null));
        assertThrows(NullPointerException.class, () -> service.confirm(null));
        assertThrows(NullPointerException.class, () -> service.release(null));
        assertThrows(NullPointerException.class,
                () -> service.holdOrWait("buyer", Set.of(A), null));
        assertThrows(IllegalArgumentException.class,
                () -> service.holdOrWait("buyer", Set.of(A), Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> service.hold(" ", Set.of(A)));
        assertThrows(IllegalArgumentException.class, () -> service.status(new Seat(5, 1)));
        assertThrows(IllegalArgumentException.class, () -> service.hold("buyer", Set.of()));
        Set<Seat> tooMany = new HashSet<>();
        for (int i = 1; i <= 10; i++) {
            tooMany.add(new Seat(1, i));
        }
        tooMany.add(new Seat(2, 1));
        assertThrows(IllegalArgumentException.class, () -> service.hold("buyer", tooMany));
        Set<Seat> withNull = new HashSet<>();
        withNull.add(null);
        assertThrows(NullPointerException.class, () -> service.hold("buyer", withNull));
    }

    @Test
    void veryLargeDurationsDoNotOverflow() throws Exception {
        BookingService service = service(Duration.ofSeconds(Long.MAX_VALUE));
        Hold hold = service.hold("buyer", Set.of(A)).orElseThrow();
        assertEquals(SeatStatus.HELD, service.status(A));
        var waiting = start(() -> service.holdOrWait("other", Set.of(A), Duration.ofSeconds(Long.MAX_VALUE)));
        awaitState(waiting.thread, Thread.State.TIMED_WAITING);
        assertTrue(service.release(hold));
        assertTrue(waiting.get().isPresent());
    }

    private record Pending<T>(Thread thread, FutureTask<T> task) {
        T get() throws Exception {
            return task.get(3, TimeUnit.SECONDS);
        }
    }
}
