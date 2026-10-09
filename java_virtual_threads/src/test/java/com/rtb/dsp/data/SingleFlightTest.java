package com.rtb.dsp.data;

import com.rtb.dsp.compute.AuctionContext;
import com.rtb.dsp.compute.AuctionContextData;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleFlightTest {

    private static final Duration WAIT = Duration.ofSeconds(5);

    @Test
    void thousandConcurrentMissesCauseOneLoad() throws Exception {
        var flight = new SingleFlight<String, Long>("test");
        var loads = new AtomicInteger();
        var start = new CountDownLatch(1);
        List<Future<Long>> results = new ArrayList<>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 1_000; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return flight.execute("campaign:1", () -> {
                        loads.incrementAndGet();
                        Thread.sleep(50);
                        return 42L;
                    }, WAIT);
                }));
            }
            start.countDown();
            for (Future<Long> f : results) {
                assertThat(f.get()).isEqualTo(42L);
            }
        }

        assertThat(loads.get()).isEqualTo(1);
        assertThat(flight.flights()).isEqualTo(1);
        assertThat(flight.joined()).isEqualTo(999);
        assertThat(flight.inFlightKeys()).isZero();
    }

    @Test
    void failureReachesEveryWaiterAndTheNextCallRetries() throws Exception {
        var flight = new SingleFlight<String, Long>("test");
        var loads = new AtomicInteger();

        assertThatThrownBy(() -> flight.execute("k", () -> {
            loads.incrementAndGet();
            throw new IOException("replica unavailable");
        }, WAIT)).isInstanceOf(IOException.class).hasMessage("replica unavailable");

        assertThat(flight.execute("k", () -> {
            loads.incrementAndGet();
            return 7L;
        }, WAIT)).isEqualTo(7L);
        assertThat(loads.get()).isEqualTo(2);
    }

    @Test
    void anImpatientWaiterTimesOutWithoutFailingTheSharedLoad() throws Exception {
        var flight = new SingleFlight<String, Long>("test");
        var release = new CountDownLatch(1);

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Long> patient = executor.submit(() -> flight.execute("k", () -> {
                release.await();
                return 9L;
            }, WAIT));
            Thread.sleep(20);

            assertThatThrownBy(() -> flight.execute("k", () -> -1L, Duration.ofMillis(5)))
                    .isInstanceOf(TimeoutException.class);

            release.countDown();
            assertThat(patient.get()).isEqualTo(9L);
        }
        assertThat(flight.flights()).isEqualTo(1);
    }

    @Test
    void cancellingTheRequestThatStartedTheLoadDoesNotPoisonIt() throws Exception {
        var flight = new SingleFlight<String, Long>("test");
        var loads = new AtomicInteger();
        var release = new CountDownLatch(1);

        Thread first = Thread.ofVirtual().start(() -> {
            try {
                flight.execute("k", () -> {
                    loads.incrementAndGet();
                    release.await();
                    return 11L;
                }, WAIT);
            } catch (Exception expected) {
                // interrupted while waiting: this request gave up, the load did not
            }
        });
        Thread.sleep(20);
        first.interrupt();
        first.join();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Long> second = executor.submit(() -> flight.execute("k", () -> -1L, WAIT));
            Thread.sleep(20);
            release.countDown();
            assertThat(second.get()).isEqualTo(11L);
        }
        assertThat(loads.get()).isEqualTo(1);
    }

    @Test
    void theSharedLoadDoesNotSeeTheCallersAuction() throws Exception {
        var flight = new SingleFlight<String, Boolean>("test");
        long now = System.nanoTime();
        var auction = new AuctionContextData("a-1", "x", "2.6", now, now + 50_000_000, Instant.now().plusMillis(50));

        boolean loaderSawAuction = AuctionContext.callWith(auction,
                () -> flight.execute("k", () -> AuctionContext.currentIfBound().isPresent(), WAIT));

        assertThat(loaderSawAuction).isFalse();
    }
}
