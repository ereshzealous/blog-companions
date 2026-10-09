package com.rtb.dsp.resilience;

import com.rtb.dsp.model.WinNotice;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class BoundedWinNoticePublisherTest {

    private BoundedWinNoticePublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.stop();
        }
    }

    private BoundedWinNoticePublisher start(int capacity, int maxBatch, BoundedWinNoticePublisher.BatchSink sink) {
        publisher = new BoundedWinNoticePublisher(capacity, Duration.ofMillis(20), maxBatch, Duration.ofMillis(5),
                sink, new SimpleMeterRegistry());
        publisher.start();
        return publisher;
    }

    private static WinNotice win(int i) {
        return new WinNotice("auction-" + i, 1 + i % 8, 1_000_000, System.currentTimeMillis());
    }

    @Test
    void drainsEverythingInBatchesNoLargerThanMaxBatch() throws Exception {
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        var p = start(10_000, 256, batch -> batchSizes.add(batch.size()));

        for (int i = 0; i < 2_000; i++) {
            assertThat(p.offer(win(i))).isTrue();
        }
        awaitPublished(p, 2_000);

        assertThat(batchSizes).allMatch(size -> size <= 256);
        assertThat(batchSizes.stream().mapToInt(Integer::intValue).sum()).isEqualTo(2_000);
        assertThat(batchSizes.size()).isLessThan(100);
    }

    @Test
    void aFullBufferPushesBackInsteadOfGrowing() throws Exception {
        var brokerDown = new CountDownLatch(1);
        var p = start(10, 4, batch -> brokerDown.await());

        int accepted = 0;
        int rejected = 0;
        for (int i = 0; i < 50; i++) {
            if (p.offer(win(i))) {
                accepted++;
            } else {
                rejected++;
            }
        }
        assertThat(accepted).isLessThanOrEqualTo(10 + 4);
        assertThat(rejected).isPositive();
        assertThat(p.depth()).isLessThanOrEqualTo(10);

        brokerDown.countDown();
        awaitPublished(p, accepted);
    }

    @Test
    void anUnacknowledgedBatchIsRetriedNotDropped() throws Exception {
        var attempts = new AtomicInteger();
        List<String> delivered = new CopyOnWriteArrayList<>();
        var p = start(100, 16, batch -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new IllegalStateException("NOT_ENOUGH_REPLICAS");
            }
            batch.forEach(n -> delivered.add(n.auctionId()));
        });

        for (int i = 0; i < 10; i++) {
            p.offer(win(i));
        }
        awaitPublished(p, 10);

        assertThat(delivered).hasSize(10).doesNotHaveDuplicates();
        assertThat(attempts.get()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void stopDrainsWhatIsAlreadyBuffered() throws Exception {
        List<String> delivered = new CopyOnWriteArrayList<>();
        var p = start(1_000, 64, batch -> batch.forEach(n -> delivered.add(n.auctionId())));
        for (int i = 0; i < 300; i++) {
            p.offer(win(i));
        }
        p.stop();
        assertThat(delivered).hasSize(300);
    }

    private static void awaitPublished(BoundedWinNoticePublisher p, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (p.publishedCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(p.publishedCount()).isEqualTo(expected);
    }
}
