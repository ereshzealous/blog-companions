package com.rtb.dsp.resilience;

import com.rtb.dsp.model.WinNotice;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                Duration.ofSeconds(3), sink, new SimpleMeterRegistry());
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
            assertThat(p.submit(win(i))).isNotCompletedExceptionally();
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
            if (!p.submit(win(i)).isCompletedExceptionally()) {
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
            p.submit(win(i));
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
            p.submit(win(i));
        }
        p.stop();
        assertThat(delivered).hasSize(300);
    }

    @Test
    void receiptWaitsForBrokerAcknowledgment() throws Exception {
        var entered = new CountDownLatch(1);
        var acknowledged = new CountDownLatch(1);
        var p = start(10, 1, batch -> {
            entered.countDown();
            acknowledged.await();
        });
        var receipt = p.submit(win(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(receipt).isNotDone();
        acknowledged.countDown();
        receipt.get(2, TimeUnit.SECONDS);
    }

    @Test
    void responseTimeoutDoesNotCancelDelivery() throws Exception {
        var acknowledged = new CountDownLatch(1);
        publisher = new BoundedWinNoticePublisher(10, Duration.ofMillis(1), 1, Duration.ofMillis(5),
                Duration.ofMillis(30), batch -> acknowledged.await(), new SimpleMeterRegistry());
        publisher.start();
        var receipt = publisher.submit(win(1));
        assertThatThrownBy(() -> receipt.get(2, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
        acknowledged.countDown();
        awaitPublished(publisher, 1);
        assertThat(receipt).isCompletedExceptionally();
    }

    @Test
    void shutdownFailsUnacknowledgedReceiptsAndRejectsNewWork() throws Exception {
        var entered = new CountDownLatch(1);
        publisher = new BoundedWinNoticePublisher(10, Duration.ofMillis(1), 1, Duration.ofMillis(5),
                Duration.ofSeconds(30), batch -> {
                    entered.countDown();
                    new CountDownLatch(1).await();
                }, new SimpleMeterRegistry());
        publisher.start();
        var receipt = publisher.submit(win(1));
        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
        publisher.stop();
        assertThat(receipt).isCompletedExceptionally();
        assertThat(publisher.submit(win(2))).isCompletedExceptionally();
        assertThat(publisher.isRunning()).isFalse();
    }

    private static void awaitPublished(BoundedWinNoticePublisher p, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (p.publishedCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(p.publishedCount()).isEqualTo(expected);
    }
}
