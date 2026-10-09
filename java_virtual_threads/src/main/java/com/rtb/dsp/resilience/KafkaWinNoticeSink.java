package com.rtb.dsp.resilience;

import com.rtb.dsp.model.WinNotice;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Waits for broker acknowledgment on the drainer thread. Campaign keys preserve partition order.
 */
public class KafkaWinNoticeSink implements BoundedWinNoticePublisher.BatchSink {

    private final KafkaTemplate<String, String> kafka;
    private final String topic;
    private final Duration ackTimeout;

    public KafkaWinNoticeSink(KafkaTemplate<String, String> kafka, String topic, Duration ackTimeout) {
        this.kafka = kafka;
        this.topic = topic;
        this.ackTimeout = ackTimeout;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void publish(List<WinNotice> batch) throws Exception {
        CompletableFuture<SendResult<String, String>>[] acks = new CompletableFuture[batch.size()];
        for (int i = 0; i < batch.size(); i++) {
            WinNotice n = batch.get(i);
            acks[i] = kafka.send(topic, Long.toString(n.campaignId()), n.encode());
        }
        CompletableFuture.allOf(acks).get(ackTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}
