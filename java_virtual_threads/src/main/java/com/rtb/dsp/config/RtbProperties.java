package com.rtb.dsp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "rtb")
public record RtbProperties(
        @DefaultValue Auction auction,
        @DefaultValue Cache cache,
        @DefaultValue Replica replica,
        @DefaultValue Limiter limiter,
        @DefaultValue Admission admission,
        @DefaultValue WinNotices winNotices,
        @DefaultValue Pinning pinning) {

    /**
     * @param defaultTmax   used when the exchange sends no tmax header
     * @param networkMargin carved out of tmax for the return trip; the bidder works to what is left
     */
    public record Auction(
            @DefaultValue("80ms") Duration defaultTmax,
            @DefaultValue("30ms") Duration networkMargin,
            @DefaultValue("3") int maxCandidates,
            @DefaultValue("rtb-dsp-seat-1") String seat) {
    }

    public record Cache(
            @DefaultValue("10s") Duration l1Ttl,
            @DefaultValue("20000") long l1MaxSize,
            @DefaultValue("30s") Duration l2Ttl,
            @DefaultValue("campaign:budget:invalidations") String invalidationChannel,
            @DefaultValue("40ms") Duration singleFlightTimeout) {
    }

    public record Replica(
            @DefaultValue("200ms") Duration lagThreshold,
            @DefaultValue("500ms") Duration probeInterval,
            @DefaultValue("3") int closeAfterHealthyProbes) {
    }

    /**
     * @param expectedRps     per-pod arrival rate used with Little's Law to seed the initial limit
     * @param baselineLatency per-request latency at that rate under normal conditions
     */
    public record Limiter(
            @DefaultValue("30000") double expectedRps,
            @DefaultValue("8ms") Duration baselineLatency,
            @DefaultValue("8") int minLimit,
            @DefaultValue("5000") int maxLimit,
            @DefaultValue("35ms") Duration targetP90,
            @DefaultValue("0.7") double backoffRatio,
            @DefaultValue("32") int additiveStep,
            @DefaultValue("100ms") Duration window,
            @DefaultValue("16") int minWindowSamples) {
    }

    public enum ShedResponse { NO_BID, UNAVAILABLE }

    public record Admission(@DefaultValue("NO_BID") ShedResponse shedResponse) {
    }

    public record WinNotices(
            @DefaultValue("rtb.win-notices") String topic,
            @DefaultValue("12") int partitions,
            @DefaultValue("10000") int queueCapacity,
            @DefaultValue("20ms") Duration flushInterval,
            @DefaultValue("512") int maxBatch,
            @DefaultValue("5ms") Duration offerTimeout,
            @DefaultValue("2s") Duration ackTimeout) {
    }

    public record Pinning(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("5ms") Duration threshold,
            @DefaultValue("64") int maxSites) {
    }
}
