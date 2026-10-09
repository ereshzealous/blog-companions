package com.rtb.dsp.model;

import java.util.List;
import java.util.Objects;

/**
 * Money uses integer micros.
 * @param ingressEpochNanos wall-clock nanos for correlation only; elapsed checks use System.nanoTime()
 */
public record OpenRtbBidRequest(
        String auctionId,
        String exchange,
        Imp imp,
        Geo deviceGeo,
        long bidFloorMicros,
        long ingressEpochNanos) {

    public OpenRtbBidRequest {
        Objects.requireNonNull(auctionId, "auctionId");
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(imp, "imp");
        Objects.requireNonNull(deviceGeo, "deviceGeo");
        if (bidFloorMicros < 0) {
            throw new IllegalArgumentException("bidFloorMicros must be >= 0");
        }
    }

    public OpenRtbBidRequest withIngressEpochNanos(long epochNanos) {
        return new OpenRtbBidRequest(auctionId, exchange, imp, deviceGeo, bidFloorMicros, epochNanos);
    }

    public record Imp(String id, Banner banner, Video video) {
        public Imp {
            Objects.requireNonNull(id, "imp.id");
            if (banner == null && video == null) {
                throw new IllegalArgumentException("imp needs a banner or a video object");
            }
        }

        public String format() {
            return video != null ? "video" : "banner";
        }
    }

    public record Banner(int w, int h) {
    }

    public record Video(int minDurationSec, int maxDurationSec, List<String> mimes) {
        public Video {
            mimes = mimes == null ? List.of() : List.copyOf(mimes);
        }
    }

    public record Geo(String country, String region, double lat, double lon) {
        public Geo {
            Objects.requireNonNull(country, "geo.country");
        }
    }
}
