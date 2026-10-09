package com.rtb.dsp.model;

public record WinNotice(String auctionId, long campaignId, long clearingPriceMicros, long receivedEpochMillis) {

    public String encode() {
        return auctionId + ":" + campaignId + ":" + clearingPriceMicros + ":" + receivedEpochMillis;
    }

    public static WinNotice decode(String encoded) {
        int c3 = encoded.lastIndexOf(':');
        int c2 = encoded.lastIndexOf(':', c3 - 1);
        int c1 = encoded.lastIndexOf(':', c2 - 1);
        if (c1 <= 0) {
            throw new IllegalArgumentException("malformed win notice: " + encoded);
        }
        return new WinNotice(encoded.substring(0, c1),
                Long.parseLong(encoded.substring(c1 + 1, c2)),
                Long.parseLong(encoded.substring(c2 + 1, c3)),
                Long.parseLong(encoded.substring(c3 + 1)));
    }
}
