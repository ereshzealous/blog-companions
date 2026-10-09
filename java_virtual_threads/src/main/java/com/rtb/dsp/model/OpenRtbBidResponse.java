package com.rtb.dsp.model;

public record OpenRtbBidResponse(
        String auctionId,
        String seat,
        String impId,
        long campaignId,
        String creativeId,
        long bidPriceMicros,
        Latency latency) {

    /**
     * @param budgetTier which cache tier answered the budget read: L1, L2 or DB
     */
    public record Latency(long totalMicros, long budgetLookupMicros, String budgetTier) {
    }
}
