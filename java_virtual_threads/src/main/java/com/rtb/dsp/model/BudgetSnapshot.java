package com.rtb.dsp.model;

/**
 * Caches an immutable value instead of a JPA entity and its persistence context.
 */
public record BudgetSnapshot(
        long campaignId,
        long maxBidMicros,
        long remainingMicros,
        long dailyPacingThresholdMicros,
        long spentTodayMicros) {

    public boolean canSpend(long priceMicros) {
        return remainingMicros >= priceMicros
                && spentTodayMicros + priceMicros <= dailyPacingThresholdMicros;
    }

    /** L2 encoding: five colon-separated longs. */
    public String encode() {
        return campaignId + ":" + maxBidMicros + ":" + remainingMicros + ":" + dailyPacingThresholdMicros
                + ":" + spentTodayMicros;
    }

    public static BudgetSnapshot decode(String encoded) {
        String[] f = encoded.split(":", 5);
        if (f.length != 5) {
            throw new IllegalArgumentException("malformed budget snapshot: " + encoded);
        }
        return new BudgetSnapshot(Long.parseLong(f[0]), Long.parseLong(f[1]), Long.parseLong(f[2]),
                Long.parseLong(f[3]), Long.parseLong(f[4]));
    }
}
