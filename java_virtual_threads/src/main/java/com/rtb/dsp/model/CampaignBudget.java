package com.rtb.dsp.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "campaign_budget")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class CampaignBudget {

    @Id
    @Column(name = "campaign_id")
    private Long campaignId;

    @Column(name = "target_country", nullable = false, length = 2, columnDefinition = "char(2)")
    private String targetCountry;

    @Column(name = "max_bid_micros", nullable = false)
    private long maxBidMicros;

    @Column(name = "total_cap_micros", nullable = false)
    private long totalCapMicros;

    @Column(name = "remaining_micros", nullable = false)
    private long remainingMicros;

    @Column(name = "daily_pacing_threshold_micros", nullable = false)
    private long dailyPacingThresholdMicros;

    @Column(name = "spent_today_micros", nullable = false)
    private long spentTodayMicros;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public CampaignBudget(long campaignId, String targetCountry, long maxBidMicros, long totalCapMicros,
                          long remainingMicros, long dailyPacingThresholdMicros, long spentTodayMicros) {
        this.campaignId = campaignId;
        this.targetCountry = targetCountry;
        this.maxBidMicros = maxBidMicros;
        this.totalCapMicros = totalCapMicros;
        this.remainingMicros = remainingMicros;
        this.dailyPacingThresholdMicros = dailyPacingThresholdMicros;
        this.spentTodayMicros = spentTodayMicros;
        this.updatedAt = Instant.now();
    }

    public BudgetSnapshot toSnapshot() {
        return new BudgetSnapshot(campaignId, maxBidMicros, remainingMicros, dailyPacingThresholdMicros,
                spentTodayMicros);
    }
}
