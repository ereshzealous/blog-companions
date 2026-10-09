package com.rtb.dsp.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ModelCodecTest {

    @Test
    void budgetSnapshotRoundTripsThroughItsL2Encoding() {
        var snapshot = new BudgetSnapshot(7, 9_000_000, 123_456_789, 50_000_000_000L, 42);
        assertThat(BudgetSnapshot.decode(snapshot.encode())).isEqualTo(snapshot);
    }

    @Test
    void pacingAndBalanceBothGateSpend() {
        var snapshot = new BudgetSnapshot(1, 9_000_000, 10_000_000, 20_000_000, 15_000_000);
        assertThat(snapshot.canSpend(5_000_000)).isTrue();
        assertThat(snapshot.canSpend(5_000_001)).isFalse();
        assertThat(new BudgetSnapshot(1, 9_000_000, 1_000, 20_000_000, 0).canSpend(5_000)).isFalse();
    }

    @Test
    void winNoticeSurvivesAuctionIdsContainingTheSeparator() {
        var notice = new WinNotice("exch:2026:abc", 12, 7_650_000, 1_790_000_000_000L);
        assertThat(WinNotice.decode(notice.encode())).isEqualTo(notice);
    }
}
