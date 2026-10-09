package com.rtb.dsp.data;

import com.rtb.dsp.model.WinNotice;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * The ledger deduplicates redelivery. HTTP acknowledges a volatile queue before Kafka,
 * so a process failure can still lose an accepted notice.
 */
public class WinNoticeLedgerConsumer {

    private final BudgetLedger ledger;

    public WinNoticeLedgerConsumer(BudgetLedger ledger) {
        this.ledger = ledger;
    }

    @KafkaListener(topics = "${rtb.win-notices.topic:rtb.win-notices}", groupId = "rtb-budget-ledger")
    public void onWin(String payload) {
        ledger.settle(WinNotice.decode(payload));
    }
}
