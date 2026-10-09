package com.rtb.dsp.data;

import com.rtb.dsp.model.CampaignBudget;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Inherited SimpleJpaRepository read methods open read-only transactions for replica routing.
 */
public interface CampaignBudgetRepository extends JpaRepository<CampaignBudget, Long> {
}
