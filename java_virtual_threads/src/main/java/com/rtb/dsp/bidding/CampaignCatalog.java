package com.rtb.dsp.bidding;

import com.rtb.dsp.data.CampaignBudgetRepository;
import com.rtb.dsp.model.CampaignBudget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Publishes an immutable targeting index with a volatile swap. Spend stays in the budget cache.
 */
@Slf4j
public class CampaignCatalog {

    public record Candidate(long campaignId, long maxBidMicros) {
    }

    private final CampaignBudgetRepository repository;
    private volatile Map<String, List<Candidate>> byCountry = Map.of();

    public CampaignCatalog(CampaignBudgetRepository repository) {
        this.repository = repository;
    }

    @EventListener(ApplicationReadyEvent.class)
    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
    public void refresh() {
        try {
            Map<String, List<Candidate>> next = repository.findAll().stream()
                    .collect(Collectors.groupingBy(b -> b.getTargetCountry().trim(),
                            Collectors.collectingAndThen(Collectors.toList(), CampaignCatalog::rank)));
            byCountry = Map.copyOf(next);
            log.info("campaign catalog loaded: {} markets, {} campaigns", next.size(),
                    next.values().stream().mapToInt(List::size).sum());
        } catch (RuntimeException e) {
            log.warn("campaign catalog refresh failed; keeping {} markets from the previous load", byCountry.size(), e);
        }
    }

    private static List<Candidate> rank(List<CampaignBudget> budgets) {
        List<Candidate> ranked = new ArrayList<>(budgets.size());
        for (CampaignBudget b : budgets) {
            ranked.add(new Candidate(b.getCampaignId(), b.getMaxBidMicros()));
        }
        ranked.sort(Comparator.comparingLong(Candidate::maxBidMicros).reversed());
        return List.copyOf(ranked);
    }

    public List<Candidate> candidates(String country, long floorMicros, int max) {
        List<Candidate> market = byCountry.getOrDefault(country, List.of());
        List<Candidate> out = new ArrayList<>(Math.min(max, market.size()));
        for (Candidate c : market) {
            if (c.maxBidMicros() < floorMicros) {
                break;  // sorted descending: nothing further can clear the floor
            }
            out.add(c);
            if (out.size() == max) {
                break;
            }
        }
        return out;
    }

    public int markets() {
        return byCountry.size();
    }
}
