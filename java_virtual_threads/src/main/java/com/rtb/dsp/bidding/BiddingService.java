package com.rtb.dsp.bidding;

import com.rtb.dsp.compute.AuctionContext;
import com.rtb.dsp.compute.AuctionContextData;
import com.rtb.dsp.data.MultiTierBudgetCache;
import com.rtb.dsp.data.MultiTierBudgetCache.Lookup;
import com.rtb.dsp.model.BudgetSnapshot;
import com.rtb.dsp.model.OpenRtbBidRequest;
import com.rtb.dsp.model.OpenRtbBidResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.TimeoutException;

/**
 * Budget forks inherit the auction context. Closing the scope interrupts unfinished reads;
 * an unavailable budget produces a no-bid.
 */
@Slf4j
public class BiddingService {

    private final CampaignCatalog catalog;
    private final MultiTierBudgetCache budgets;
    private final String seat;
    private final int maxCandidates;
    private final Counter deadlineMisses;
    private final Counter lookupFailures;

    public BiddingService(CampaignCatalog catalog, MultiTierBudgetCache budgets, String seat, int maxCandidates,
                          MeterRegistry registry) {
        this.catalog = catalog;
        this.budgets = budgets;
        this.seat = seat;
        this.maxCandidates = maxCandidates;
        this.deadlineMisses = registry.counter("rtb.bid.no_bid", "reason", "deadline");
        this.lookupFailures = registry.counter("rtb.bid.no_bid", "reason", "budget_lookup");
    }

    public Optional<OpenRtbBidResponse> bid(OpenRtbBidRequest request) throws InterruptedException {
        AuctionContextData auction = AuctionContext.current();
        List<CampaignCatalog.Candidate> candidates =
                catalog.candidates(request.deviceGeo().country(), request.bidFloorMicros(), maxCandidates);
        if (candidates.isEmpty() || auction.expired()) {
            return Optional.empty();
        }
        long lookupStart = System.nanoTime();
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            List<Subtask<Lookup>> lookups = new ArrayList<>(candidates.size());
            for (CampaignCatalog.Candidate c : candidates) {
                lookups.add(scope.fork(() -> budgets.get(c.campaignId())));
            }
            scope.joinUntil(auction.deadline());
            scope.throwIfFailed();
            long lookupMicros = (System.nanoTime() - lookupStart) / 1_000;
            for (int i = 0; i < candidates.size(); i++) {
                Lookup lookup = lookups.get(i).get();
                BudgetSnapshot budget = lookup.snapshot();
                long price = price(candidates.get(i), request.bidFloorMicros());
                if (budget != null && budget.canSpend(price)) {
                    return Optional.of(new OpenRtbBidResponse(
                            request.auctionId(), seat, request.imp().id(), budget.campaignId(),
                            "cr-" + budget.campaignId() + "-" + request.imp().format(), price,
                            new OpenRtbBidResponse.Latency(auction.elapsedMicros(), lookupMicros, lookup.tier().name())));
                }
            }
            return Optional.empty();
        } catch (TimeoutException e) {
            deadlineMisses.increment();
            return Optional.empty();
        } catch (ExecutionException e) {
            lookupFailures.increment();
            log.debug("budget lookup failed for auction {}", request.auctionId(), e.getCause());
            return Optional.empty();
        }
    }

    /** First-price shading: 85% of the campaign's max bid, never below the floor. */
    static long price(CampaignCatalog.Candidate candidate, long floorMicros) {
        long shaded = candidate.maxBidMicros() * 85 / 100;
        return Math.min(candidate.maxBidMicros(), Math.max(floorMicros, shaded));
    }
}
