package com.rtb.dsp.bidding;

import com.rtb.dsp.model.OpenRtbBidRequest;
import com.rtb.dsp.model.OpenRtbBidResponse;
import com.rtb.dsp.model.WinNotice;
import com.rtb.dsp.resilience.BoundedWinNoticePublisher;
import com.rtb.dsp.resilience.RtbAdmissionFilter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;

@RestController
@RequestMapping("/openrtb/2.6")
public class BidController {

    private final BiddingService bidding;
    private final BoundedWinNoticePublisher winNotices;

    public BidController(BiddingService bidding, BoundedWinNoticePublisher winNotices) {
        this.bidding = bidding;
        this.winNotices = winNotices;
    }

    @PostMapping(path = "/bid", consumes = "application/json", produces = "application/json")
    public ResponseEntity<OpenRtbBidResponse> bid(@RequestBody OpenRtbBidRequest request) throws InterruptedException {
        OpenRtbBidRequest stamped = request.ingressEpochNanos() > 0 ? request
                : request.withIngressEpochNanos(epochNanos(Instant.now()));
        return bidding.bid(stamped)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.noContent().build());
    }

    /** Win URL (nurl). The exchange substitutes the clearing price; we answer before it is settled. */
    @GetMapping("/win")
    public ResponseEntity<Void> win(@RequestParam String auctionId, @RequestParam long campaignId,
                                    @RequestParam long priceMicros) throws InterruptedException {
        WinNotice notice = new WinNotice(auctionId, campaignId, priceMicros, System.currentTimeMillis());
        if (winNotices.offer(notice)) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header("Retry-After", "1")
                .header(RtbAdmissionFilter.HEADER_LOAD_SHED, "true")
                .build();
    }

    private static long epochNanos(Instant now) {
        return now.getEpochSecond() * 1_000_000_000L + now.getNano();
    }
}
