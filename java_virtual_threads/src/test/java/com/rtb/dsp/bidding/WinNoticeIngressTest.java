package com.rtb.dsp.bidding;

import com.rtb.dsp.resilience.BoundedWinNoticePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class WinNoticeIngressTest {
    private final BoundedWinNoticePublisher publisher = mock(BoundedWinNoticePublisher.class);
    private MockMvc mvc;
    private static final String WIN = "/openrtb/2.6/win?auctionId=one&campaignId=7&priceMicros=1000";

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new BidController(mock(BiddingService.class), publisher)).build();
    }

    @Test
    void httpSuccessWaitsForAcknowledgment() throws Exception {
        var acknowledgment = new CompletableFuture<Void>();
        when(publisher.submit(any())).thenReturn(acknowledgment);
        var result = mvc.perform(get(WIN)).andExpect(request().asyncStarted()).andReturn();
        assertThat(result.getRequest().getAsyncContext().getResponse().isCommitted()).isFalse();
        assertThat(acknowledgment).isNotDone();
        acknowledgment.complete(null);
        mvc.perform(asyncDispatch(result)).andExpect(status().isNoContent());
    }

    @Test
    void fullQueueReturnsRetryableFailure() throws Exception {
        when(publisher.submit(any())).thenReturn(CompletableFuture.failedFuture(new RejectedExecutionException()));
        var result = mvc.perform(get(WIN)).andExpect(request().asyncStarted()).andReturn();
        mvc.perform(asyncDispatch(result)).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"));
    }

    @Test
    void missingAcknowledgmentCannotBecomeHttpSuccess() throws Exception {
        var acknowledgment = new CompletableFuture<Void>();
        when(publisher.submit(any())).thenReturn(acknowledgment);
        var result = mvc.perform(get(WIN)).andExpect(request().asyncStarted()).andReturn();
        acknowledgment.completeExceptionally(new TimeoutException());
        mvc.perform(asyncDispatch(result)).andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"));
    }
}
