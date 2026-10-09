package com.rtb.dsp.compute;

import com.rtb.dsp.config.RtbProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AuctionContextTest {

    private static AuctionContextData auction(String id) {
        long now = System.nanoTime();
        return new AuctionContextData(id, "exchange-a", "2.6", now, now + 50_000_000, Instant.now().plusMillis(50));
    }

    @Test
    void bindingIsVisibleForTheCallAndGoneAfterIt() throws Exception {
        String seen = AuctionContext.callWith(auction("a-1"), () -> AuctionContext.current().requestId());

        assertThat(seen).isEqualTo("a-1");
        assertThat(AuctionContext.currentIfBound()).isEmpty();
        assertThatThrownBy(AuctionContext::current).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nestedRebindingShadowsAndThenRestores() throws Exception {
        AuctionContext.callWith(auction("outer"), () -> {
            String inner = AuctionContext.callWith(auction("inner"), () -> AuctionContext.current().requestId());
            assertThat(inner).isEqualTo("inner");
            assertThat(AuctionContext.current().requestId()).isEqualTo("outer");
            return null;
        });
    }

    @Test
    void structuredTaskScopeForksInheritTheBinding() throws Exception {
        String inherited = AuctionContext.callWith(auction("parent"), () -> {
            try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                var child = scope.fork(() -> AuctionContext.current().requestId());
                scope.join().throwIfFailed();
                return child.get();
            }
        });

        assertThat(inherited).isEqualTo("parent");
    }

    @Test
    void unstructuredThreadsDoNotSeeTheBinding() throws Exception {
        boolean visibleInExecutor = AuctionContext.callWith(auction("parent"), () -> {
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                return executor.submit(() -> AuctionContext.currentIfBound().isPresent()).get();
            }
        });

        assertThat(visibleInExecutor).isFalse();
    }

    @Test
    void filterBindsDeadlineFromTmaxMinusNetworkMargin() throws Exception {
        var filter = new ScopedAuctionFilter(new RtbProperties.Auction(Duration.ofMillis(80), Duration.ofMillis(30), 3, "seat"));
        var request = new MockHttpServletRequest("POST", "/openrtb/2.6/bid");
        request.addHeader(ScopedAuctionFilter.HEADER_TMAX, "120");
        request.addHeader(ScopedAuctionFilter.HEADER_EXCHANGE, "exchange-b");
        var response = new MockHttpServletResponse();
        AtomicReference<AuctionContextData> captured = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> captured.set(AuctionContext.current()));

        AuctionContextData data = captured.get();
        assertThat(data.exchange()).isEqualTo("exchange-b");
        assertThat(data.deadlineNanos() - data.ingressNanos()).isEqualTo(Duration.ofMillis(90).toNanos());
        assertThat(response.getHeader(ScopedAuctionFilter.HEADER_VERSION)).isEqualTo("2.6");
        assertThat(AuctionContext.currentIfBound()).isEmpty();
    }
}
