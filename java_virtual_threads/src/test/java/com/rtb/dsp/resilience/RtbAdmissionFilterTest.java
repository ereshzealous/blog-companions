package com.rtb.dsp.resilience;

import com.rtb.dsp.config.RtbProperties.ShedResponse;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class RtbAdmissionFilterTest {

    private static AdaptiveAimdLimiter saturated() {
        var limiter = new AdaptiveAimdLimiter(new AdaptiveAimdLimiter.Config(1, 1, 10, Duration.ofMillis(35), 0.7, 1,
                Duration.ofMillis(100), 50));
        assertThat(limiter.tryAcquire()).isTrue();
        return limiter;
    }

    private static MockHttpServletResponse run(RtbAdmissionFilter filter, AtomicBoolean reachedController)
            throws Exception {
        var response = new MockHttpServletResponse();
        filter.doFilter(new MockHttpServletRequest("POST", "/openrtb/2.6/bid"), response,
                (req, res) -> reachedController.set(true));
        return response;
    }

    @Test
    void overCapacityBidRequestsGetAnImmediateNoBid() throws Exception {
        var reached = new AtomicBoolean();
        var response = run(new RtbAdmissionFilter(saturated(), ShedResponse.NO_BID, new SimpleMeterRegistry()), reached);

        assertThat(response.getStatus()).isEqualTo(204);
        assertThat(response.getHeader(RtbAdmissionFilter.HEADER_LOAD_SHED)).isEqualTo("true");
        assertThat(response.getHeader("Retry-After")).isNull();
        assertThat(reached).isFalse();
    }

    @Test
    void unavailableModeAnswers503WithRetryAfter() throws Exception {
        var reached = new AtomicBoolean();
        var response = run(new RtbAdmissionFilter(saturated(), ShedResponse.UNAVAILABLE, new SimpleMeterRegistry()), reached);

        assertThat(response.getStatus()).isEqualTo(503);
        assertThat(response.getHeader("Retry-After")).isEqualTo("1");
        assertThat(response.getHeader(RtbAdmissionFilter.HEADER_LOAD_SHED)).isEqualTo("true");
        assertThat(reached).isFalse();
    }

    @Test
    void admittedRequestsReleaseTheirSlot() throws Exception {
        var limiter = new AdaptiveAimdLimiter(new AdaptiveAimdLimiter.Config(4, 1, 10, Duration.ofMillis(35), 0.7, 1,
                Duration.ofMillis(100), 50));
        var reached = new AtomicBoolean();
        var response = run(new RtbAdmissionFilter(limiter, ShedResponse.NO_BID, new SimpleMeterRegistry()), reached);

        assertThat(reached).isTrue();
        assertThat(response.getHeader(RtbAdmissionFilter.HEADER_LOAD_SHED)).isNull();
        assertThat(limiter.inFlight()).isZero();
    }
}
