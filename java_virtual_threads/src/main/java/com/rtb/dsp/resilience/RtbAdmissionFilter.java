package com.rtb.dsp.resilience;

import com.rtb.dsp.config.RtbProperties.ShedResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Sheds before context binding and body parsing, using 204 or 503 with Retry-After.
 */
public class RtbAdmissionFilter extends OncePerRequestFilter {

    public static final String HEADER_LOAD_SHED = "X-Load-Shed";

    private final AdaptiveAimdLimiter limiter;
    private final ShedResponse shedResponse;
    private final Counter shed;

    public RtbAdmissionFilter(AdaptiveAimdLimiter limiter, ShedResponse shedResponse, MeterRegistry registry) {
        this.limiter = limiter;
        this.shedResponse = shedResponse;
        this.shed = registry.counter("rtb.admission.shed", "response", shedResponse.name());
        registry.gauge("rtb.admission.limit", limiter, AdaptiveAimdLimiter::limit);
        registry.gauge("rtb.admission.in_flight", limiter, AdaptiveAimdLimiter::inFlight);
        registry.gauge("rtb.admission.p90.ms", limiter, l -> l.lastP90Nanos() / 1e6);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!limiter.tryAcquire()) {
            shed.increment();
            response.setHeader(HEADER_LOAD_SHED, "true");
            if (shedResponse == ShedResponse.UNAVAILABLE) {
                response.setHeader("Retry-After", "1");
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            } else {
                response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            }
            return;
        }
        long start = System.nanoTime();
        AdaptiveAimdLimiter.Outcome outcome = AdaptiveAimdLimiter.Outcome.DROPPED;
        try {
            chain.doFilter(request, response);
            if (response.getStatus() < 500) {
                outcome = AdaptiveAimdLimiter.Outcome.SUCCESS;
            }
        } finally {
            limiter.release(System.nanoTime() - start, outcome);
        }
    }
}
