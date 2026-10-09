package com.rtb.dsp.compute;

import com.rtb.dsp.config.RtbProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binds a request deadline derived from exchange tmax minus the return-path allowance.
 */
public class ScopedAuctionFilter extends OncePerRequestFilter {

    public static final String HEADER_VERSION = "x-openrtb-version";
    public static final String HEADER_EXCHANGE = "x-exchange-id";
    public static final String HEADER_TMAX = "x-openrtb-tmax";
    public static final String HEADER_REQUEST_ID = "x-request-id";

    private static final AtomicLong SEQUENCE = new AtomicLong();

    private final Duration defaultTmax;
    private final Duration networkMargin;

    public ScopedAuctionFilter(RtbProperties.Auction auction) {
        this.defaultTmax = auction.defaultTmax();
        this.networkMargin = auction.networkMargin();
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/openrtb/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long ingress = System.nanoTime();
        long budgetNanos = Math.max(0, tmax(request).toNanos() - networkMargin.toNanos());
        String version = headerOr(request, HEADER_VERSION, "2.6");
        AuctionContextData data = new AuctionContextData(
                headerOr(request, HEADER_REQUEST_ID, Long.toString(SEQUENCE.incrementAndGet(), 36)),
                headerOr(request, HEADER_EXCHANGE, "unknown"),
                version,
                ingress,
                ingress + budgetNanos,
                Instant.now().plusNanos(budgetNanos));
        response.setHeader(HEADER_VERSION, version);
        try {
            AuctionContext.callWith(data, () -> {
                chain.doFilter(request, response);
                return null;
            });
        } catch (IOException | ServletException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    private Duration tmax(HttpServletRequest request) {
        String raw = request.getHeader(HEADER_TMAX);
        if (raw == null || raw.isBlank()) {
            return defaultTmax;
        }
        try {
            return Duration.ofMillis(Long.parseLong(raw.trim()));
        } catch (NumberFormatException e) {
            return defaultTmax;
        }
    }

    private static String headerOr(HttpServletRequest request, String name, String fallback) {
        String value = request.getHeader(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
