package com.rtb.dsp.compute;

import java.util.Optional;
import java.util.concurrent.Callable;

/**
 * StructuredTaskScope forks inherit this binding; detached loader threads do not.
 */
public final class AuctionContext {

    public static final ScopedValue<AuctionContextData> CURRENT = ScopedValue.newInstance();

    private AuctionContext() {
    }

    public static AuctionContextData current() {
        return CURRENT.orElseThrow(() -> new IllegalStateException("no auction is bound to this thread"));
    }

    public static Optional<AuctionContextData> currentIfBound() {
        return CURRENT.isBound() ? Optional.of(CURRENT.get()) : Optional.empty();
    }

    public static <R> R callWith(AuctionContextData data, Callable<R> op) throws Exception {
        return ScopedValue.where(CURRENT, data).call(op);
    }

    public static void runWith(AuctionContextData data, Runnable op) {
        ScopedValue.where(CURRENT, data).run(op);
    }
}
