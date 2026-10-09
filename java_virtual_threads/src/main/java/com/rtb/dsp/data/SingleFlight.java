package com.rtb.dsp.data;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Loads run outside map locks on detached virtual threads. A caller timeout abandons
 * only its own wait; it must not cancel the shared future.
 */
public final class SingleFlight<K, V> {

    private final ConcurrentHashMap<K, CompletableFuture<V>> inFlight = new ConcurrentHashMap<>();
    private final ThreadFactory loaders;
    private final LongAdder flights = new LongAdder();
    private final LongAdder joined = new LongAdder();

    public SingleFlight(String name) {
        this.loaders = Thread.ofVirtual().name("singleflight-" + name + "-", 0).factory();
    }

    /**
     * Returns the value for {@code key}, starting {@code loader} only if no load for that key is
     * already running.
     *
     * @throws TimeoutException if the shared load does not finish within {@code timeout}
     * @throws Exception        whatever the shared load threw, unwrapped
     */
    public V execute(K key, Callable<? extends V> loader, Duration timeout) throws Exception {
        CompletableFuture<V> call = inFlight.get(key);
        if (call == null) {
            CompletableFuture<V> mine = new CompletableFuture<>();
            call = inFlight.putIfAbsent(key, mine);
            if (call == null) {
                call = mine;
                flights.increment();
                loaders.newThread(() -> runLoad(key, mine, loader)).start();
            } else {
                joined.increment();
            }
        } else {
            joined.increment();
        }
        return await(call, timeout);
    }

    private void runLoad(K key, CompletableFuture<V> call, Callable<? extends V> loader) {
        try {
            call.complete(loader.call());
        } catch (Throwable t) {
            call.completeExceptionally(t);
        } finally {
            // Complete before removal so racing callers do not start a duplicate load.
            inFlight.remove(key, call);
        }
    }

    private V await(CompletableFuture<V> call, Duration timeout) throws Exception {
        try {
            return call.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }

    public long flights() {
        return flights.sum();
    }

    public long joined() {
        return joined.sum();
    }

    public int inFlightKeys() {
        return inFlight.size();
    }
}
