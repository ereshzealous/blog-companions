package com.rtb.dsp.data;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Requires LazyConnectionDataSourceProxy: the read-only transaction flag is published
 * after doBegin requests a connection. Unhealthy replicas fall back to the primary.
 */
public class ResilientRoutingDataSource extends AbstractRoutingDataSource {

    public enum Route { PRIMARY, REPLICA }

    private final ReplicaHealthTracker health;
    private final LongAdder replicaRoutes = new LongAdder();
    private final LongAdder primaryRoutes = new LongAdder();
    private final LongAdder fallbackRoutes = new LongAdder();

    public ResilientRoutingDataSource(DataSource primary, DataSource replica, ReplicaHealthTracker health) {
        this.health = health;
        setTargetDataSources(Map.of(Route.PRIMARY, primary, Route.REPLICA, replica));
        setDefaultTargetDataSource(primary);
        setLenientFallback(false);
        afterPropertiesSet();
    }

    @Override
    protected Object determineCurrentLookupKey() {
        if (!TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
            primaryRoutes.increment();
            return Route.PRIMARY;
        }
        if (health.isReplicaUsable()) {
            replicaRoutes.increment();
            return Route.REPLICA;
        }
        fallbackRoutes.increment();
        return Route.PRIMARY;
    }

    public long replicaRoutes() {
        return replicaRoutes.sum();
    }

    public long primaryRoutes() {
        return primaryRoutes.sum();
    }

    /** Read-only transactions that wanted the replica but were sent to the primary. */
    public long fallbackRoutes() {
        return fallbackRoutes.sum();
    }
}
