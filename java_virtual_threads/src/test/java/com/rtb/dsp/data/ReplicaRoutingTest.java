package com.rtb.dsp.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplicaRoutingTest {

    private DataSource primary;
    private DataSource replica;
    private ReplicaHealthTracker health;
    private ResilientRoutingDataSource routing;

    @BeforeEach
    void setUp() throws SQLException {
        primary = mock(DataSource.class);
        replica = mock(DataSource.class);
        when(primary.getConnection()).thenAnswer(i -> mock(Connection.class));
        when(replica.getConnection()).thenAnswer(i -> mock(Connection.class));
        health = new ReplicaHealthTracker(() -> Duration.ofMillis(3), Duration.ofMillis(200), Duration.ofSeconds(1), 1, null);
        health.probeOnce();
        routing = new ResilientRoutingDataSource(primary, replica, health);
    }

    @Test
    void withoutLazyProxyReadOnlyTransactionsSilentlyLandOnPrimary() throws SQLException {
        readOnlyTransaction(routing);

        verify(primary).getConnection();
        verify(replica, never()).getConnection();
    }

    @Test
    void lazyProxyLetsReadOnlyTransactionsReachTheReplica() throws SQLException {
        readOnlyTransaction(lazy(routing));

        verify(replica).getConnection();
        verify(primary, never()).getConnection();
        assertThat(routing.replicaRoutes()).isEqualTo(1);
    }

    @Test
    void readWriteTransactionsAlwaysUsePrimary() throws SQLException {
        DataSource ds = lazy(routing);
        new TransactionTemplate(new DataSourceTransactionManager(ds)).executeWithoutResult(status -> touch(ds));

        verify(primary).getConnection();
        verify(replica, never()).getConnection();
    }

    @Test
    void openReplicaCircuitFallsBackToPrimary() throws SQLException {
        var lagging = new ReplicaHealthTracker(() -> Duration.ofMillis(900), Duration.ofMillis(200), Duration.ofSeconds(1), 1, null);
        lagging.probeOnce();
        var fallbackRouting = new ResilientRoutingDataSource(primary, replica, lagging);

        readOnlyTransaction(lazy(fallbackRouting));

        verify(primary).getConnection();
        verify(replica, never()).getConnection();
        assertThat(fallbackRouting.fallbackRoutes()).isEqualTo(1);
    }

    private static LazyConnectionDataSourceProxy lazy(DataSource target) {
        var proxy = new LazyConnectionDataSourceProxy(target);
        // Known defaults, so the proxy does not borrow a connection just to discover them.
        proxy.setDefaultAutoCommit(true);
        proxy.setDefaultTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return proxy;
    }

    private static void readOnlyTransaction(DataSource ds) {
        var tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
        tx.setReadOnly(true);
        tx.executeWithoutResult(status -> touch(ds));
    }

    private static void touch(DataSource ds) {
        try {
            DataSourceUtils.getConnection(ds).createStatement();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
