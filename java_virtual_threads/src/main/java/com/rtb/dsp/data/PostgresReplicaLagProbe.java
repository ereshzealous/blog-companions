package com.rtb.dsp.data;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.time.Duration;

/**
 * Replay timestamps alone misreport idle or disconnected replicas. Check receiver state
 * and received/replayed WAL positions first. Timestamp lag includes cross-host clock skew.
 */
public class PostgresReplicaLagProbe implements ReplicaHealthTracker.LagProbe {

    private static final String LAG_SQL = """
            SELECT pg_is_in_recovery() AS in_recovery,
                   COALESCE((SELECT status = 'streaming' FROM pg_stat_wal_receiver LIMIT 1), false) AS streaming,
                   CASE
                     WHEN NOT pg_is_in_recovery() THEN 0
                     WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
                     ELSE COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - pg_last_xact_replay_timestamp()) * 1000, 0)
                   END AS lag_ms
            """;

    private final JdbcTemplate jdbc;

    public PostgresReplicaLagProbe(DataSource replica) {
        this.jdbc = new JdbcTemplate(replica);
        this.jdbc.setQueryTimeout(1);
    }

    @Override
    public Duration measure() {
        return jdbc.queryForObject(LAG_SQL, (rs, row) -> {
            boolean inRecovery = rs.getBoolean("in_recovery");
            if (inRecovery && !rs.getBoolean("streaming")) {
                throw new IllegalStateException("standby is not streaming WAL from the primary");
            }
            return Duration.ofMillis(Math.round(rs.getDouble("lag_ms")));
        });
    }
}
