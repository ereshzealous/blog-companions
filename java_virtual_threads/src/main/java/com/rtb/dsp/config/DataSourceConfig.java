package com.rtb.dsp.config;

import com.rtb.dsp.data.PostgresReplicaLagProbe;
import com.rtb.dsp.data.ReplicaHealthTracker;
import com.rtb.dsp.data.ResilientRoutingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

import javax.sql.DataSource;

/**
 * Lazy routing defers pool selection until the transaction read-only flag is available.
 * Eager pool startup avoids synchronized first-use initialization on JDK 21.
 */
@Configuration(proxyBeanMethods = false)
public class DataSourceConfig {

    @Bean
    @ConfigurationProperties("rtb.datasource.primary")
    public HikariConfig primaryHikariConfig() {
        return new HikariConfig();
    }

    @Bean
    @ConfigurationProperties("rtb.datasource.replica")
    public HikariConfig replicaHikariConfig() {
        return new HikariConfig();
    }

    @Bean(destroyMethod = "close")
    public HikariDataSource primaryDataSource(@Qualifier("primaryHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    @Bean(destroyMethod = "close")
    public HikariDataSource replicaDataSource(@Qualifier("replicaHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    @Bean
    public ReplicaHealthTracker replicaHealthTracker(@Qualifier("replicaDataSource") DataSource replica,
                                                     RtbProperties props, MeterRegistry registry) {
        RtbProperties.Replica cfg = props.replica();
        return new ReplicaHealthTracker(new PostgresReplicaLagProbe(replica), cfg.lagThreshold(),
                cfg.probeInterval(), cfg.closeAfterHealthyProbes(), registry);
    }

    @Bean
    public ResilientRoutingDataSource routingDataSource(@Qualifier("primaryDataSource") DataSource primary,
                                                        @Qualifier("replicaDataSource") DataSource replica,
                                                        ReplicaHealthTracker health, MeterRegistry registry) {
        ResilientRoutingDataSource routing = new ResilientRoutingDataSource(primary, replica, health);
        registry.gauge("rtb.datasource.routes.replica", routing, ResilientRoutingDataSource::replicaRoutes);
        registry.gauge("rtb.datasource.routes.primary", routing, ResilientRoutingDataSource::primaryRoutes);
        registry.gauge("rtb.datasource.routes.fallback", routing, ResilientRoutingDataSource::fallbackRoutes);
        return routing;
    }

    @Bean
    @Primary
    public DataSource dataSource(ResilientRoutingDataSource routing) {
        return new LazyConnectionDataSourceProxy(routing);
    }
}
