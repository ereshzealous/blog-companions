package com.rtb.dsp;

import com.rtb.dsp.data.MultiTierBudgetCache;
import com.rtb.dsp.data.ReplicaHealthTracker;
import com.rtb.dsp.data.ResilientRoutingDataSource;
import com.rtb.dsp.model.OpenRtbBidRequest;
import com.rtb.dsp.model.OpenRtbBidResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Primary and replica pools share one PostgreSQL container; this tests routing, not replication.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "rtb.replica.probe-interval=100ms",
        "rtb.auction.default-tmax=1000ms",
        "rtb.auction.network-margin=0ms",
        "rtb.cache.single-flight-timeout=2s",
        "spring.data.redis.timeout=2s"
})
class RtbDspApplicationIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("dsp_db")
            .withUsername("dsp_user")
            .withPassword("dsp_pass")
            .withCopyFileToContainer(MountableFile.forClasspathResource("db/schema.sql"),
                    "/docker-entrypoint-initdb.d/10-schema.sql")
            .withCopyFileToContainer(MountableFile.forClasspathResource("db/seed.sql"),
                    "/docker-entrypoint-initdb.d/20-seed.sql");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.2-alpine").withExposedPorts(6379);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("rtb.datasource.primary.jdbc-url", POSTGRES::getJdbcUrl);
        registry.add("rtb.datasource.replica.jdbc-url", POSTGRES::getJdbcUrl);
        registry.add("rtb.datasource.replica.minimum-idle", () -> "2");
        registry.add("rtb.datasource.primary.minimum-idle", () -> "2");
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    TestRestTemplate http;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    StringRedisTemplate redis;
    @Autowired
    ReplicaHealthTracker replicaHealth;
    @Autowired
    ResilientRoutingDataSource routing;
    @Autowired
    MultiTierBudgetCache budgets;
    @Autowired
    @Qualifier("replicaDataSource")
    DataSource replicaDataSource;

    @BeforeEach
    void replicaIsTrusted() {
        await(Duration.ofSeconds(10), replicaHealth::isReplicaUsable);
    }

    private ResponseEntity<OpenRtbBidResponse> bid(String auctionId, String country, long floorMicros) {
        var request = new OpenRtbBidRequest(auctionId, "exchange-it",
                new OpenRtbBidRequest.Imp("1", new OpenRtbBidRequest.Banner(300, 250), null),
                new OpenRtbBidRequest.Geo(country, "CA", 37.77, -122.41), floorMicros, 0);
        var headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.add("x-openrtb-version", "2.6");
        return http.postForEntity("/openrtb/2.6/bid", new HttpEntity<>(request, headers), OpenRtbBidResponse.class);
    }

    @Test
    void bidsFromTheDatabaseThenFromL1() {
        long replicaRoutesBefore = routing.replicaRoutes();

        ResponseEntity<OpenRtbBidResponse> first = bid("it-1", "JP", 1_000_000);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.OK);
        OpenRtbBidResponse body = first.getBody();
        assertThat(body).isNotNull();
        assertThat(body.campaignId()).isEqualTo(7);
        assertThat(body.bidPriceMicros()).isEqualTo(7_650_000);
        assertThat(body.latency().budgetTier()).isIn("DB", "L2");

        ResponseEntity<OpenRtbBidResponse> second = bid("it-2", "JP", 1_000_000);
        assertThat(second.getBody()).isNotNull();
        assertThat(second.getBody().latency().budgetTier()).isEqualTo("L1");
        assertThat(routing.replicaRoutes()).isGreaterThan(replicaRoutesBefore);
    }

    @Test
    void floorAboveEveryCampaignIsANoBid() {
        assertThat(bid("it-floor", "US", 50_000_000).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    @Test
    void sharedLoaderSqlHasAnIndependentServerTimeout() {
        JdbcTemplate replica = new JdbcTemplate(replicaDataSource);
        assertThat(replica.queryForObject("SHOW statement_timeout", String.class)).isEqualTo("1s");
        assertThatThrownBy(() -> replica.execute("SELECT pg_sleep(3)"))
                .hasRootCauseMessage("ERROR: canceling statement due to statement timeout");
    }

    @Test
    void winNoticeSettlesOnceAndInvalidatesTheCaches() {
        assertThat(bid("it-3", "DE", 1_000_000).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(bid("it-4", "DE", 1_000_000).getBody().latency().budgetTier()).isEqualTo("L1");
        long before = remaining(3);

        String win = "/openrtb/2.6/win?auctionId=it-win-1&campaignId=3&priceMicros=7650000";
        assertThat(http.getForEntity(win, Void.class).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(http.getForEntity(win, Void.class).getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        await(Duration.ofSeconds(30), () -> remaining(3) == before - 7_650_000);
        await(Duration.ofSeconds(10), () -> !"L1".equals(bid("it-5", "DE", 1_000_000).getBody().latency().budgetTier()));

        Integer ledgerRows = jdbc.queryForObject("SELECT count(*) FROM win_ledger WHERE auction_id = 'it-win-1'", Integer.class);
        assertThat(ledgerRows).isEqualTo(1);
        assertThat(remaining(3)).isEqualTo(before - 7_650_000);
    }

    private long remaining(long campaignId) {
        Long value = jdbc.queryForObject("SELECT remaining_micros FROM campaign_budget WHERE campaign_id = ?",
                Long.class, campaignId);
        return value == null ? -1 : value;
    }

    private static void await(Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(condition.getAsBoolean()).as("condition within %s", timeout).isTrue();
    }
}
