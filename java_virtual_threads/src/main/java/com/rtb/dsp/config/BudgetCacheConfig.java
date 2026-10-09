package com.rtb.dsp.config;

import com.rtb.dsp.bidding.BiddingService;
import com.rtb.dsp.bidding.CampaignCatalog;
import com.rtb.dsp.data.BudgetLedger;
import com.rtb.dsp.data.CampaignBudgetRepository;
import com.rtb.dsp.data.MultiTierBudgetCache;
import com.rtb.dsp.data.RedisBudgetInvalidationSub;
import com.rtb.dsp.data.WinNoticeLedgerConsumer;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration(proxyBeanMethods = false)
public class BudgetCacheConfig {

    @Bean
    public MultiTierBudgetCache multiTierBudgetCache(RtbProperties props, StringRedisTemplate redis,
                                                     CampaignBudgetRepository repository, MeterRegistry registry) {
        return new MultiTierBudgetCache(props.cache(), redis, repository, registry);
    }

    @Bean
    public RedisBudgetInvalidationSub redisBudgetInvalidationSub(MultiTierBudgetCache cache, MeterRegistry registry) {
        return new RedisBudgetInvalidationSub(cache, registry);
    }

    @Bean
    public RedisMessageListenerContainer budgetInvalidationListener(RedisConnectionFactory connectionFactory,
                                                                    RedisBudgetInvalidationSub subscriber,
                                                                    RtbProperties props) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(subscriber, new ChannelTopic(props.cache().invalidationChannel()));
        return container;
    }

    @Bean
    public BudgetLedger budgetLedger(JdbcTemplate jdbc, StringRedisTemplate redis, RtbProperties props,
                                     MeterRegistry registry) {
        return new BudgetLedger(jdbc, redis, props.cache().invalidationChannel(), registry);
    }

    @Bean
    public WinNoticeLedgerConsumer winNoticeLedgerConsumer(BudgetLedger ledger) {
        return new WinNoticeLedgerConsumer(ledger);
    }

    @Bean
    public CampaignCatalog campaignCatalog(CampaignBudgetRepository repository) {
        return new CampaignCatalog(repository);
    }

    @Bean
    public BiddingService biddingService(CampaignCatalog catalog, MultiTierBudgetCache budgets, RtbProperties props,
                                         MeterRegistry registry) {
        return new BiddingService(catalog, budgets, props.auction().seat(), props.auction().maxCandidates(), registry);
    }
}
