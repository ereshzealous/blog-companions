package com.rtb.dsp.config;

import com.rtb.dsp.compute.JfrPinningMonitor;
import com.rtb.dsp.compute.ScopedAuctionFilter;
import com.rtb.dsp.resilience.AdaptiveAimdLimiter;
import com.rtb.dsp.resilience.BoundedWinNoticePublisher;
import com.rtb.dsp.resilience.KafkaWinNoticeSink;
import com.rtb.dsp.resilience.RtbAdmissionFilter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Admission runs before context binding and body parsing.
 */
@Configuration(proxyBeanMethods = false)
public class FlowControlConfig {

    @Bean
    public AdaptiveAimdLimiter adaptiveAimdLimiter(RtbProperties props) {
        RtbProperties.Limiter l = props.limiter();
        return new AdaptiveAimdLimiter(AdaptiveAimdLimiter.Config.fromLittlesLaw(
                l.expectedRps(), l.baselineLatency(), l.minLimit(), l.maxLimit(), l.targetP90(),
                l.backoffRatio(), l.additiveStep(), l.window(), l.minWindowSamples()));
    }

    @Bean
    public FilterRegistrationBean<RtbAdmissionFilter> rtbAdmissionFilter(AdaptiveAimdLimiter limiter,
                                                                         RtbProperties props,
                                                                         MeterRegistry registry) {
        var registration = new FilterRegistrationBean<>(
                new RtbAdmissionFilter(limiter, props.admission().shedResponse(), registry));
        registration.addUrlPatterns("/openrtb/2.6/bid");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    public FilterRegistrationBean<ScopedAuctionFilter> scopedAuctionFilter(RtbProperties props) {
        var registration = new FilterRegistrationBean<>(new ScopedAuctionFilter(props.auction()));
        registration.addUrlPatterns("/openrtb/*");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        return registration;
    }

    @Bean
    @ConditionalOnProperty(prefix = "rtb.pinning", name = "enabled", havingValue = "true", matchIfMissing = true)
    public JfrPinningMonitor jfrPinningMonitor(MeterRegistry registry, RtbProperties props) {
        return new JfrPinningMonitor(registry, props.pinning().threshold(), props.pinning().maxSites());
    }

    @Bean
    public NewTopic winNoticeTopic(RtbProperties props) {
        return TopicBuilder.name(props.winNotices().topic())
                .partitions(props.winNotices().partitions())
                .replicas(1)
                .build();
    }

    @Bean
    public BoundedWinNoticePublisher boundedWinNoticePublisher(KafkaTemplate<String, String> kafka,
                                                               RtbProperties props, MeterRegistry registry) {
        RtbProperties.WinNotices w = props.winNotices();
        return new BoundedWinNoticePublisher(w.queueCapacity(), w.flushInterval(), w.maxBatch(), w.offerTimeout(),
                new KafkaWinNoticeSink(kafka, w.topic(), w.ackTimeout()), registry);
    }
}
