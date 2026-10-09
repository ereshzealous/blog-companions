package com.rtb.dsp.compute;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * On JDK 21, both VirtualThreadPinned and virtual-thread JavaMonitorEnter events hold carriers.
 * Events are attributed to the first application frame, with bounded metric cardinality.
 */
@Slf4j
public class JfrPinningMonitor implements SmartLifecycle {

    public static final String PINNED = "jdk.VirtualThreadPinned";
    public static final String MONITOR_ENTER = "jdk.JavaMonitorEnter";
    private static final String OTHER = "other";

    private final MeterRegistry registry;
    private final Duration threshold;
    private final int maxSites;
    private final Map<String, Site> pinnedSites = new ConcurrentHashMap<>();
    private final Map<String, Site> blockedSites = new ConcurrentHashMap<>();
    private final Timer pinnedDuration;
    private volatile RecordingStream stream;

    public JfrPinningMonitor(MeterRegistry registry, Duration threshold, int maxSites) {
        this.registry = registry;
        this.threshold = threshold;
        this.maxSites = maxSites;
        this.pinnedDuration = Timer.builder("rtb.vthread.pinned.duration")
                .description("How long virtual threads stayed pinned to their carrier")
                .publishPercentiles(0.5, 0.99)
                .register(registry);
    }

    @Override
    public void start() {
        RecordingStream rs = new RecordingStream();
        rs.enable(PINNED).withThreshold(threshold).withStackTrace();
        rs.enable(MONITOR_ENTER).withThreshold(threshold).withStackTrace();
        rs.setMaxAge(Duration.ofSeconds(30));
        rs.onEvent(PINNED, this::onPinned);
        rs.onEvent(MONITOR_ENTER, this::onMonitorEnter);
        rs.startAsync();
        stream = rs;
        log.info("JFR carrier monitor streaming {} and {} with threshold {}", PINNED, MONITOR_ENTER, threshold);
    }

    @Override
    public void stop() {
        RecordingStream rs = stream;
        stream = null;
        if (rs != null) {
            rs.close();
        }
    }

    @Override
    public boolean isRunning() {
        return stream != null;
    }

    void onPinned(RecordedEvent event) {
        record(pinnedSites, "rtb.vthread.pinned", "jdk.VirtualThreadPinned events by first application frame", event);
        pinnedDuration.record(event.getDuration());
    }

    void onMonitorEnter(RecordedEvent event) {
        RecordedThread thread = event.getThread();
        if (thread == null || !thread.isVirtual()) {
            return;
        }
        record(blockedSites, "rtb.vthread.monitor.blocked",
                "Virtual threads blocked entering a contended monitor, by first application frame", event);
    }

    private void record(Map<String, Site> sites, String metric, String description, RecordedEvent event) {
        String key = siteOf(event.getStackTrace());
        if (!sites.containsKey(key) && sites.size() >= maxSites) {
            key = OTHER;
        }
        Site site = sites.computeIfAbsent(key, s -> new Site(s, Counter.builder(metric)
                .description(description)
                .tag("site", s)
                .register(registry)));
        site.record();
        if (site.events.sum() == 1) {
            log.warn("{} for {} ms at {}\n{}", event.getEventType().getName(), event.getDuration().toMillis(), key,
                    render(event.getStackTrace()));
        }
    }

    public long totalPinnedEvents() {
        return pinnedSites.values().stream().mapToLong(s -> s.events.sum()).sum();
    }

    public long totalMonitorBlockedEvents() {
        return blockedSites.values().stream().mapToLong(s -> s.events.sum()).sum();
    }

    public Map<String, Long> eventsBySite() {
        return pinnedSites.values().stream().collect(Collectors.toUnmodifiableMap(s -> s.site, s -> s.events.sum()));
    }

    static String siteOf(RecordedStackTrace trace) {
        if (trace == null || trace.getFrames().isEmpty()) {
            return "unknown";
        }
        List<RecordedFrame> frames = trace.getFrames();
        for (RecordedFrame frame : frames) {
            String type = frame.getMethod().getType().getName();
            if (!type.startsWith("java.") && !type.startsWith("jdk.") && !type.startsWith("sun.")) {
                return describe(frame);
            }
        }
        return describe(frames.getFirst());
    }

    private static String describe(RecordedFrame frame) {
        String type = frame.getMethod().getType().getName();
        return type.substring(type.lastIndexOf('.') + 1) + "." + frame.getMethod().getName() + ":" + frame.getLineNumber();
    }

    private static String render(RecordedStackTrace trace) {
        if (trace == null) {
            return "  (no stack trace)";
        }
        return trace.getFrames().stream().limit(24)
                .map(f -> "  at " + f.getMethod().getType().getName() + "." + f.getMethod().getName() + ":" + f.getLineNumber())
                .collect(Collectors.joining("\n"));
    }

    private static final class Site {
        final String site;
        final Counter counter;
        final LongAdder events = new LongAdder();

        Site(String site, Counter counter) {
            this.site = site;
            this.counter = counter;
        }

        void record() {
            events.increment();
            counter.increment();
        }
    }
}
