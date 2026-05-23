package com.wearable.health;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/users/{userId}")
public class UserController {

    private final JdbcTemplate jdbc;

    public UserController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/dashboard")
    public Map<String, Object> dashboard(@PathVariable String userId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("user", fetchUser(userId));
        out.put("latest_vitals", fetchLatestVitals(userId));
        out.put("open_alerts", fetchOpenAlerts(userId));
        out.put("recent_workouts", fetchRecentWorkouts(userId));
        return out;
    }

    @GetMapping("/vitals")
    public List<Map<String, Object>> vitals(@PathVariable String userId,
                                            @RequestParam(defaultValue = "60") int minutes) {
        return jdbc.queryForList(
            "SELECT ts, hr_avg, hr_min, hr_max, spo2_avg, steps, motion_class " +
            "FROM vitals_rollup_1min WHERE user_id = ?::uuid " +
            "  AND ts > NOW() - (? || ' minutes')::interval " +
            "ORDER BY ts DESC LIMIT 500",
            userId, String.valueOf(minutes)
        );
    }

    @GetMapping("/alerts")
    public List<Map<String, Object>> alerts(@PathVariable String userId,
                                            @RequestParam(defaultValue = "open") String status) {
        return jdbc.queryForList(
            "SELECT a.id, a.status, a.created_at, a.acknowledged_at, " +
            "       e.type AS event_type, e.severity " +
            "FROM alerts a LEFT JOIN events e ON a.event_id = e.id " +
            "WHERE a.user_id = ?::uuid AND a.status = ? " +
            "ORDER BY a.created_at DESC LIMIT 100",
            userId, status
        );
    }

    @PostMapping("/alerts/{alertId}/ack")
    public Map<String, Object> acknowledge(@PathVariable String userId, @PathVariable String alertId) {
        int updated = jdbc.update(
            "UPDATE alerts SET status='acknowledged', acknowledged_at=NOW() " +
            "WHERE id=?::uuid AND user_id=?::uuid AND status='open'",
            alertId, userId
        );
        return Map.of("alert_id", alertId, "updated", updated);
    }

    @GetMapping("/insights")
    public List<Map<String, Object>> insights(@PathVariable String userId,
                                              @RequestParam(defaultValue = "7") int days) {
        return jdbc.queryForList(
            "SELECT day, sleep_score, recovery_score, readiness, summary " +
            "FROM insights_daily WHERE user_id = ?::uuid " +
            "  AND day > CURRENT_DATE - (? || ' days')::interval " +
            "ORDER BY day DESC",
            userId, String.valueOf(days)
        );
    }

    private Map<String, Object> fetchUser(String userId) {
        List<Map<String, Object>> rs = jdbc.queryForList(
            "SELECT id, email, created_at, emergency_contact FROM users WHERE id = ?::uuid",
            userId);
        return rs.isEmpty() ? Collections.emptyMap() : rs.get(0);
    }

    private List<Map<String, Object>> fetchLatestVitals(String userId) {
        return jdbc.queryForList(
            "SELECT ts, hr_avg, spo2_avg, steps, motion_class " +
            "FROM vitals_rollup_1min WHERE user_id = ?::uuid " +
            "ORDER BY ts DESC LIMIT 20",
            userId);
    }

    private List<Map<String, Object>> fetchOpenAlerts(String userId) {
        return jdbc.queryForList(
            "SELECT a.id, a.created_at, e.type, e.severity " +
            "FROM alerts a LEFT JOIN events e ON a.event_id = e.id " +
            "WHERE a.user_id = ?::uuid AND a.status = 'open' " +
            "ORDER BY a.created_at DESC LIMIT 10",
            userId);
    }

    private List<Map<String, Object>> fetchRecentWorkouts(String userId) {
        return jdbc.queryForList(
            "SELECT id, type, start_ts, end_ts, hr_avg, distance_m, calories " +
            "FROM workouts WHERE user_id = ?::uuid ORDER BY start_ts DESC LIMIT 10",
            userId);
    }
}
