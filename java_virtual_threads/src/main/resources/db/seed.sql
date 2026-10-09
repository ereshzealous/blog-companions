-- 400 campaigns across eight markets. Campaigns 1..8 are the "hot" ones: highest max bid in
-- their market, so they are the first candidate on most requests and the key a stampede hits.

INSERT INTO campaign_budget (campaign_id, target_country, max_bid_micros, total_cap_micros,
                             remaining_micros, daily_pacing_threshold_micros, spent_today_micros)
SELECT id,
       (ARRAY['US','GB','DE','FR','IN','BR','JP','AU'])[1 + (id - 1) % 8],
       CASE WHEN id <= 8 THEN 9000000 ELSE 1500000 + (id * 7919) % 6000000 END,
       500000000000,
       500000000000 - (id * 104729) % 1000000000,
       50000000000,
       (id * 15485863) % 1000000000
FROM generate_series(1::bigint, 400::bigint) AS id
ON CONFLICT (campaign_id) DO NOTHING;
