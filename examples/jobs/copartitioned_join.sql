-- Class 4 · co-partitioned join (illustrative; not part of the demo data).
-- Both tables are larger than nodra.broadcast_threshold, partitioned by `day`, and joined on
-- it: each worker joins whole days locally. Every day is processed once (batch mode).
INSERT INTO delta('/lake/gold/spend_vs_impressions', partition_by => 'day')
SELECT i.day, i.campaign_id, i.impressions, s.spend
FROM delta('/lake/silver/daily_impressions') i
JOIN delta('/lake/silver/daily_spend') s
  ON i.day = s.day AND i.campaign_id = s.campaign_id;
