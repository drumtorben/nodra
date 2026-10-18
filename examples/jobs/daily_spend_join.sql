-- Class 4 · co-partitioned join: both tables are partitioned by `day` and joined on it.
INSERT INTO delta('/data/lake/gold/spend_vs_impressions', partition_by => 'day')
SELECT i.day, i.campaign_id, i.impressions, s.spend
FROM delta('/data/lake/silver/daily_impressions') i
JOIN delta('/data/lake/silver/daily_spend') s
  ON i.day = s.day AND i.campaign_id = s.campaign_id;
