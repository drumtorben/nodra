-- Class 3 · mergeable aggregation from a stream.
-- Workers write partial aggregate states; worker 0 merges them every minute and replaces
-- only the days that changed.
SET nodra.trigger = '30s';
SET nodra.merge_interval = '60s';

INSERT INTO delta('/data/lake/gold/hourly_brand_counts', partition_by => 'day')
SELECT date_trunc('hour', e.ts)      AS hour,
       CAST(e.ts AS DATE)            AS day,
       c.brand,
       count(*)                      AS requests,
       approx_distinct(e.user_id)    AS users
FROM kafka('events',
           brokers => 'kafka:9092',
           format  => 'json',
           schema  => 'ts TIMESTAMP, campaign_id BIGINT, user_id VARCHAR') e
JOIN delta('/data/lake/dim/campaigns') c ON e.campaign_id = c.campaign_id
GROUP BY 1, 2, 3;
