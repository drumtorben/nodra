-- Class 3 · streaming aggregation. `jsonl_stream` is a broker-free stand-in for Kafka; with
-- Kafka (feature `kafka`) the source would be:
--   kafka('clicks', brokers => 'kafka:9092', format => 'json',
--         schema => 'ts TIMESTAMP, campaign_id BIGINT, user_id VARCHAR')
SET nodra.trigger = '10s';
SET nodra.merge_interval = '30s';

INSERT INTO delta('/tmp/nodra-demo/gold/hourly_clicks', partition_by => 'day')
SELECT date_trunc('hour', s.ts)    AS hour,
       CAST(s.ts AS DATE)          AS day,
       c.brand,
       count(*)                    AS clicks,
       count(DISTINCT s.user_id)   AS users
FROM jsonl_stream('/tmp/nodra-demo/stream/clicks',
                  schema => 'ts TIMESTAMP, campaign_id BIGINT, user_id VARCHAR') s
JOIN delta('/tmp/nodra-demo/dim/campaigns') c ON s.campaign_id = c.campaign_id
GROUP BY 1, 2, 3;
