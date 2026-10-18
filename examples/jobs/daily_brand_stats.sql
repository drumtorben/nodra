-- Class 3 · mergeable aggregation over a Delta table.
-- Workers write partial states; the merge step replaces only the days that changed.
INSERT INTO delta('/tmp/nodra-demo/gold/daily_brand_stats', partition_by => 'day')
SELECT e.day,
       c.brand,
       count(*)                   AS events,
       sum(c.cpm) / 1000          AS revenue,
       approx_distinct(e.user_id) AS users
FROM delta('/tmp/nodra-demo/bronze/events') e
JOIN delta('/tmp/nodra-demo/dim/campaigns') c ON e.campaign_id = c.campaign_id
GROUP BY e.day, c.brand;
