-- Class 2 · broadcast join: enrich raw events with campaign data, partition by day.
INSERT INTO delta('/data/lake/silver/events_enriched', partition_by => 'day')
SELECT e.event_id,
       e.ts,
       CAST(e.ts AS DATE) AS day,
       e.campaign_id,
       c.brand,
       c.country
FROM delta('/data/lake/bronze/events') e
JOIN delta('/data/lake/dim/campaigns') c ON e.campaign_id = c.campaign_id
WHERE e.event_type = 'impression';
