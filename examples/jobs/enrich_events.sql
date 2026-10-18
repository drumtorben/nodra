-- Class 2 · broadcast join: enrich raw events with campaign attributes.
-- Data: `cargo run --example demo_data -- /tmp/nodra-demo`
INSERT INTO delta('/tmp/nodra-demo/silver/events_enriched', partition_by => 'day')
SELECT e.event_id,
       e.ts,
       e.day,
       e.campaign_id,
       c.brand,
       c.country
FROM delta('/tmp/nodra-demo/bronze/events') e
JOIN delta('/tmp/nodra-demo/dim/campaigns') c ON e.campaign_id = c.campaign_id
WHERE e.event_type = 'impression';
