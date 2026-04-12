-- Demo 2: Gradual Enrichment via event-time temporal join.
--
-- Prerequisites: run ../setup/create-tables.sql

-- ---------------------------------------------------------------------------
-- Sink: enriched orders
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_orders_enriched_v2 (
  order_id       STRING NOT NULL,
  customer_id    STRING NOT NULL,
  customer_name  STRING,
  tier           STRING,
  country        STRING,
  amount         DECIMAL(10, 2),
  order_time     TIMESTAMP_LTZ(3),
  customer_version_time TIMESTAMP_LTZ(3)
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '1 d'
);

-- ---------------------------------------------------------------------------
-- Streaming job: event-time temporal join.
--
-- FOR SYSTEM_TIME AS OF o.order_time tells Flink: "look up the version of
-- the customer that was valid at the moment the order was placed."
--
-- Key properties:
--   * enrich_demo_customers_cdc has a PRIMARY KEY and watermark, so Flink
--     treats it as a versioned temporal table.
--   * Unmatched orders (customer not yet in state OR order time < any
--     customer version) don't emit. Switch to LEFT JOIN to pass them through.
--   * Versioning: if c-002's tier changes SILVER -> GOLD at 10:05:00, an
--     order at 10:02:00 joins against SILVER and an order at 10:06:00 joins
--     against GOLD.
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_orders_enriched_v2
SELECT
  o.order_id,
  o.customer_id,
  c.name    AS customer_name,
  c.tier,
  c.country,
  o.amount,
  o.order_time,
  c.updated_at AS customer_version_time
FROM enrich_demo_orders o
JOIN enrich_demo_customers_cdc FOR SYSTEM_TIME AS OF o.order_time AS c
  ON o.customer_id = c.customer_id;
