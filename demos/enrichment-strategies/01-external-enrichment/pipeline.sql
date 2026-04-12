-- Demo 1: External Enrichment on Confluent Cloud.
--
-- NOTE: Confluent Cloud Flink does NOT support PROCTIME() — the textbook
-- `FOR SYSTEM_TIME AS OF PROCTIME()` lookup join syntax fails with
-- "Function 'PROCTIME' is not supported in Confluent's Flink SQL dialect".
--
-- Workaround: a regular join between the append orders stream and the
-- upsert-kafka customers_ref table. CC Flink treats the upsert stream as a
-- dynamic table and holds its current state in the join operator. This is
-- semantically equivalent to the article's "lookup with cache" pattern.
--
-- Side effect: the output becomes a changelog stream (updates / retracts),
-- so the sink must be upsert-capable with a PRIMARY KEY.
--
-- Prerequisites: run ../setup/create-tables.sql and seed customers_ref.

-- ---------------------------------------------------------------------------
-- Sink: enriched orders. Upsert mode + PK on order_id.
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_orders_enriched_v1 (
  order_id       STRING NOT NULL,
  customer_id    STRING NOT NULL,
  customer_name  STRING,
  tier           STRING,
  country        STRING,
  amount         DECIMAL(10, 2),
  order_time     TIMESTAMP_LTZ(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.retention.time' = '1 d'
);

-- ---------------------------------------------------------------------------
-- Streaming job: regular LEFT JOIN against upsert-kafka reference table.
--
-- Why LEFT JOIN? So orders with unknown customer_ids still pass through with
-- NULL enrichment — mirrors the article's "cache miss → degraded output".
--
-- Expected warnings on submit:
--   * "primary key does not match the upsert key" — cosmetic
--   * "highly state-intensive operator without TTL" — exactly the article's
--     "doesn't scale" tradeoff
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_orders_enriched_v1
SELECT
  o.order_id,
  o.customer_id,
  c.name        AS customer_name,
  c.tier,
  c.country,
  o.amount,
  o.order_time
FROM enrich_demo_orders
  /*+ OPTIONS('scan.startup.mode' = 'earliest-offset') */ o
LEFT JOIN enrich_demo_customers_ref
  /*+ OPTIONS('scan.startup.mode' = 'earliest-offset') */ c
  ON o.customer_id = c.customer_id;
