-- Shared tables for all 4 enrichment-strategy demos.
--
-- Target: Confluent Cloud for Apache Flink
--   Catalog:    current-2025-demo
--   Database:   maestro_gcp (cluster lkc-6vyxz8)
--   Compute:    lfcp-qdn2q6
--
-- Run from the CC Flink SQL shell or via `confluent flink statement create`.
-- On Confluent Cloud, CREATE TABLE automatically provisions the backing Kafka
-- topic — no separate topic creation needed.

-- ---------------------------------------------------------------------------
-- 1. Source event stream: orders (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_orders (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  amount DECIMAL(10, 2),
  order_time TIMESTAMP_LTZ(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '1 d'
);

-- ---------------------------------------------------------------------------
-- 2. Enrichment CDC stream: customers (upsert / compacted)
--    Used by Demo 2 (gradual) and Demo 3 (bootstrap).
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_customers_cdc (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  country STRING,
  updated_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (customer_id) NOT ENFORCED,
  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.retention.time' = '0',
  'kafka.cleanup-policy' = 'compact'
);

-- ---------------------------------------------------------------------------
-- 3. Reference table for external enrichment: customers (compacted, no time)
--    Used by Demo 1. This plays the role of the "external source" that the
--    article describes — we use Kafka instead of JDBC because CC Flink
--    doesn't support JDBC lookup joins.
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_customers_ref (
  customer_id STRING NOT NULL,
  name STRING,
  tier STRING,
  country STRING,
  PRIMARY KEY (customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.retention.time' = '0',
  'kafka.cleanup-policy' = 'compact'
);

-- ---------------------------------------------------------------------------
-- 4. Gate signal stream: used by Demo 4 (PTF gated enrichment)
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_gate_signals (
  partition_key STRING NOT NULL,
  signal_type STRING NOT NULL,    -- 'OPEN' | 'CLOSE'
  signal_time TIMESTAMP_LTZ(3),
  WATERMARK FOR signal_time AS signal_time - INTERVAL '5' SECOND
) DISTRIBUTED BY HASH(partition_key) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '1 d'
);
