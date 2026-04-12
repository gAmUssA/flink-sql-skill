-- Register the Gated Enrichment PTF and wire it into the pipeline.
--
-- Prerequisites:
--   1. Tables from ../setup/create-tables.sql exist
--   2. The JAR has been built (mvn clean package) and uploaded:
--        confluent flink artifact create enrich-gated-ptf \
--          --cloud gcp --region us-east4 \
--          --artifact-file target/gated-enrichment-1.0.0.jar
--   3. The artifact ID from step 2 is substituted below.

-- ---------------------------------------------------------------------------
-- Sink: enriched orders, with a flag showing whether the order was buffered
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_orders_enriched_v4 (
  order_id       STRING NOT NULL,
  customer_id    STRING NOT NULL,
  customer_name  STRING,
  tier           STRING,
  country        STRING,
  amount         DECIMAL(10, 2),
  order_time     TIMESTAMP_LTZ(3),
  was_buffered   BOOLEAN
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'kafka.retention.time' = '1 d'
);

-- ---------------------------------------------------------------------------
-- Register the PTF
-- Replace <ARTIFACT_ID> with the cfa-xxxxx ID you got from
-- `confluent flink artifact create`.
-- ---------------------------------------------------------------------------
CREATE FUNCTION gated_enrichment
AS 'io.gamussa.enrichment.GatedEnrichmentPtf'
USING JAR 'confluent-artifact://<ARTIFACT_ID>';

-- ---------------------------------------------------------------------------
-- Invoke the PTF with two co-partitioned set-semantic table arguments.
--
-- Both orders and customer updates are PARTITION BY customer_id so that
-- the PTF's per-key state is scoped correctly.
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_orders_enriched_v4
SELECT *
FROM gated_enrichment(
  orders   => TABLE enrich_demo_orders         PARTITION BY customer_id,
  customer => TABLE enrich_demo_customers_cdc  PARTITION BY customer_id,
  uid      => 'gated-enrichment-v1'
);
