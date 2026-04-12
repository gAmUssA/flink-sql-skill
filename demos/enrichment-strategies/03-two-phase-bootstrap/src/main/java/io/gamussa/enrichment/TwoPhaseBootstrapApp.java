package io.gamussa.enrichment;

import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.plugin.ConfluentTools;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Two-phase bootstrap orchestration for enrichment state.
 *
 * Phase 1: materialize a bounded snapshot of customers_cdc into
 *          customers_snapshot. Bounded = the job runs until it reaches
 *          the latest offset that existed when the job started.
 *
 * Phase 2: start the live enrichment job. Because Phase 1 has completed
 *          before Phase 2 begins, the snapshot table is guaranteed to
 *          hold every customer record that existed at snapshot time.
 *
 * This achieves the same correctness guarantee as the article's
 * State Processor API approach, without requiring direct savepoint
 * access (which Confluent Cloud abstracts away).
 */
public final class TwoPhaseBootstrapApp {

    private TwoPhaseBootstrapApp() {}

    public static void main(String[] args) throws Exception {
        final TableEnvironment env = TableEnvironment.create(
            ConfluentSettings.fromGlobalVariables()
        );

        // -------------------------------------------------------------------
        // Phase 0: make sure the snapshot sink table exists. Idempotent.
        // -------------------------------------------------------------------
        env.executeSql(
            "CREATE TABLE IF NOT EXISTS enrich_demo_customers_snapshot (" +
            "  customer_id STRING NOT NULL," +
            "  name STRING," +
            "  tier STRING," +
            "  country STRING," +
            "  updated_at TIMESTAMP_LTZ(3)," +
            "  PRIMARY KEY (customer_id) NOT ENFORCED," +
            "  WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND" +
            ") DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS" +
            " WITH (" +
            "  'changelog.mode' = 'upsert'," +
            "  'kafka.retention.time' = '0'," +
            "  'kafka.cleanup-policy' = 'compact'" +
            ")"
        );

        env.executeSql(
            "CREATE TABLE IF NOT EXISTS enrich_demo_orders_enriched_v3 (" +
            "  order_id STRING NOT NULL," +
            "  customer_id STRING NOT NULL," +
            "  customer_name STRING," +
            "  tier STRING," +
            "  country STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP_LTZ(3)" +
            ") DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS" +
            " WITH (" +
            "  'changelog.mode' = 'append'," +
            "  'kafka.retention.time' = '1 d'" +
            ")"
        );

        // -------------------------------------------------------------------
        // Phase 1: BOUNDED bootstrap from customers_cdc -> customers_snapshot.
        //
        // The hint scan.bounded.mode=latest-offset makes the read bounded:
        // Flink reads everything up to the latest offset at job-start time
        // and then the statement transitions to COMPLETED.
        // -------------------------------------------------------------------
        System.out.println("[Phase 1] Submitting bootstrap statement...");
        TableResult bootstrapResult = env.executeSql(
            "INSERT INTO enrich_demo_customers_snapshot" +
            " SELECT customer_id, name, tier, country, updated_at" +
            " FROM enrich_demo_customers_cdc" +
            "   /*+ OPTIONS(" +
            "     'scan.startup.mode' = 'earliest-offset'," +
            "     'scan.bounded.mode' = 'latest-offset'" +
            "   ) */"
        );

        String bootstrapName = ConfluentTools.getStatementName(bootstrapResult);
        System.out.println("[Phase 1] Statement: " + bootstrapName);
        System.out.println("[Phase 1] Waiting for COMPLETED...");

        bootstrapResult.await();    // blocks until the bounded job finishes
        System.out.println("[Phase 1] ✓ Bootstrap complete.");

        // -------------------------------------------------------------------
        // Phase 2: live enrichment. Orders -> temporal join against the
        // snapshot table. Snapshot is guaranteed fully populated.
        // -------------------------------------------------------------------
        System.out.println("[Phase 2] Submitting live enrichment statement...");
        TableResult liveResult = env.executeSql(
            "INSERT INTO enrich_demo_orders_enriched_v3" +
            " SELECT" +
            "   o.order_id," +
            "   o.customer_id," +
            "   c.name AS customer_name," +
            "   c.tier," +
            "   c.country," +
            "   o.amount," +
            "   o.order_time" +
            " FROM enrich_demo_orders o" +
            " JOIN enrich_demo_customers_snapshot" +
            "   FOR SYSTEM_TIME AS OF o.order_time AS c" +
            "   ON o.customer_id = c.customer_id"
        );

        String liveName = ConfluentTools.getStatementName(liveResult);
        System.out.println("[Phase 2] Statement: " + liveName);
        System.out.println("[Phase 2] ✓ Running. Orders are now being enriched.");
        System.out.println();
        System.out.println("The live statement stays running on Confluent Cloud.");
        System.out.println("To stop it: confluent flink statement delete " + liveName);
    }
}
