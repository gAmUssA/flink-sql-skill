# Demo 3: Two-Phase Bootstrap (Confluent Table API — Java)

> **Article strategy:** Pre-populate enrichment state before live processing begins. The article uses Flink's **State Processor API** to write a savepoint offline, then the live job starts from that savepoint.
>
> **Flink mechanic on Confluent Cloud:** Orchestrate two statements via the **Confluent Table API plugin** — Phase 1 materializes a bounded snapshot of the CDC source; Phase 2 starts the live enrichment job once Phase 1 is complete.

## Why this differs from the article

The article uses the Flink State Processor API to write operator state to a savepoint directly. **Confluent Cloud abstracts savepoints** — you can't write them yourself. But the *semantic* the article is after is:

> "No live events are processed until the full enrichment state is in place."

On Confluent Cloud, we achieve that semantic by splitting the pipeline into two phases and orchestrating them from Java:

```
Phase 1 (bootstrap):
  customers_cdc  ──[scan.bounded.mode = latest-offset]──▶ customers_snapshot
                                                          (compacted, bounded)
  ⏸ wait for COMPLETED
                                                                    ▼
Phase 2 (live):
  orders         ──[event-time temporal JOIN]───────────▶ customers_snapshot
                                                          (now fully loaded)
                                    ▼
                             orders_enriched_v3
```

The key properties:

- Phase 1 reads `customers_cdc` from **earliest-offset** to **latest-offset** (at the moment Phase 1 starts). It's a *bounded* stream — the statement completes when it reaches the end.
- Phase 2 cannot start until Phase 1 is in `COMPLETED` status. This guarantees the snapshot is fully populated.
- The Java orchestration layer checks status via the Confluent Table API plugin and waits.

## Why Table API instead of pure SQL

Pure SQL can declare these statements, but it **can't orchestrate the wait**. The Java Table API plugin provides:

- `ConfluentTools.getStatementName(tableResult)` — capture the statement name after submission
- `TableInfo` APIs — poll statement status
- Programmatic ability to submit Phase 2 conditionally after Phase 1 succeeds

You could hand-run two `confluent flink statement` CLI calls with a `sleep && check-status` loop between them, but that's a shell-based version of the same orchestration — the Java code makes the handoff explicit and reproducible.

## Prerequisites

- Tables from `../setup/create-tables.sql` already exist
- JDK 17 and Maven installed
- Confluent Cloud Flink API key with write access
- Environment variables set (see "Running" below)

## Building

```bash
mvn clean package
```

This produces `target/two-phase-bootstrap-1.0.0.jar`.

## Running

Set the required environment variables (same as any CC Flink Table API app):

```bash
export CLOUD_PROVIDER="gcp"
export CLOUD_REGION="us-east4"
export ORG_ID="3f8fd115-e14d-431a-9ea8-577fa6963a7f"
export ENV_ID="env-j03r6m"
export COMPUTE_POOL_ID="lfcp-qdn2q6"
export FLINK_API_KEY="<your-flink-api-key>"
export FLINK_API_SECRET="<your-flink-api-secret>"
```

Then run:

```bash
java -jar target/two-phase-bootstrap-1.0.0.jar
```

## Expected output

```
[Phase 1] Creating customers_snapshot (bootstrap)...
[Phase 1] Statement submitted: enrich-demo-bootstrap-<uuid>
[Phase 1] Waiting for completion...
[Phase 1] ✓ Completed in 24s
[Phase 2] Starting live enrichment...
[Phase 2] Statement submitted: enrich-demo-live-<uuid>
[Phase 2] ✓ Running. Orders will be enriched against the snapshot.
```

The live statement stays running indefinitely. Kill it with Ctrl-C when you're done observing — the statement continues on Confluent Cloud until explicitly stopped.

## Verifying

```sql
SELECT * FROM enrich_demo_orders_enriched_v3;
```

All orders should be enriched immediately with no warmup gap — that's the point.

## Cleanup

```sql
DROP TABLE IF EXISTS enrich_demo_customers_snapshot;
DROP TABLE IF EXISTS enrich_demo_orders_enriched_v3;
```

Plus delete the two Flink statements (the Java app prints their names, or use `confluent flink statement list`).

## When to use this pattern

Pick this when:
- You have a large CDC history that must be in state before live processing
- Any warmup gap (Demo 2's behavior) is unacceptable
- You can afford a delayed startup (bootstrap takes time proportional to CDC size)

Avoid when:
- CDC history is small (Demo 2 is simpler)
- Live processing needs to start immediately
- Orchestration complexity isn't worth it
