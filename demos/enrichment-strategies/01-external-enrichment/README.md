# Demo 1: External Enrichment (Flink SQL)

> **Article strategy:** Per-record request against an external source (database/API), optionally cached in state with TTLs.
>
> **Flink mechanic:** Temporal join with processing-time lookup against a compacted Kafka reference table.

## The Pattern

The source stream (`enrich_demo_orders`) joins against a reference table (`enrich_demo_customers_ref`) using `FOR SYSTEM_TIME AS OF PROCTIME()`. Each incoming event triggers a *point-in-time* lookup that reads the current value of the reference table at the moment the event is processed.

```
orders  ──────────────▶ [ Flink: proc-time JOIN ] ──▶ enriched orders
                              │
                              ▼
                        customers_ref
                        (compacted Kafka topic, held in task-local state)
```

## Why this represents "external enrichment"

The article describes a per-record API call. Confluent Cloud Flink doesn't support JDBC lookup joins — the closest pattern is a **Kafka-backed lookup** where the reference table lives in a compacted topic. Flink loads it into task-local state and evaluates the join per-row at processing time.

**Behavior matches the article's characterization:**
- ✅ Per-row evaluation
- ✅ Current value at processing time (no versioning)
- ✅ Caching happens automatically in the join operator's state
- ❌ No external network call per row (Kafka is internal)

For a true per-row HTTP call you'd need a Java `AsyncTableFunction` UDF on Confluent Cloud — but UDFs on CC can't make outbound network calls. That's a hard limitation, not a demo artifact.

## Tradeoffs (same as the article)

| Aspect         | Behavior                                                                 |
|----------------|--------------------------------------------------------------------------|
| Consistency    | Reads latest value of reference table at processing time                 |
| Scale          | Doesn't scale — every row triggers a lookup. State grows with ref table  |
| Startup        | Immediate — works as soon as customers_ref has data                      |
| Staleness      | Depends on how fresh the Kafka topic is kept                             |

## Confluent Cloud gotcha

**`PROCTIME()` is NOT supported** in Confluent Cloud's Flink SQL dialect. A textbook `FOR SYSTEM_TIME AS OF PROCTIME()` lookup join fails with:
> `Function 'PROCTIME' is not supported in Confluent's Flink SQL dialect.`

Workaround used here: a **regular join** between the append orders stream and the upsert-kafka `customers_ref` table. CC Flink treats the upsert stream as a dynamic table and maintains its current state in the join operator — semantically the same as the article's "lookup with cache". The tradeoff: the output becomes a changelog stream (update/retract), so the sink must be declared with `changelog.mode = 'upsert'` and a `PRIMARY KEY` (see `pipeline.sql`).

You'll also see two warnings when the statement starts:
- **PK/upsert key mismatch** — cosmetic; output still lands correctly
- **State-intensive operator without TTL** — exactly the article's "state grows" tradeoff

## Observed output (Confluent Cloud run, 2026-04-12)

After seeding 4 reference customers and 7 orders:

```
order_id  customer_id  customer_name  tier      country  amount
────────  ───────────  ─────────────  ────────  ───────  ──────
o-1001    c-001        Alice Martin   GOLD      US        49.99
o-1002    c-002        Bob Chen       SILVER    SG        19.50
o-1003    c-003        Carol Dupont   PLATINUM  FR       150.00
o-1004    c-001        Alice Martin   GOLD      US         9.99
o-1005    c-002        Bob Chen       SILVER    SG        79.00
o-1006    c-004        David Kumar    SILVER    IN        29.99
o-1007    c-999        NULL           NULL      NULL     199.99  ← LEFT JOIN miss
```

All 7 orders emitted. The unknown customer (`c-999`) passes through with `NULL` enrichment columns — the article's "cache miss" case.

## Running the demo

### Prerequisites
- Tables created via `../setup/create-tables.sql`
- Seed data loaded via `../setup/seed-data.sql` (specifically the `enrich_demo_customers_ref` INSERTs)

### Run the enrichment

Execute `pipeline.sql` — this registers a single streaming statement that writes enriched orders to `enrich_demo_orders_enriched_v1`.

### Verify

Produce a new order:

```sql
INSERT INTO enrich_demo_orders VALUES
  ('o-2001', 'c-003', 75.00, NOW());
```

Select from the output:

```sql
SELECT * FROM enrich_demo_orders_enriched_v1;
```

Expected: the order appears with `customer_name`, `tier`, `country` populated.

### The "external failure" case

Produce an order with an unknown customer:

```sql
INSERT INTO enrich_demo_orders VALUES
  ('o-2002', 'c-999', 10.00, NOW());
```

Because we use `LEFT JOIN`, the row appears with `NULL` enrichment columns. In the article's external-API pattern, this would be a cache miss followed by a failed external call — in Flink SQL it's just a missing row.

## Cleanup

```sql
DROP TABLE IF EXISTS enrich_demo_orders_enriched_v1;
```

## When to use this pattern

Pick this when:
- Enrichment data rarely changes and is small enough to fit in task state
- You don't care about the enrichment value at the *time the event happened*, only *now*
- You want the simplest possible SQL

Avoid when:
- You need point-in-time correctness (use Demo 2's event-time temporal join)
- Reference table is very large (state explosion)
- Enrichment data changes rapidly and you need strict ordering
