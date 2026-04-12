# Demo 4: Gated Enrichment (Process Table Function)

> **Article strategy:** The job consumes source events and enrichment data simultaneously, but **delays emitting results** until the enrichment state reaches a "ready" condition. Source events are buffered; when the gate opens, buffered events are flushed.
>
> **Flink mechanic:** A Process Table Function (PTF) with two set-semantic table inputs — orders and customer updates — partitioned by `customer_id`. Per-customer gating: each customer's orders wait until that customer's enrichment record has been seen.

## Why PTF is the right tool

The article notes that gated enrichment is "discussed far more often than successfully implemented in production" because getting the state machine right is hard. PTFs are purpose-built for exactly this kind of stateful, cross-input coordination:

- **Two table inputs** with co-partitioning (both `PARTITION BY customer_id`)
- **`ValueState<CustomerProfile>`** holds the enrichment (the gate)
- **`ListView<PendingOrder>`** holds the buffer
- **Per-key scope** — each `customer_id` gets its own independent gate

The PTF framework gives you what the article calls "significant complexity around getting gating conditions right" for free.

## The state machine

```
For each customer_id, independently:

     ┌───────────────────┐    customer update arrives
     │  GATE CLOSED      │────────────────────────────┐
     │  (buffering)      │                            │
     └───────────────────┘                            │
              │                                       │
              │ order arrives                         │
              │                                       ▼
              │  buffer it         ┌───────────────────────────────┐
              └──────────────────▶ │  FLUSH                        │
                                   │  1. save profile to state    │
                                   │  2. emit buffered orders      │
                                   │     (was_buffered = true)     │
                                   └───────────────────────────────┘
                                              │
                                              ▼
                                   ┌───────────────────┐
                                   │  GATE OPEN        │
                                   │  (pass-through)   │
                                   └───────────────────┘
                                       │
                                       │ order arrives
                                       │  → emit immediately
                                       │    (was_buffered = false)
                                       ▼
```

## How this matches the article

| Article claim                                  | This demo                                                                |
|-----------------------------------------------|---------------------------------------------------------------------------|
| Consumes source + enrichment simultaneously   | Two co-partitioned set-semantic table inputs                              |
| Buffers events while gate is closed           | `ListView<PendingOrder>` holds orders with no known profile               |
| Gate opens on a "ready" signal                | First customer record for that customer_id                                |
| Flushes buffer on gate open                   | PTF iterates `pendingOrders` and emits enriched rows                      |
| Processing proceeds normally after            | Subsequent orders bypass the buffer and emit immediately                  |
| Strong correctness guarantees                 | Per-customer — no order emits until its customer profile exists in state  |

The article also mentions a *global* gate (one gate-open signal releases all partitions). You could build that by co-partitioning on a constant key, but that defeats parallelism. The per-key gate shown here is what's actually useful in production.

## Project structure

```
04-gated-enrichment/
├── pom.xml
├── register.sql                  # CREATE FUNCTION + SELECT usage
└── src/main/java/io/gamussa/enrichment/
    └── GatedEnrichmentPtf.java
```

## Building

```bash
mvn clean package
```

Produces `target/gated-enrichment-1.0.0.jar`.

## Deploying to Confluent Cloud

Step 1 — Upload the artifact:

```bash
confluent flink artifact create enrich-gated-ptf \
  --cloud gcp --region us-east4 \
  --artifact-file target/gated-enrichment-1.0.0.jar
```

Capture the returned artifact ID (something like `cfa-xxxxx`).

Step 2 — Register the function and create the sink table + insert statement via `register.sql`. Edit the `USING JAR 'confluent-artifact://cfa-xxxxx'` line to use your artifact ID, then run the statements.

## Verifying

Produce orders **before** any customer records exist for a new `customer_id`:

```sql
-- These get buffered (gate closed for c-010)
INSERT INTO enrich_demo_orders VALUES
  ('o-gate-1', 'c-010', 25.00, TIMESTAMP '2026-04-12 12:00:00.000'),
  ('o-gate-2', 'c-010', 50.00, TIMESTAMP '2026-04-12 12:00:10.000');

-- No output in enrich_demo_orders_enriched_v4 yet.

-- Now open the gate by inserting the customer:
INSERT INTO enrich_demo_customers_cdc VALUES
  ('c-010', 'Fiona Park', 'GOLD', 'KR', TIMESTAMP '2026-04-12 11:59:00.000');

-- Both orders emit, with was_buffered = true.

-- A new order now passes through immediately:
INSERT INTO enrich_demo_orders VALUES
  ('o-gate-3', 'c-010', 75.00, TIMESTAMP '2026-04-12 12:01:00.000');
-- Emits with was_buffered = false.
```

Check the output:

```sql
SELECT order_id, customer_name, tier, amount, was_buffered
FROM enrich_demo_orders_enriched_v4
WHERE customer_id = 'c-010';
```

Expected:
```
o-gate-1 | Fiona Park | GOLD | 25.00 | true
o-gate-2 | Fiona Park | GOLD | 50.00 | true
o-gate-3 | Fiona Park | GOLD | 75.00 | false
```

## Cleanup

```sql
DROP TABLE IF EXISTS enrich_demo_orders_enriched_v4;
DROP FUNCTION IF EXISTS gated_enrichment;
```

Then delete the artifact:
```bash
confluent flink artifact delete <cfa-id>
```

## When to use this pattern

Pick this when:
- You need strong per-key correctness without a full bootstrap phase
- The enrichment side is a stream (not a static table)
- You're comfortable reasoning about PTF state machines

Avoid when:
- A temporal join (Demo 2) already gives you enough correctness
- Buffer growth could be unbounded (add a max-buffer-size guard or TTL)
- You want pure SQL with no JAR artifacts
