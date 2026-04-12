# Demo 2: Gradual Enrichment (Flink SQL)

> **Article strategy:** CDC stream of enrichment data continuously fills Flink state; source events join against whatever is there. Eventual consistency — the enrichment cache fills "organically."
>
> **Flink mechanic:** Event-time temporal join against a versioned upsert-kafka table.

## The Pattern

Two streams:

1. `enrich_demo_customers_cdc` — CDC changelog of customer records, keyed by `customer_id`, with a `PRIMARY KEY` and watermark. Flink treats this as a **versioned table** indexed by event time.
2. `enrich_demo_orders` — append-only order events with their own watermark.

The query joins orders against the customer *version that was valid at the moment the order occurred* using `FOR SYSTEM_TIME AS OF o.order_time`.

```
customers_cdc ──▶ versioned state ──┐
                                     ▶ [ event-time temporal JOIN ] ──▶ enriched orders
orders        ──────────────────────┘
```

## How this matches the article

- ✅ Enrichment data arrives as a stream and is stored in Flink state
- ✅ State fills over time — early orders may arrive before any customer data exists
- ✅ Eventual consistency guarantees: once a customer record is in state, all orders with `order_time >= updated_at` join correctly
- ✅ No external calls — everything happens on local state

## The warmup gap (the article's key caveat)

If orders arrive **before** their corresponding customer records, the join will not produce those rows until the customer record arrives *and its event time is earlier than the order's event time*. With an `INNER JOIN` (used here), such orders are dropped entirely. With `LEFT JOIN`, they pass through with NULL enrichment.

This demo deliberately uses `INNER JOIN` so you can *see* the warmup gap by ordering inserts carefully:

| Step | Insert                                                    | Result                                              |
|------|-----------------------------------------------------------|-----------------------------------------------------|
| 1    | Order `o-1001` (customer c-001 @ 10:01:00)                | No output (c-001 not yet in state)                  |
| 2    | Customer c-001 @ 10:00:00 arrives in CDC                  | Order `o-1001` now emits with c-001's data          |
| 3    | Order `o-1002` arrives (customer c-002 @ 10:02:00)        | No output yet                                       |
| 4    | Customer c-002 @ 10:00:01 arrives                         | Order `o-1002` emits                                |

This demonstrates the article's "eventual convergence" property — the system will converge once enough CDC data has accumulated.

## Versioned lookups

Customer `c-002` gets upgraded from `SILVER` to `GOLD` at `10:05:00`. An order at `10:02:00` joins against the `SILVER` version; an order at `10:06:00` joins against the `GOLD` version. This is the **point-in-time correctness** that Demo 1 can't give you.

## Running the demo

### Prerequisites
- `../setup/create-tables.sql` has been executed
- `../setup/seed-data.sql` INSERTs for `enrich_demo_customers_cdc` are loaded
- **Do not** seed orders until after the pipeline is running (to see the warmup behavior)

### Deploy the pipeline

Run `pipeline.sql`.

### Trigger the warmup behavior

Produce orders one at a time and observe the output:

```sql
-- Order before any customer exists
INSERT INTO enrich_demo_orders VALUES
  ('o-warm-1', 'c-005', 10.00, TIMESTAMP '2026-04-12 11:00:00.000');

-- No output yet. Now add the customer:
INSERT INTO enrich_demo_customers_cdc VALUES
  ('c-005', 'Eve Nakamura', 'BRONZE', 'JP', TIMESTAMP '2026-04-12 10:59:00.000');

-- Watermarks need to advance — produce one more order to push time forward:
INSERT INTO enrich_demo_orders VALUES
  ('o-warm-2', 'c-005', 20.00, TIMESTAMP '2026-04-12 11:01:00.000');
```

### Verify

```sql
SELECT order_id, customer_id, customer_name, tier, order_time
FROM enrich_demo_orders_enriched_v2;
```

Both orders should eventually appear with enrichment.

## Observed output (Confluent Cloud run, 2026-04-12)

Seed data: 4 customers in the CDC stream plus an **upgrade** for `c-002` (SILVER → GOLD) at `10:05:00`. Then 7 orders spanning `10:01` through `10:08`.

```
order_id  customer  name          tier      amount  version_time
────────  ────────  ────────────  ────────  ──────  ────────────────────
o-1001    c-001     Alice Martin  GOLD       49.99  2026-04-12 10:00:00
o-1004    c-001     Alice Martin  GOLD        9.99  2026-04-12 10:00:00
o-1005    c-002     Bob Chen      GOLD       79.00  2026-04-12 10:05:00  ← UPGRADED
o-1003    c-003     Carol Dupont  PLATINUM  150.00  2026-04-12 10:00:02
o-1006    c-004     David Kumar   SILVER     29.99  2026-04-12 10:00:03
```

**Key observations:**

1. **Point-in-time correctness** — `o-1005` (order at `10:06`) correctly joins against Bob's **GOLD** version from `10:05:00`, not the earlier SILVER version. This is the property Demo 1 can't give you.
2. **Unknown customer dropped** — `o-1007` (`c-999`) is absent. `INNER JOIN` drops unmatched rows. Switch to `LEFT JOIN` if you want them to pass through with NULL enrichment.
3. **Early-version matching** — orders at `10:01`-`10:04` joined against customer versions from `10:00:00`-`10:00:03`, demonstrating that Flink maintains the full version history of the upsert stream.

## Cleanup

```sql
DROP TABLE IF EXISTS enrich_demo_orders_enriched_v2;
```

## When to use this pattern

Pick this when:
- You need point-in-time correctness (join against the version valid when the event occurred)
- You have a reliable CDC stream for the enrichment side
- Eventual consistency during warmup is acceptable

Avoid when:
- You can't tolerate dropped/delayed events during warmup (use Demo 3 or 4)
- You don't have event-time information on the enrichment side
