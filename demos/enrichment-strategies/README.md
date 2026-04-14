# Enrichment Strategies for Apache Flink — Practical Demos

Four concrete Flink implementations of the enrichment strategies described in [Rion Williams's "Prepare for Launch"](https://rion.io/2026/01/27/prepare-for-launch-enrichment-strategies-for-apache-flink/).

The article is architectural. These demos make each strategy runnable on **Confluent Cloud for Apache Flink**, using only:

- **Flink SQL** (declarative)
- **Table API** (Java, imperative)
- **Process Table Functions (PTF)** (Java, stateful)

## Use Case

All four demos share the same problem: enrich an `orders` event stream with `customer` data. The source events carry a `customer_id`; the enrichment side holds the full customer profile (name, tier, country). The variations are in *how* the enrichment state gets populated and *when* events can be emitted.

```
orders (source)                     customers (enrichment side)
  ├── order_id                        ├── customer_id (PK)
  ├── customer_id  ────── JOIN ──────>├── name
  ├── amount                          ├── tier
  └── order_time                      └── country
```

## Strategy Matrix

| # | Strategy                           | Flink mechanic                                                      | Implementation | Best for                                     | Key risk                              |
|---|------------------------------------|---------------------------------------------------------------------|----------------|----------------------------------------------|---------------------------------------|
| 1 | External enrichment (Kafka-only)   | Regular join against upsert-kafka reference                         | SQL            | Reference already in Kafka; self-contained   | Doesn't scale; per-row lookup cost    |
| 2 | Gradual enrichment                 | Event-time temporal join with CDC stream                            | SQL            | Scale; eventual-consistency is acceptable    | Warmup gap — early events miss joins  |
| 3 | Two-phase bootstrap                | Bootstrap statement → live statement                                | Table API      | Correctness-critical; startup delay is OK    | Operational orchestration complexity  |
| 4 | Gated enrichment                   | PTF with buffered state + gate signal                               | PTF            | Strong correctness without bootstrap job     | Buffer growth; gate logic correctness |
| 5 | External enrichment (real DB/API)  | `LATERAL TABLE(KEY_SEARCH_AGG(...))` against a CC **External Table** | SQL            | Reference lives in a real DB / REST API      | External call latency; no cache/TTL   |

## Directory Layout

```
enrichment-strategies/
├── README.md                       # This file
├── setup/
│   ├── create-topics.sql           # Shared topic DDL
│   └── seed-data.sql               # Test data producers
├── 01-external-enrichment/
│   ├── README.md                   # Strategy explanation + tradeoffs
│   └── pipeline.sql                # Flink SQL (lookup pattern)
├── 02-gradual-enrichment/
│   ├── README.md
│   └── pipeline.sql                # Flink SQL (event-time temporal join)
├── 03-two-phase-bootstrap/
│   ├── README.md
│   ├── pom.xml
│   └── src/main/java/.../BootstrapApp.java   # Table API orchestration
├── 04-gated-enrichment/
│   ├── README.md
│   ├── pom.xml
│   ├── src/main/java/.../GatedEnrichmentPTF.java   # PTF implementation
│   └── register.sql                # CREATE FUNCTION + usage
└── 05-external-key-search/
    ├── README.md
    └── pipeline.sql                # CC External Tables + KEY_SEARCH_AGG (REST)
```

## Prerequisites

- A Confluent Cloud environment with a Flink compute pool
- Kafka cluster accessible to Flink (same region)
- `confluent` CLI configured (`confluent flink statement ...`)
- For demos 3 and 4: JDK 17 + Maven (to build JAR artifacts)

## Mapping to the Article

| Article section            | Article claim                                          | Demo fidelity                                                                     |
|----------------------------|---------------------------------------------------------|-----------------------------------------------------------------------------------|
| External enrichment        | "Per-record request against external source"           | **Demo 5** is the canonical path (`KEY_SEARCH_AGG` against a real REST API). **Demo 1** is a Kafka-only approximation for when the reference already lives in a compacted topic. |
| Gradual enrichment         | "Enrichment cache fills in organically"                | Demo 2 shows the warmup gap explicitly in test-data timing                        |
| Two-phase bootstrap        | "State Processor API + savepoint"                      | Demo 3 uses Confluent Cloud statement lifecycle (not savepoints) to orchestrate   |
| Gated enrichment           | "Buffer events until gate opens"                       | Demo 4 is the most direct mapping — PTF state machine is purpose-built for this   |

**Note on two-phase bootstrap:** The article uses the Flink State Processor API to write a savepoint offline. Confluent Cloud abstracts savepoints, so Demo 3 achieves the same *correctness guarantee* (no events processed until enrichment state is fully loaded) via a different mechanism: orchestrated bootstrap statement that runs to completion before the live statement starts.

**Note on external enrichment:** Confluent Cloud provides an official **External Tables** feature (`confluent-jdbc`, `rest`, `mongodb`, `couchbase` connectors) with the `KEY_SEARCH_AGG`, `TEXT_SEARCH_AGG`, and `VECTOR_SEARCH_AGG` table-valued functions. It is the canonical lookup-join path on CC — the `PROCTIME()`/`FOR SYSTEM_TIME AS OF` syntax from OSS Flink is not supported. Demo 5 shows this pattern end-to-end with no credentials required (uses the public Open Library REST API).

## Running the Demos

Each demo directory has its own README with:
1. Setup steps (topics, sample data)
2. The Flink statement(s) or Java code
3. Expected behavior and how to verify
4. Cleanup commands

Start with `setup/create-topics.sql` — topics are shared across demos.
