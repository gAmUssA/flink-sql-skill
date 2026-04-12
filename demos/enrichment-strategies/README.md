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

| # | Strategy            | Flink mechanic                          | Implementation | Best for                                     | Key risk                              |
|---|---------------------|------------------------------------------|----------------|----------------------------------------------|---------------------------------------|
| 1 | External enrichment | Lookup via `FOR SYSTEM_TIME AS OF proc_time` on reference table | SQL            | Low volume, tolerant of staleness            | Doesn't scale; per-row lookup cost    |
| 2 | Gradual enrichment  | Event-time temporal join with CDC stream | SQL            | Scale; eventual-consistency is acceptable    | Warmup gap — early events miss joins  |
| 3 | Two-phase bootstrap | Bootstrap statement → live statement     | Table API      | Correctness-critical; startup delay is OK    | Operational orchestration complexity  |
| 4 | Gated enrichment    | PTF with buffered state + gate signal    | PTF            | Strong correctness without bootstrap job     | Buffer growth; gate logic correctness |

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
└── 04-gated-enrichment/
    ├── README.md
    ├── pom.xml
    ├── src/main/java/.../GatedEnrichmentPTF.java   # PTF implementation
    └── register.sql                # CREATE FUNCTION + usage
```

## Prerequisites

- A Confluent Cloud environment with a Flink compute pool
- Kafka cluster accessible to Flink (same region)
- `confluent` CLI configured (`confluent flink statement ...`)
- For demos 3 and 4: JDK 17 + Maven (to build JAR artifacts)

## Mapping to the Article

| Article section            | Article claim                                          | Demo fidelity                                                                     |
|----------------------------|---------------------------------------------------------|-----------------------------------------------------------------------------------|
| External enrichment        | "Per-record request against external source"           | Demo 1 uses Kafka-backed reference table (CC doesn't support JDBC lookup)         |
| Gradual enrichment         | "Enrichment cache fills in organically"                | Demo 2 shows the warmup gap explicitly in test-data timing                        |
| Two-phase bootstrap        | "State Processor API + savepoint"                      | Demo 3 uses Confluent Cloud statement lifecycle (not savepoints) to orchestrate   |
| Gated enrichment           | "Buffer events until gate opens"                       | Demo 4 is the most direct mapping — PTF state machine is purpose-built for this   |

**Note on two-phase bootstrap:** The article uses the Flink State Processor API to write a savepoint offline. Confluent Cloud abstracts savepoints, so Demo 3 achieves the same *correctness guarantee* (no events processed until enrichment state is fully loaded) via a different mechanism: orchestrated bootstrap statement that runs to completion before the live statement starts.

## Running the Demos

Each demo directory has its own README with:
1. Setup steps (topics, sample data)
2. The Flink statement(s) or Java code
3. Expected behavior and how to verify
4. Cleanup commands

Start with `setup/create-topics.sql` — topics are shared across demos.
