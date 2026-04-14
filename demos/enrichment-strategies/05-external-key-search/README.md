# Demo 5: External Enrichment via KEY_SEARCH_AGG (REST connector)

> **Article strategy:** Per-record lookup against an **external** source (database or API), with cache/TTL characteristics. This is the *canonical* implementation of the article's "external enrichment" pattern on Confluent Cloud — a real external call per row, not a Kafka-table approximation.
>
> **Flink mechanic:** `LATERAL TABLE(KEY_SEARCH_AGG(...))` against a Confluent Cloud **External Table** backed by the `rest` connector.

## Why this is the article-faithful version

When the enrichment article talks about "a per-record request against a database or API", that maps 1:1 to Confluent Cloud's **External Tables** feature — specifically the `KEY_SEARCH_AGG` table-valued function. The SQL looks nothing like an OSS Flink `FOR SYSTEM_TIME AS OF PROCTIME()` lookup join because CC doesn't support that syntax — but the semantics are identical:

- ✅ One external call per input row (with built-in async, retry, timeout)
- ✅ Real external network I/O (JDBC, REST, MongoDB, or Couchbase)
- ✅ No Kafka topic needed on the enrichment side
- ✅ No `PROCTIME()` needed — the `LATERAL TABLE` pattern sidesteps that limitation
- ⚠️  No built-in cache/TTL (unlike the OSS JDBC lookup connector)

Compare to Demo 1, which uses an upsert-kafka reference table. That works and is zero-dependency, but it's a *Kafka-only approximation* — the enrichment data has to be in a Kafka topic first. Demo 5 is the "real" external lookup.

## What this demo looks up

We'll use the free, public **Open Library** search API (`openlibrary.org/search.json`) as the external source. Given an order that references a book title, we look up the book's metadata (author, first publish year, etc.) via a live REST call and emit an enriched order.

```
orders (local)          openlibrary.org (external)
  ├── order_id          ├── title
  ├── title  ────── KEY_SEARCH_AGG ────▶ author_name[]
  └── qty               └── first_publish_year
```

No database, no credentials, no secrets — just a public HTTPS endpoint. Great for trying out External Tables on a fresh Confluent Cloud environment.

## Supported connectors (for reference)

`KEY_SEARCH_AGG` works against any of these backends — the demo uses `rest`:

| `connector`      | Dialect / backend                            |
|------------------|-----------------------------------------------|
| `confluent-jdbc` | Postgres, MySQL, SQL Server, Oracle           |
| `rest`           | Any HTTPS JSON endpoint                       |
| `mongodb`        | MongoDB Atlas                                 |
| `couchbase`      | Couchbase                                     |

## The two-step setup

### Step 1 — `CREATE CONNECTION`

The connection holds the endpoint and credentials (credentials are empty for a public API).

```sql
CREATE CONNECTION openlibrary_connection
WITH (
  'type' = 'rest',
  'endpoint' = 'https://openlibrary.org/',
  'username' = '',
  'password' = ''
);
```

> **Naming gotcha:** the `type` in `CREATE CONNECTION` uses an **underscore** when it's `confluent_jdbc`. The `connector` in `CREATE TABLE` uses a **hyphen** (`confluent-jdbc`). This is verbatim from the docs. For `rest`/`mongodb`/`couchbase` there's no hyphen/underscore mismatch.

### Step 2 — `CREATE TABLE` (the external table)

```sql
CREATE TABLE openlibrary_books_ext (
  numFound BIGINT,
  q STRING NOT NULL,
  docs ARRAY<ROW<`author_name` ARRAY<STRING>, `title` STRING, `first_publish_year` INT>>,
  PRIMARY KEY (q) NOT ENFORCED
) WITH (
  'connector' = 'rest',
  'rest.connection' = 'openlibrary_connection',
  'rest.method' = 'GET',
  'rest.path' = 'search.json'
);
```

The `q` column is the REST query parameter (Open Library uses `?q=<term>`). The `PRIMARY KEY` on `q` tells Flink this is the lookup key.

## Running the demo

```bash
# 1. Create the 2 shared tables (input + output)
confluent flink statement create create-book-input < pipeline.sql

# 2. Watch the enriched output
confluent flink statement create verify \
  --sql "SELECT * FROM enrich_demo_books_enriched;"
```

Or paste `pipeline.sql` into the Confluent Cloud SQL workspace.

## Expected output

Given these input titles:

```
harry-potter
wish
crucial conversation
```

The pipeline should produce rows like:

```
title                  | author_name         | first_publish_year
─────────────────────  | ───────────────────  | ──────────────────
harry-potter           | J. K. Rowling       | 1997
wish                   | Barbara O'Connor    | 2016
crucial conversation   | Kerry Patterson     | 2001
```

Results depend on Open Library's current index — they may differ slightly.

## Tuning (optional)

The 4th argument to `KEY_SEARCH_AGG` is a tuning `MAP`:

```sql
LATERAL TABLE(KEY_SEARCH_AGG(
  openlibrary_books_ext,
  DESCRIPTOR(bookname),
  q,
  MAP[
    'async_enabled',   'true',
    'client_timeout',  '30',       -- seconds
    'max_parallelism', '10',       -- concurrent external calls
    'retry_count',     '3'
  ]
))
```

| Option              | Default | Notes                                                      |
|---------------------|---------|-------------------------------------------------------------|
| `async_enabled`     | `true`  | Non-blocking lookups                                        |
| `client_timeout`    | `30`    | Seconds                                                     |
| `max_parallelism`   | `10`    | Only applies when async                                    |
| `retry_count`       | `3`     |                                                              |
| `retry_error_list`  | —       | CSV of error codes; only listed codes retry                |

**No `lookup.cache.*` knobs** — unlike OSS Flink's JDBC lookup connector, CC's external tables don't document a cache/TTL. If the external API changes values mid-stream, results vary; plan accordingly.

## Limitations (from the CC docs)

- **Single-column key only.** `KEY_SEARCH_AGG` doesn't support composite keys.
- **Output is an array.** Always follow with `CROSS JOIN UNNEST(search_results) AS t(...)`.
- **REST-specific:** HTTPS only, no IP-address endpoints, endpoint cannot be under `confluent.cloud`, fixed auth header format.
- **No point-in-time semantics.** Every lookup hits the *current* state of the external source. Replays get "what it says now", not "what it said when the order was placed." (The article flags this exact risk.)
- **External calls dominate cost and latency.** The article's "doesn't scale at high volume" warning applies directly here.

## When to use this pattern

Pick this when:
- Your reference data lives in a **real database or API** and copying it into Kafka isn't feasible
- Low-to-moderate event volume (or tolerant of bounded external-call parallelism)
- Staleness is acceptable (no cache / TTL knobs, no point-in-time guarantees)

Avoid when:
- Reference data is in Kafka already — use Demo 1 (simpler) or Demo 2 (point-in-time)
- High throughput — external calls become the bottleneck and cascade failures to the pipeline
- You need strict consistency on replay — external values change independently

## Cleanup

```sql
DROP TABLE IF EXISTS enrich_demo_books_enriched;
DROP TABLE IF EXISTS enrich_demo_books_input;
DROP TABLE IF EXISTS openlibrary_books_ext;
DROP CONNECTION IF EXISTS openlibrary_connection;
```

## Reference

- [Key Search with External Sources — Confluent Cloud docs](https://docs.confluent.io/cloud/current/ai/external-tables/key-search.html)
- [External Tables overview](https://docs.confluent.io/cloud/current/ai/external-tables/overview.html)
