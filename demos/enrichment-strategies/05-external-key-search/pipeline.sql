-- Demo 5: External enrichment via KEY_SEARCH_AGG against a public REST API.
--
-- Looks up book metadata from openlibrary.org for each "order" title.
-- Runs on Confluent Cloud Flink with zero credentials (public endpoint).

-- ---------------------------------------------------------------------------
-- 1. Connection — public REST endpoint, empty creds
-- ---------------------------------------------------------------------------
CREATE CONNECTION openlibrary_connection
WITH (
  'type' = 'rest',
  'endpoint' = 'https://openlibrary.org/',
  'username' = '',
  'password' = ''
);

-- ---------------------------------------------------------------------------
-- 2. External table — Open Library /search.json endpoint
--
-- Open Library returns JSON like:
--   { "numFound": 42, "docs": [ {"title": "...", "author_name": [...], ...}, ... ] }
-- We map only the fields we care about. `q` is the REST query parameter and
-- serves as the lookup key (marked PRIMARY KEY NOT ENFORCED).
-- ---------------------------------------------------------------------------
CREATE TABLE openlibrary_books_ext (
  numFound BIGINT,
  q STRING NOT NULL,
  docs ARRAY<ROW<
    `author_name`         ARRAY<STRING>,
    `title`               STRING,
    `first_publish_year`  INT
  >>,
  PRIMARY KEY (q) NOT ENFORCED
) WITH (
  'connector' = 'rest',
  'rest.connection' = 'openlibrary_connection',
  'rest.method' = 'GET',
  'rest.path' = 'search.json'
);

-- ---------------------------------------------------------------------------
-- 3. Driving stream — "orders" referencing book titles
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_books_input (
  order_id STRING NOT NULL,
  bookname STRING NOT NULL,
  qty      INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.retention.time' = '1 d'
);

INSERT INTO enrich_demo_books_input VALUES
  ('o-1', 'harry potter and the sorcerers stone', 2),
  ('o-2', 'the lord of the rings', 1),
  ('o-3', 'crucial conversations', 3);

-- ---------------------------------------------------------------------------
-- 4. Enriched sink — one row per matching external document
-- ---------------------------------------------------------------------------
CREATE TABLE enrich_demo_books_enriched (
  order_id            STRING NOT NULL,
  bookname            STRING NOT NULL,
  qty                 INT,
  matched_title       STRING,
  author_name         ARRAY<STRING>,
  first_publish_year  INT,
  PRIMARY KEY (order_id) NOT ENFORCED
) DISTRIBUTED BY HASH(order_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'kafka.retention.time' = '1 d'
);

-- ---------------------------------------------------------------------------
-- 5. The enrichment pipeline
--
-- KEY_SEARCH_AGG(external_table, DESCRIPTOR(input_col), search_col)
--   - input_col   : bookname (from the driving stream)
--   - search_col  : q (the REST query-param column of the external table)
--   - returns a column `search_results` containing matched documents
--
-- CROSS JOIN UNNEST(search_results) flattens the returned array. Open Library
-- returns a single row per call whose `docs` field is itself an array — we
-- pick `docs[1]` (Flink SQL is 1-indexed) to get the top hit.
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_books_enriched
SELECT
  i.order_id,
  i.bookname,
  i.qty,
  r.docs[1].title              AS matched_title,
  r.docs[1].author_name        AS author_name,
  r.docs[1].first_publish_year AS first_publish_year
FROM enrich_demo_books_input
  /*+ OPTIONS('scan.startup.mode' = 'earliest-offset') */ i,
LATERAL TABLE(KEY_SEARCH_AGG(
  openlibrary_books_ext,
  DESCRIPTOR(bookname),
  q,
  MAP[
    'async_enabled',   'true',
    'client_timeout',  '30',
    'max_parallelism', '10',
    'retry_count',     '3'
  ]
))
CROSS JOIN UNNEST(search_results) AS r(numFound, q, docs);
