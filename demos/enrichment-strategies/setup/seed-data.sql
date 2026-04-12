-- Seed data for all enrichment demos.
--
-- Run these as separate Flink statements (each INSERT is its own job on CC).
-- Timing is deliberate — demos 2 and 4 depend on the ordering between
-- customer records and order events to illustrate the "warmup gap".

-- ---------------------------------------------------------------------------
-- Seed customers into the CDC stream (event-time temporal table)
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_customers_cdc VALUES
  ('c-001', 'Alice Martin',   'GOLD',     'US', TIMESTAMP '2026-04-12 10:00:00.000'),
  ('c-002', 'Bob Chen',       'SILVER',   'SG', TIMESTAMP '2026-04-12 10:00:01.000'),
  ('c-003', 'Carol Dupont',   'PLATINUM', 'FR', TIMESTAMP '2026-04-12 10:00:02.000'),
  ('c-004', 'David Kumar',    'SILVER',   'IN', TIMESTAMP '2026-04-12 10:00:03.000');

-- Later: a tier upgrade (demonstrates versioned temporal join in Demo 2)
INSERT INTO enrich_demo_customers_cdc VALUES
  ('c-002', 'Bob Chen',       'GOLD',     'SG', TIMESTAMP '2026-04-12 10:05:00.000');

-- ---------------------------------------------------------------------------
-- Seed the reference table (used by Demo 1 — no updated_at column)
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_customers_ref VALUES
  ('c-001', 'Alice Martin',   'GOLD',     'US'),
  ('c-002', 'Bob Chen',       'SILVER',   'SG'),
  ('c-003', 'Carol Dupont',   'PLATINUM', 'FR'),
  ('c-004', 'David Kumar',    'SILVER',   'IN');

-- ---------------------------------------------------------------------------
-- Seed orders (source event stream)
-- ---------------------------------------------------------------------------
INSERT INTO enrich_demo_orders VALUES
  ('o-1001', 'c-001',  49.99, TIMESTAMP '2026-04-12 10:01:00.000'),
  ('o-1002', 'c-002',  19.50, TIMESTAMP '2026-04-12 10:02:00.000'),
  ('o-1003', 'c-003', 150.00, TIMESTAMP '2026-04-12 10:03:00.000'),
  ('o-1004', 'c-001',   9.99, TIMESTAMP '2026-04-12 10:04:00.000'),
  ('o-1005', 'c-002',  79.00, TIMESTAMP '2026-04-12 10:06:00.000'),  -- after Bob's upgrade
  ('o-1006', 'c-004',  29.99, TIMESTAMP '2026-04-12 10:07:00.000'),
  ('o-1007', 'c-999', 199.99, TIMESTAMP '2026-04-12 10:08:00.000');  -- unknown customer
