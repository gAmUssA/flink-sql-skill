# E-Commerce Order Enrichment Pipeline

## Problem Description

An e-commerce platform needs to build a real-time order enrichment pipeline using Flink SQL. The platform has several data sources:

1. **Orders stream** — Kafka topic `orders` with fields: `order_id` (STRING), `customer_id` (STRING), `product_id` (STRING), `quantity` (INT), `unit_price` (DECIMAL(10,2)), `currency` (STRING), `order_time` (TIMESTAMP(3)). Orders can arrive up to 5 seconds late.

2. **Shipments stream** — Kafka topic `shipments` with fields: `shipment_id` (STRING), `order_id` (STRING), `warehouse_id` (STRING), `ship_time` (TIMESTAMP(3)). A shipment for an order always happens within 48 hours of the order.

3. **Currency rates table** — Kafka topic `currency_rates` with fields: `currency` (STRING), `rate_to_usd` (DECIMAL(10,6)), `update_time` (TIMESTAMP(3)). This is a versioned table (primary key on currency) that receives rate updates.

4. **Customer profiles** — An external MySQL database table that can be used as a lookup dimension. Fields: `customer_id` (STRING), `name` (STRING), `tier` (STRING), `country` (STRING).

5. **Customer CDC stream** — Kafka topic `customer_changes` carrying Debezium CDC events from the customer database, with fields: `customer_id` (INT), `name` (STRING), `email` (STRING), `updated_at` (TIMESTAMP(3)).

The team needs:

- **Order-Shipment matching**: Join orders with their shipments using the 48-hour time constraint
- **Currency conversion**: Convert order amounts to USD using the exchange rate valid at order time
- **Customer enrichment**: Enrich orders with customer name and tier from the external database
- **CDC ingestion**: Create a table to consume the Debezium CDC stream from the customer database
- **Customer summary**: Aggregate order data per customer into an upsert output topic, tracking total orders, total spend, and last order time
- **Deduplication**: Remove duplicate orders (same order_id), keeping only the latest version

Write all DDL and DML statements in a single file.

## Output Specification

- `enrichment.sql` — All CREATE TABLE, CREATE VIEW, INSERT INTO, and SELECT statements
