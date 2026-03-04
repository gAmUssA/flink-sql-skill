# Confluent Cloud Flink Pipeline with Table API

## Problem Description

A logistics company is migrating their batch analytics to real-time streaming on Confluent Cloud. They have three Kafka topics already in their Confluent Cloud environment:

- `shipment_events` — tracking events for packages (scanned, loaded, in_transit, delivered)
- `route_updates` — GPS coordinates and ETA updates from delivery vehicles
- `customer_notifications` — output topic for delivery notifications

The team needs a complete pipeline that:

1. **Java Table API application** that connects to Confluent Cloud, reads from `shipment_events`, filters for delivered packages, and writes delivery confirmations to `customer_notifications`. Use the Confluent-specific Table API plugin.

2. **Multi-output routing** using SQL — split `shipment_events` into three output topics based on event type: `delayed_shipments` (events where status = 'delayed'), `completed_deliveries` (status = 'delivered'), and `shipment_metrics` (hourly counts per status using 1-hour tumbling windows).

3. **A pattern detection query** that identifies suspicious delivery patterns using pattern matching: packages that are scanned more than 3 times at different locations within 2 hours, followed by a status change to 'lost'.

4. **Operational scripts** — CLI commands for:
   - Creating and managing a compute pool
   - Deploying the streaming statements
   - Creating a savepoint and performing a rolling upgrade of one of the statements
   - Viewing statement exceptions for debugging

The `shipment_events` topic has fields: `tracking_id` (STRING), `status` (STRING), `location` (STRING), `event_time` (TIMESTAMP_LTZ(3)), `carrier_id` (STRING).

The `route_updates` topic has fields: `vehicle_id` (STRING), `tracking_id` (STRING), `latitude` (DOUBLE), `longitude` (DOUBLE), `eta` (TIMESTAMP_LTZ(3)), `update_time` (TIMESTAMP_LTZ(3)).

## Output Specification

- `FlinkApp.java` — Java Table API application with Confluent Cloud setup
- `pipeline.sql` — All SQL statements including multi-output routing, pattern detection, and table DDL
- `operations.sh` — CLI commands for compute pool management, deployment, savepoints, and debugging
