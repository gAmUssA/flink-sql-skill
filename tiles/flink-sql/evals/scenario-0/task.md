# Real-Time Sensor Monitoring Dashboard

## Problem Description

An IoT manufacturing company has sensors deployed across 3 factory floors. Each sensor emits temperature, humidity, and vibration readings every second. The data engineering team needs to build a streaming analytics pipeline that:

1. Computes **hourly averages** per sensor for a real-time dashboard (fixed, non-overlapping time windows)
2. Computes **rolling 1-hour averages updated every 5 minutes** for trend detection (overlapping windows)
3. Detects **user sessions** on their monitoring web app, grouping activity with 30-minute inactivity gaps
4. Provides **daily cumulative** energy consumption totals updated hourly

The sensor data arrives in a Kafka topic called `sensor_readings` with fields: `sensor_id` (STRING), `floor_id` (INT), `temperature` (DOUBLE), `humidity` (DOUBLE), `vibration` (DOUBLE), `reading_time` (TIMESTAMP(3)). Readings can arrive up to 10 seconds out of order.

The web app activity arrives in a Kafka topic called `app_activity` with fields: `user_id` (STRING), `action_type` (STRING), `page` (STRING), `activity_time` (TIMESTAMP(3)). Activity events can arrive up to 5 seconds late.

The energy data arrives in a Kafka topic called `energy_usage` with fields: `meter_id` (STRING), `kwh` (DOUBLE), `measurement_time` (TIMESTAMP(3)).

Write all the Flink SQL DDL and queries needed for this pipeline. Put everything in a single file called `pipeline.sql`.

## Output Specification

- `pipeline.sql` — All CREATE TABLE statements with proper configurations and all SELECT/INSERT queries
