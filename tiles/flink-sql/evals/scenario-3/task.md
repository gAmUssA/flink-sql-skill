# Real-Time Fraud Detection with Stateful Processing

## Problem Description

A payment processing company needs to build a real-time fraud scoring system that goes beyond what standard Flink SQL can express. They need custom stateful operators that can:

1. **Transaction Velocity Tracker** — A stateful function that tracks per-user transaction patterns. For each incoming transaction, it should:
   - Maintain a count of transactions in the last hour using state
   - Track the running average transaction amount per user
   - Emit a fraud score (0.0-1.0) based on: velocity (>10 txns/hour = 0.5), amount spike (>3x average = 0.3), new merchant (first time = 0.2)
   - Use a 24-hour state expiration to clean up inactive users
   - The output should include: `user_id`, `transaction_id`, `fraud_score`, `risk_factors` (comma-separated), `transaction_amount`, `avg_amount`

2. **Session Revenue Calculator** — A stateful function that groups checkout events into sessions and emits session summaries when a session expires. Requirements:
   - Session timeout of 15 minutes (passed as a parameter)
   - Track session start time, event count, and total revenue
   - Use timers to detect session expiration and emit the session summary
   - Output: `user_id`, `session_start` (TIMESTAMP), `session_end` (TIMESTAMP), `event_count` (INT), `total_revenue` (DOUBLE)

3. Write the SQL queries that invoke both functions against input tables, with proper partitioning and time ordering.

The transaction data comes from a table `transactions` with fields: `user_id` (STRING), `transaction_id` (STRING), `amount` (DOUBLE), `merchant_id` (STRING), `event_time` (TIMESTAMP(3)) with a 1-second watermark.

The checkout data comes from a table `checkouts` with fields: `user_id` (STRING), `item_id` (STRING), `price` (DOUBLE), `checkout_time` (TIMESTAMP(3)) with a 2-second watermark.

## Output Specification

- `TransactionVelocityTracker.java` — The fraud detection PTF implementation
- `SessionRevenueCalculator.java` — The session revenue PTF implementation
- `queries.sql` — SQL statements to invoke both PTFs with correct syntax
