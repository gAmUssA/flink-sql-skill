package io.gamussa.enrichment;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.ValueState;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.annotation.ArgumentTrait.SET_SEMANTIC_TABLE;

/**
 * Per-customer gated enrichment.
 *
 * Takes two co-partitioned table inputs (both partitioned by customer_id):
 *   1. orders            — source events that need to be enriched
 *   2. customer updates  — enrichment stream (acts as the "gate signal")
 *
 * Behavior, per customer_id:
 *   - Orders arriving before any customer record: buffered in ListView.
 *   - First customer record arrives: saves profile, flushes the buffer,
 *     emits each buffered order with was_buffered = true.
 *   - Subsequent orders: enriched immediately from state, was_buffered = false.
 *   - Customer updates after the gate opens: replace the stored profile
 *     (future orders see the new values).
 *
 * This is the article's "gated enrichment" pattern with per-key granularity.
 */
@FunctionHint(
    output = @DataTypeHint(
        "ROW<order_id STRING, customer_id STRING, customer_name STRING, " +
        "tier STRING, country STRING, amount DECIMAL(10, 2), " +
        "order_time TIMESTAMP_LTZ(3), was_buffered BOOLEAN>"
    )
)
public class GatedEnrichmentPtf extends ProcessTableFunction<Row> {

    /** Enrichment profile stored in state once the gate opens. */
    public static class CustomerProfile {
        public String name;
        public String tier;
        public String country;
    }

    /** Order buffered while the gate is closed. */
    public static class PendingOrder {
        public String orderId;
        public String customerId;
        public BigDecimal amount;
        public Instant orderTime;
    }

    public void eval(
            @StateHint(ttl = "1 d") ValueState<CustomerProfile> profileState,
            @StateHint(ttl = "1 d") ListView<PendingOrder> pendingOrders,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row order,
            @ArgumentHint(SET_SEMANTIC_TABLE) Row customer
    ) throws Exception {

        // --------------------------------------------------------------
        // Gate signal: a customer record arrived.
        // Save profile, flush any buffered orders, future orders pass through.
        // --------------------------------------------------------------
        if (customer != null) {
            CustomerProfile profile = new CustomerProfile();
            profile.name    = customer.getFieldAs("name");
            profile.tier    = customer.getFieldAs("tier");
            profile.country = customer.getFieldAs("country");
            profileState.update(profile);

            // Snapshot the buffer so we can clear it safely.
            List<PendingOrder> toFlush = new ArrayList<>();
            for (PendingOrder p : pendingOrders.get()) {
                toFlush.add(p);
            }
            pendingOrders.clear();

            for (PendingOrder p : toFlush) {
                collect(Row.of(
                    p.orderId,
                    p.customerId,
                    profile.name,
                    profile.tier,
                    profile.country,
                    p.amount,
                    p.orderTime,
                    Boolean.TRUE
                ));
            }
            return;
        }

        // --------------------------------------------------------------
        // Source event: an order arrived.
        // Emit immediately if the gate is open; otherwise buffer.
        // --------------------------------------------------------------
        if (order != null) {
            String orderId      = order.getFieldAs("order_id");
            String customerId   = order.getFieldAs("customer_id");
            BigDecimal amount   = order.getFieldAs("amount");
            Instant orderTime   = order.getFieldAs("order_time");

            CustomerProfile profile = profileState.value();
            if (profile != null) {
                collect(Row.of(
                    orderId,
                    customerId,
                    profile.name,
                    profile.tier,
                    profile.country,
                    amount,
                    orderTime,
                    Boolean.FALSE
                ));
            } else {
                PendingOrder p = new PendingOrder();
                p.orderId    = orderId;
                p.customerId = customerId;
                p.amount     = amount;
                p.orderTime  = orderTime;
                pendingOrders.add(p);
            }
        }
    }
}
