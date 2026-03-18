-- Core KPI Summary
-- Calculates AOV, repurchase rate, and revenue metrics

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_spend,
        AVG(total_amount) AS avg_order_value,
        MIN(transaction_date) AS first_purchase,
        MAX(transaction_date) AS last_purchase
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
)
SELECT
    COUNT(*) AS total_customers,
    SUM(order_count) AS total_orders,
    ROUND(SUM(total_spend), 2) AS total_revenue,
    ROUND(AVG(avg_order_value), 2) AS overall_aov,
    ROUND(SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS repurchase_rate_pct,
    ROUND(AVG(total_spend), 2) AS avg_clv
FROM customer_orders;
