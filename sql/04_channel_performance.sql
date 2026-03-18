-- Channel Performance Comparison
-- Compares revenue, AOV, and customer acquisition by marketing channel

SELECT
    channel,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS aov,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer,
    ROUND(SUM(CASE WHEN returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM transactions
GROUP BY channel
ORDER BY total_revenue DESC;
