-- Monthly Revenue, Orders, AOV Trends
-- Tracks business performance over time

SELECT
    strftime('%Y-%m', transaction_date) AS month,
    COUNT(*) AS orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(SUM(total_amount), 2) AS revenue,
    ROUND(AVG(total_amount), 2) AS aov,
    ROUND(SUM(CASE WHEN returned = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS return_rate_pct
FROM transactions
GROUP BY month
ORDER BY month;
