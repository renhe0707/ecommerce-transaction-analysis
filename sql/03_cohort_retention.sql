-- Cohort Retention Analysis
-- Tracks how many customers return each month after first purchase

WITH first_purchase AS (
    SELECT
        customer_id,
        strftime('%Y-%m', MIN(transaction_date)) AS cohort_month
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
),
monthly_activity AS (
    SELECT
        t.customer_id,
        fp.cohort_month,
        strftime('%Y-%m', t.transaction_date) AS activity_month,
        (CAST(strftime('%Y', t.transaction_date) AS INT) -
         CAST(SUBSTR(fp.cohort_month, 1, 4) AS INT)) * 12 +
        (CAST(strftime('%m', t.transaction_date) AS INT) -
         CAST(SUBSTR(fp.cohort_month, 6, 2) AS INT)) AS month_offset
    FROM transactions t
    JOIN first_purchase fp ON t.customer_id = fp.customer_id
    WHERE t.returned = 0
)
SELECT
    cohort_month,
    month_offset,
    COUNT(DISTINCT customer_id) AS active_customers
FROM monthly_activity
WHERE month_offset BETWEEN 0 AND 11
GROUP BY cohort_month, month_offset
ORDER BY cohort_month, month_offset;
