-- RFM Customer Segmentation
-- Classifies customers by Recency, Frequency, and Monetary value

WITH rfm_raw AS (
    SELECT
        customer_id,
        CAST(julianday('2026-01-01') - julianday(MAX(transaction_date)) AS INT) AS recency_days,
        COUNT(*) AS frequency,
        ROUND(SUM(total_amount), 2) AS monetary
    FROM transactions
    WHERE returned = 0
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(4) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_raw
)
SELECT
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score + f_score + m_score AS rfm_total,
    CASE
        WHEN r_score + f_score + m_score >= 10 THEN 'Champions'
        WHEN r_score + f_score + m_score >= 8 THEN 'Loyal'
        WHEN r_score + f_score + m_score >= 6 THEN 'Potential'
        WHEN r_score + f_score + m_score >= 4 THEN 'At Risk'
        ELSE 'Lost'
    END AS segment
FROM rfm_scored
ORDER BY rfm_total DESC;
