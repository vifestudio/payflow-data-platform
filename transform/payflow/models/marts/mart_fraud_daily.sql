-- Business question: how does fraud trend day over day?
-- One row per calendar day. Feeds the main time-series chart in Looker Studio.
SELECT
    d.transaction_date,
    d.year,
    d.month,
    d.quarter,
    d.day_of_week,
    d.is_weekend,
    COUNT(*)                                                                AS total_transactions,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)                            AS fraud_count,
    COUNT(*) - SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)                AS approved_count,
    ROUND(AVG(CASE WHEN f.is_fraud THEN 1.0 ELSE 0.0 END), 4)              AS fraud_rate,
    ROUND(SUM(f.amount_usd), 2)                                            AS total_amount_usd,
    ROUND(SUM(CASE WHEN f.is_fraud THEN f.amount_usd ELSE 0 END), 2)       AS fraud_amount_usd,
    ROUND(AVG(f.amount_usd), 2)                                            AS avg_amount_usd
FROM {{ ref('fct_transactions') }} f
JOIN {{ ref('dim_date') }} d ON f.transaction_date = d.transaction_date
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1
