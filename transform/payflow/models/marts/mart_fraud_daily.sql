-- Business question: how does fraud trend day over day?
-- One row per calendar day. Feeds the main time-series chart in Looker Studio.
SELECT
    transaction_date,
    COUNT(*)                                                            AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)                           AS fraud_count,
    COUNT(*) - SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)               AS approved_count,
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)             AS fraud_rate,
    ROUND(SUM(amount_usd), 2)                                           AS total_amount_usd,
    ROUND(SUM(CASE WHEN is_fraud THEN amount_usd ELSE 0 END), 2)        AS fraud_amount_usd,
    ROUND(AVG(amount_usd), 2)                                           AS avg_amount_usd
FROM {{ ref('gold_fraud_features') }}
GROUP BY transaction_date
ORDER BY transaction_date
