SELECT
    t.merchant,
    COUNT(*)                                                        AS total_transactions,
    ROUND(SUM(t.amount_usd), 2)                                    AS total_amount_usd,
    ROUND(AVG(t.amount_usd), 2)                                    AS avg_amount_usd,
    ROUND(MIN(t.amount_usd), 2)                                    AS min_amount_usd,
    ROUND(MAX(t.amount_usd), 2)                                    AS max_amount_usd,
    COUNT(DISTINCT t.account_id)                                    AS distinct_accounts,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)                   AS fraud_count,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END), 4)    AS fraud_rate
FROM {{ ref('silver_transactions') }} t
LEFT JOIN {{ ref('silver_fraud_labels') }} fl
    ON t.transaction_id = fl.transaction_id
GROUP BY t.merchant
ORDER BY total_transactions DESC
