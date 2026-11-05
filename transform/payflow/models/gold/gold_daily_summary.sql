SELECT
    t.transaction_date,
    COUNT(*)                                                        AS total_transactions,
    ROUND(SUM(t.amount_usd), 2)                                    AS total_amount_usd,
    ROUND(AVG(t.amount_usd), 2)                                    AS avg_amount_usd,
    COUNT(DISTINCT t.account_id)                                    AS distinct_accounts,
    COUNT(DISTINCT t.merchant)                                      AS distinct_merchants,
    SUM(CASE WHEN fl.is_fraud THEN 1 ELSE 0 END)                   AS fraud_count,
    ROUND(AVG(CASE WHEN fl.is_fraud THEN 1.0 ELSE 0.0 END), 4)    AS fraud_rate
FROM {{ ref('silver_transactions') }} t
LEFT JOIN {{ ref('silver_fraud_labels') }} fl
    ON t.transaction_id = fl.transaction_id
GROUP BY t.transaction_date
ORDER BY t.transaction_date
