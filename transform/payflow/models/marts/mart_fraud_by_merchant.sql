-- Business question: which merchants concentrate the most fraud?
-- One row per canonical merchant with MCC context from dim_merchants.
SELECT
    dm.merchant_clean                                                   AS merchant,
    dm.mcc_category,
    dm.risk_tier,
    COUNT(*)                                                            AS total_transactions,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)                        AS fraud_transactions,
    ROUND(AVG(CASE WHEN f.is_fraud THEN 1.0 ELSE 0.0 END), 4)          AS fraud_rate,
    ROUND(SUM(CASE WHEN f.is_fraud THEN f.amount_usd ELSE 0 END), 2)   AS fraud_amount_usd,
    ROUND(SUM(f.amount_usd), 2)                                        AS total_amount_usd
FROM {{ ref('fct_transactions') }} f
JOIN {{ ref('dim_merchants') }} dm ON f.merchant_clean = dm.merchant_clean
GROUP BY 1, 2, 3
ORDER BY fraud_rate DESC
