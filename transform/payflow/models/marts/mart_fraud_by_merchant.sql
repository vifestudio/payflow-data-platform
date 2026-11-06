-- Business question: which merchants concentrate the most fraud?
-- One row per canonical merchant, ranked by fraud rate. Reads the labeled Gold
-- feature table (merchants already normalized in Silver).
SELECT
    merchant_clean                                                  AS merchant,
    COUNT(*)                                                        AS total_transactions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)                       AS fraud_transactions,
    ROUND(AVG(CASE WHEN is_fraud THEN 1.0 ELSE 0.0 END), 4)         AS fraud_rate,
    ROUND(SUM(CASE WHEN is_fraud THEN amount_usd ELSE 0 END), 2)    AS fraud_amount_usd,
    ROUND(SUM(amount_usd), 2)                                       AS total_amount_usd
FROM {{ ref('gold_fraud_features') }}
GROUP BY merchant_clean
ORDER BY fraud_rate DESC
