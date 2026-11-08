-- Fails if more than 5% of gold_fraud_features rows have amount_usd IS NULL.
-- A high null rate means FX lookup is silently failing for one or more currencies,
-- which causes train.py to discard those rows without any warning.
SELECT
    SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS null_rate
FROM {{ ref('gold_fraud_features') }}
HAVING null_rate > 0.05
