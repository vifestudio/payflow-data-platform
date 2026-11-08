{{ config(materialized='table') }}

-- Fact table: one row per transaction with FKs to all dimensions.
-- Measures live here; attributes belong in dims.
-- Built from gold_fraud_features (the validated, joined Gold layer).
SELECT
    transaction_id,
    account_id,
    merchant_clean,
    transaction_date,
    hour_of_day,
    currency,
    account_country,
    account_type,
    amount,
    amount_usd,
    account_age_days,
    credit_limit,
    device_type,
    latency_ms,
    is_fraud,
    fraud_pattern
FROM {{ ref('gold_fraud_features') }}
