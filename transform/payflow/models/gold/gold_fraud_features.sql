SELECT
    t.transaction_id,
    t.account_id,
    t.amount,
    t.amount_usd,
    t.currency,
    t.merchant,
    t.merchant_clean,
    t.mcc_code,
    t.mcc_category,
    t.risk_tier,
    t.transaction_date,
    t.hour_of_day,
    t.day_of_week,
    t.is_weekend,
    t.account_country,
    a.account_age_days,
    a.account_type,
    a.credit_limit,
    te.device_type,
    te.latency_ms,
    fl.is_fraud,
    fl.fraud_pattern
FROM {{ ref('silver_transactions') }} t
LEFT JOIN {{ ref('silver_fraud_labels') }} fl
    ON t.transaction_id = fl.transaction_id
LEFT JOIN {{ ref('silver_telemetry_events') }} te
    ON t.transaction_id = te.transaction_id
LEFT JOIN {{ ref('silver_accounts') }} a
    ON t.account_id = a.account_id
WHERE fl.is_fraud IS NOT NULL
