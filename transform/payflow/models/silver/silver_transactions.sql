SELECT
    t.transaction_id,
    t.account_id,
    t.amount,
    t.currency,
    CASE
        WHEN t.currency = 'USD' THEN t.amount
        WHEN fx.rate IS NOT NULL AND fx.rate > 0 THEN ROUND(t.amount / fx.rate, 6)
        ELSE NULL
    END                             AS amount_usd,
    t.merchant,
    t.status,
    t.created_at::DATE              AS transaction_date,
    t.created_at
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('stg_fx_rates') }} fx
    ON t.currency = fx.currency
    AND t.created_at::DATE = fx.date::DATE
