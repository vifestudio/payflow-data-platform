{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert'
) }}

WITH source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id ORDER BY created_at DESC
        ) AS rn
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_id IS NOT NULL
),
deduped AS (
    SELECT * FROM source WHERE rn = 1
)
SELECT
    t.transaction_id,
    t.account_id,
    t.amount,
    UPPER(TRIM(t.currency))                                             AS currency,
    CASE
        WHEN UPPER(TRIM(t.currency)) = 'USD' THEN t.amount
        WHEN fx.rate IS NOT NULL AND fx.rate > 0 THEN ROUND(t.amount / fx.rate, 6)
        ELSE NULL
    END                                                                 AS amount_usd,
    TRIM(t.merchant)                                                    AS merchant,
    LOWER(TRIM(t.status))                                               AS status,
    a.country                                                           AS account_country,
    t.created_at::DATE                                                  AS transaction_date,
    EXTRACT(HOUR FROM t.created_at)::INTEGER                            AS hour_of_day,
    EXTRACT(DOW FROM t.created_at)::INTEGER                             AS day_of_week,
    EXTRACT(DOW FROM t.created_at) IN (0, 6)                           AS is_weekend,
    t.created_at
FROM deduped t
LEFT JOIN (
    SELECT currency, rate
    FROM {{ ref('stg_fx_rates') }}
    WHERE (currency, date) IN (
        SELECT currency, MAX(date)
        FROM {{ ref('stg_fx_rates') }}
        GROUP BY currency
    )
) fx ON UPPER(TRIM(t.currency)) = UPPER(TRIM(fx.currency))
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id
{% if is_incremental() %}
WHERE t.created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
