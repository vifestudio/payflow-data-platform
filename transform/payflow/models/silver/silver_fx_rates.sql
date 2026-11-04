SELECT
    date::DATE  AS rate_date,
    base,
    currency,
    rate
FROM {{ ref('stg_fx_rates') }}
