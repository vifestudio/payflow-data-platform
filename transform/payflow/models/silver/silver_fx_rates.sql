WITH source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY currency, date ORDER BY rate DESC
        ) AS rn
    FROM {{ ref('stg_fx_rates') }}
    WHERE currency IS NOT NULL
      AND date IS NOT NULL
      AND rate > 0
),
deduped AS (
    SELECT * FROM source WHERE rn = 1
)
SELECT
    UPPER(TRIM(currency))   AS currency,
    UPPER(TRIM(base))       AS base,
    date::DATE              AS rate_date,
    rate
FROM deduped
