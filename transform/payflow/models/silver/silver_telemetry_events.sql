{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert'
) }}

WITH source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id ORDER BY timestamp DESC
        ) AS rn
    FROM {{ ref('stg_telemetry_events') }}
    WHERE _id IS NOT NULL
      AND transaction_id IS NOT NULL
),
deduped AS (
    SELECT * FROM source WHERE rn = 1
)
SELECT
    te._id                              AS event_id,
    te.transaction_id,
    te.device_type,
    te.user_agent,
    te.latency_ms,
    te.timestamp::TIMESTAMP             AS event_at,
    a.country                           AS account_country
FROM deduped te
LEFT JOIN {{ ref('stg_transactions') }} t
    ON te.transaction_id = t.transaction_id
LEFT JOIN {{ ref('stg_accounts') }} a
    ON t.account_id = a.account_id
WHERE te._ab_cdc_deleted_at IS NULL
{% if is_incremental() %}
AND te.timestamp::TIMESTAMP > (SELECT MAX(event_at) FROM {{ this }})
{% endif %}
