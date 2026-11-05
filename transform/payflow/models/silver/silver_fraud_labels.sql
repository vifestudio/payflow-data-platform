{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert'
) }}

WITH source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id ORDER BY labeled_at DESC
        ) AS rn
    FROM {{ ref('stg_fraud_labels') }}
    WHERE transaction_id IS NOT NULL
),
deduped AS (
    SELECT * FROM source WHERE rn = 1
)
SELECT
    transaction_id,
    is_fraud,
    fraud_pattern,
    labeled_at::DATE    AS label_date,
    labeled_at
FROM deduped
{% if is_incremental() %}
WHERE labeled_at > (SELECT MAX(labeled_at) FROM {{ this }})
{% endif %}
