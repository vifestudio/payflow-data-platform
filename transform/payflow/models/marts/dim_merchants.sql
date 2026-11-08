{{ config(materialized='table') }}

SELECT DISTINCT
    merchant_clean,
    mcc_code,
    mcc_category,
    risk_tier
FROM {{ ref('silver_transactions') }}
WHERE merchant_clean IS NOT NULL
