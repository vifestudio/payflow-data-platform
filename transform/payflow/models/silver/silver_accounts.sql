{{ config(materialized='table') }}

WITH source AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id ORDER BY created_at DESC
        ) AS rn
    FROM {{ ref('stg_accounts') }}
    WHERE account_id IS NOT NULL
),
deduped AS (
    SELECT * FROM source WHERE rn = 1
)
SELECT
    account_id,
    md5(LOWER(TRIM(email)))                                                     AS email_hashed,
    md5(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(TRIM(holder_name)),
        'Á','A'), 'É','E'), 'Í','I'), 'Ó','O'), 'Ú','U'), 'Ñ','N'), 'Ü','U')) AS holder_name_hashed,
    UPPER(TRIM(country))                                                         AS country,
    LOWER(TRIM(acct_type))                                                       AS account_type,
    credit_limit,
    created_at::DATE                                                             AS created_date,
    created_at,
    DATE_DIFF('day', created_at::DATE, CURRENT_DATE)                            AS account_age_days
FROM (
    SELECT
        account_id, email, holder_name, country,
        account_type AS acct_type,
        credit_limit, created_at
    FROM deduped
)
