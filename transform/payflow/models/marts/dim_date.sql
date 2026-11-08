{{ config(materialized='table') }}

SELECT DISTINCT
    transaction_date,
    EXTRACT(year  FROM transaction_date)::INT   AS year,
    EXTRACT(month FROM transaction_date)::INT   AS month,
    EXTRACT(day   FROM transaction_date)::INT   AS day,
    EXTRACT(dow   FROM transaction_date)::INT   AS day_of_week,
    CASE WHEN EXTRACT(dow FROM transaction_date) IN (0, 6)
         THEN true ELSE false END               AS is_weekend,
    EXTRACT(quarter FROM transaction_date)::INT AS quarter
FROM {{ ref('silver_transactions') }}
WHERE transaction_date IS NOT NULL
