SELECT
    transaction_id,
    is_fraud,
    fraud_pattern,
    labeled_at::DATE    AS label_date,
    labeled_at
FROM {{ ref('stg_fraud_labels') }}
