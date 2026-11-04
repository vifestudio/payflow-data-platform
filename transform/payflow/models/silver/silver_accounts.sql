SELECT
    account_id,
    md5(email)         AS email_hashed,
    md5(holder_name)   AS holder_name_hashed,
    country,
    created_at::DATE   AS created_date,
    created_at
FROM {{ ref('stg_accounts') }}
