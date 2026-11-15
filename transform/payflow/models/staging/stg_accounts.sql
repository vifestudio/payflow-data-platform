-- Bronze is append-only: Airbyte can land the same account more than once
-- (re-syncs, resets). Keep the latest row per account_id so every downstream
-- LEFT JOIN on account_id stays one-to-one instead of multiplying transactions.
SELECT *
FROM read_parquet('s3://payflow-lake/bronze/postgres/accounts/**/*.parquet')
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id
    ORDER BY created_at DESC, _airbyte_extracted_at DESC NULLS LAST
) = 1
