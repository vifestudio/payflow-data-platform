-- Bronze is append-only: Airbyte can land the same transaction more than once
-- (re-syncs, resets). Keep the latest row per transaction_id so every consumer
-- (silver_transactions, the telemetry join) sees one row per transaction.
SELECT *
FROM read_parquet('s3://payflow-lake/bronze/postgres/transactions/**/*.parquet')
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id
    ORDER BY created_at DESC, _airbyte_extracted_at DESC NULLS LAST
) = 1
