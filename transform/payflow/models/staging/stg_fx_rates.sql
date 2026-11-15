-- One rate per (currency, date). Duplicate rows from repeated Airbyte syncs would
-- otherwise multiply transactions in the FX join of silver_transactions.
SELECT *
FROM read_parquet('s3://payflow-lake/bronze/fx-rates/fx_rates/**/*.parquet')
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY currency, date
    ORDER BY _airbyte_extracted_at DESC NULLS LAST
) = 1
