-- One canonical merchant per raw name. The normalization job writes parquet files
-- under reference/merchant_map/; if more than one file carries the same
-- raw_merchant, keep the closest match so the join in silver_transactions stays
-- one-to-one.
SELECT
    raw_merchant,
    canonical_merchant,
    similarity,
    is_new,
    mcc_code
FROM read_parquet('s3://payflow-lake/reference/merchant_map/*.parquet')
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw_merchant
    ORDER BY similarity DESC NULLS LAST
) = 1
