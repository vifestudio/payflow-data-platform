SELECT
    raw_merchant,
    canonical_merchant,
    similarity,
    is_new,
    mcc_code
FROM read_parquet('s3://payflow-lake/reference/merchant_map/*.parquet')
