SELECT *
FROM read_parquet('s3://payflow-lake/bronze/postgres/accounts/**/*.parquet')
