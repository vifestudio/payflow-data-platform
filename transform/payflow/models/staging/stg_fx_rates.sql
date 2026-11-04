SELECT *
FROM read_parquet('s3://payflow-lake/bronze/fx-rates/fx_rates/**/*.parquet')
