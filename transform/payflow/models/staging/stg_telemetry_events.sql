SELECT *
FROM read_parquet('s3://payflow-lake/bronze/mongo/telemetry_events/**/*.parquet')
