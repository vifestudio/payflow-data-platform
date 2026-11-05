WITH raw AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY _id ORDER BY _airbyte_extracted_at DESC NULLS LAST
        ) AS _rn
    FROM read_parquet('s3://payflow-lake/bronze/mongo/telemetry_events/**/*.parquet')
)
SELECT * EXCLUDE (_rn)
FROM raw
WHERE _rn = 1
