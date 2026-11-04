import duckdb
import os
from dotenv import load_dotenv

load_dotenv("C:/Users/vife.boy/Desktop/payflow-data-platform/.env")

endpoint = os.environ["R2_ENDPOINT"].replace("https://", "")
key_id = os.environ["R2_ACCESS_KEY_ID"]
secret = os.environ["R2_SECRET_ACCESS_KEY"]

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    CREATE OR REPLACE PERSISTENT SECRET r2_payflow (
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{secret}',
        ENDPOINT '{endpoint}',
        URL_STYLE 'path',
        REGION 'us-east-1',
        USE_SSL true
    );
""")

tables = {
    "accounts":        "s3://payflow-lake/bronze/postgres/accounts/**/*.parquet",
    "transactions":    "s3://payflow-lake/bronze/postgres/transactions/**/*.parquet",
    "fraud_labels":    "s3://payflow-lake/bronze/postgres/fraud_labels/**/*.parquet",
    "telemetry":       "s3://payflow-lake/bronze/mongo/telemetry_events/**/*.parquet",
    "fx_rates":        "s3://payflow-lake/bronze/fx-rates/fx_rates/**/*.parquet",
}

for name, path in tables.items():
    cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
    print(f"\n--- {name} ---")
    for c in cols:
        print(f"  {c[0]:40s} {c[1]}")
