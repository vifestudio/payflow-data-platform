"""
Reads Silver tables from the local DuckDB file and writes them to
Cloudflare R2 as Delta Lake tables (append mode = time travel enabled).

Run after: dbt run --select silver
"""
import os
import duckdb
from deltalake import write_deltalake
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DBT_DUCKDB = os.path.join(
    os.path.dirname(__file__), "payflow", "dev.duckdb"
)

STORAGE = {
    "endpoint_url": os.environ["R2_ENDPOINT"],
    "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
    "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
    "aws_region": "us-east-1",
}

TABLES = [
    "silver_accounts",
    "silver_transactions",
    "silver_fraud_labels",
    "silver_telemetry_events",
    "silver_fx_rates",
]

con = duckdb.connect(DBT_DUCKDB, read_only=True)

for table in TABLES:
    name = table.replace("silver_", "")
    dest = f"s3://payflow-lake/silver/{name}"
    print(f"Writing {table} → {dest} ...", end=" ", flush=True)
    arrow = con.execute(f"SELECT * FROM main.{table}").to_arrow_table()
    write_deltalake(dest, arrow, mode="append", storage_options=STORAGE)
    print(f"done ({len(arrow)} rows)")

con.close()
print("\nAll Silver tables written to R2 as Delta Lake.")
