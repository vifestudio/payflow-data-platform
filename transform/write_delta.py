"""
Reads Silver and Gold tables from the local DuckDB file and writes them to
Cloudflare R2 as Delta Lake tables.

Silver tables: MERGE by primary key (upsert, only new/changed rows travel to R2).
Gold tables: overwrite (rebuilt from scratch by dbt each run; small aggregates).
fx_rates: overwrite (6-row lookup, always replace).

Run after: dbt run --select silver gold
"""
import os
import duckdb
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

DBT_DUCKDB = os.path.join(os.path.dirname(__file__), "payflow", "dev.duckdb")

STORAGE = {
    "endpoint_url": os.environ["R2_ENDPOINT"],
    "aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
    "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
    "aws_region": "us-east-1",
}

# (dbt_table_name, r2_path_suffix, primary_key or None for overwrite)
SILVER_TABLES = [
    ("silver_accounts",         "silver/accounts",         "account_id"),
    ("silver_transactions",     "silver/transactions",      "transaction_id"),
    ("silver_fraud_labels",     "silver/fraud_labels",      "transaction_id"),
    ("silver_telemetry_events", "silver/telemetry_events",  "event_id"),
    ("silver_fx_rates",         "silver/fx_rates",          None),
]

GOLD_TABLES = [
    ("gold_fraud_features", "gold/fraud_features", "transaction_id"),
    ("gold_daily_summary",  "gold/daily_summary",  "transaction_date"),
    ("gold_merchant_stats", "gold/merchant_stats",  "merchant"),
]


def upsert(dest: str, arrow, pk: str) -> str:
    """MERGE into existing Delta table; create it on first run."""
    if DeltaTable.is_deltatable(dest, storage_options=STORAGE):
        dt = DeltaTable(dest, storage_options=STORAGE)
        (
            dt.merge(arrow, f"s.{pk} = t.{pk}", source_alias="s", target_alias="t")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        return "merged"
    else:
        write_deltalake(dest, arrow, mode="overwrite", schema_mode="overwrite", storage_options=STORAGE)
        return "created"


def overwrite(dest: str, arrow) -> str:
    write_deltalake(dest, arrow, mode="overwrite", schema_mode="overwrite", storage_options=STORAGE)
    return "overwritten"


con = duckdb.connect(DBT_DUCKDB, read_only=True)

print("--- Silver ---")
for table, path, pk in SILVER_TABLES:
    dest = f"s3://payflow-lake/{path}"
    print(f"  {table} -> {dest} ...", end=" ", flush=True)
    arrow = con.execute(f"SELECT * FROM main.{table}").to_arrow_table()
    action = upsert(dest, arrow, pk) if pk else overwrite(dest, arrow)
    print(f"{action} ({len(arrow):,} rows)")

print("\n--- Gold ---")
for table, path, pk in GOLD_TABLES:
    dest = f"s3://payflow-lake/{path}"
    print(f"  {table} -> {dest} ...", end=" ", flush=True)
    arrow = con.execute(f"SELECT * FROM main.{table}").to_arrow_table()
    action = upsert(dest, arrow, pk) if pk else overwrite(dest, arrow)
    print(f"{action} ({len(arrow):,} rows)")

con.close()
print("\nDone.")
