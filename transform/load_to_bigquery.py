"""
Load PayFlow Gold tables from DuckDB into BigQuery.

Reads the dbt-built Gold tables from the DuckDB lakehouse and batch-loads them
into BigQuery. This is a plain dump: merchant normalization already happened in
the Silver layer (via pgvector + stg_merchant_map), so Gold already carries clean
merchants and no transformation is needed here. Batch loads only, so it is
compatible with BigQuery Sandbox (no streaming inserts).

Pipeline position: normalize_merchants.py (pgvector -> R2) -> dbt (Silver bakes
merchant_clean, Gold aggregates) -> THIS -> BigQuery -> Looker.

Uses Application Default Credentials (ADC) — run once:
  gcloud auth application-default login
  gcloud auth application-default set-quota-project payflow-analytics-513907

Env (from project .env):
  GCP_PROJECT_ID   (default: payflow-analytics-513907)
  BQ_DATASET       (default: payflow_analytics)
"""
import logging
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "payflow-analytics-513907")
DATASET     = os.getenv("BQ_DATASET",     "payflow_analytics")
DUCKDB_PATH = ROOT / "transform" / "payflow" / "dev.duckdb"

# (duckdb source table, bigquery destination table)
TABLES = [
    ("gold_fraud_features",   "gold_fraud_features"),
    ("gold_daily_summary",    "gold_daily_summary"),
    ("gold_merchant_stats",   "gold_merchant_stats"),
    ("silver_accounts",       "dim_accounts"),
    ("mart_fraud_by_merchant", "mart_fraud_by_merchant"),
    ("mart_fraud_daily",       "mart_fraud_daily"),
]


def load() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    for src, dest_table in TABLES:
        df = con.execute(f"SELECT * FROM main.{src}").df()
        dest = f"{PROJECT_ID}.{DATASET}.{dest_table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        log.info(f"Loading {len(df):,} rows -> {DATASET}.{dest_table}")
        client.load_table_from_dataframe(df, dest, job_config=job_config).result()
        log.info(f"  Done. {client.get_table(dest).num_rows:,} rows in BigQuery.")

    con.close()
    log.info("All Gold tables loaded to BigQuery.")


if __name__ == "__main__":
    load()
