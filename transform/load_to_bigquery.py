"""
Load PayFlow Gold tables from DuckDB into BigQuery.

Reads the dbt-built Gold tables from the DuckDB lakehouse, applies the pgvector
merchant normalization (raw -> canonical) so BigQuery holds clean merchant
names, and batch-loads everything into BigQuery. Batch loads only — compatible
with BigQuery Sandbox (no streaming inserts).

Pipeline position: dbt (Gold) -> normalize_merchants.py (pgvector) -> THIS -> BigQuery -> Looker.

Uses Application Default Credentials (ADC) — run once:
  gcloud auth application-default login
  gcloud auth application-default set-quota-project payflow-analytics-513907

Env (from project .env):
  POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
  GCP_PROJECT_ID   (default: payflow-analytics-513907)
  BQ_DATASET       (default: payflow_analytics)
"""
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd
import psycopg2
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
    ("gold_fraud_features", "gold_fraud_features"),
    ("gold_daily_summary",  "gold_daily_summary"),
    ("gold_merchant_stats", "gold_merchant_stats"),
    ("silver_accounts",     "dim_accounts"),
]

PG_DSN = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def load_merchant_map() -> dict[str, str]:
    """raw_merchant -> canonical_merchant, produced by normalize_merchants.py."""
    conn = psycopg2.connect(**PG_DSN)
    try:
        df = pd.read_sql(
            "SELECT raw_merchant, canonical_merchant FROM merchant_normalization", conn
        )
    finally:
        conn.close()
    return dict(zip(df["raw_merchant"], df["canonical_merchant"]))


def load() -> None:
    mapping = load_merchant_map()
    log.info(f"Loaded {len(mapping)} merchant normalizations from pgvector output")

    client = bigquery.Client(project=PROJECT_ID)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    for src, dest_table in TABLES:
        df = con.execute(f"SELECT * FROM main.{src}").df()

        if "merchant" in df.columns:
            df["merchant_clean"] = df["merchant"].map(mapping).fillna(df["merchant"])

        dest = f"{PROJECT_ID}.{DATASET}.{dest_table}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )
        log.info(f"Loading {len(df):,} rows -> {DATASET}.{dest_table}")
        client.load_table_from_dataframe(df, dest, job_config=job_config).result()
        log.info(f"  Done. {client.get_table(dest).num_rows:,} rows in BigQuery.")

    con.close()
    log.info("All Gold tables loaded to BigQuery with clean merchants.")


if __name__ == "__main__":
    load()
