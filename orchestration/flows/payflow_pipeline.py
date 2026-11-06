"""
PayFlow pipeline orchestrated with Prefect 3.

Execution order:
  1. Trigger Airbyte syncs (Postgres, MongoDB, FX API) in parallel
  2. Wait for all three to complete
  3. Run dbt (incremental Silver + rebuild Gold)
  4. Write Silver/Gold to R2 Delta Lake (MERGE for keyed tables)

Schedule: hourly via flow.serve() at the bottom, or deploy to Prefect Cloud.

Env vars required (same .env as the rest of the project):
  AIRBYTE_URL                     e.g. http://localhost:8000
  AIRBYTE_USERNAME                Airbyte basic-auth user (default: airbyte)
  AIRBYTE_PASSWORD                Airbyte basic-auth password
  AIRBYTE_POSTGRES_CONNECTION_ID  UUID from Airbyte UI
  AIRBYTE_MONGO_CONNECTION_ID     UUID from Airbyte UI
  AIRBYTE_FX_CONNECTION_ID        UUID from Airbyte UI
"""
import os
import subprocess
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

AIRBYTE_URL = os.environ.get("AIRBYTE_URL", "http://localhost:8000")
AIRBYTE_AUTH = (
    os.environ.get("AIRBYTE_USERNAME", "airbyte"),
    os.environ.get("AIRBYTE_PASSWORD", "password"),
)
DBT_DIR = ROOT / "transform" / "payflow"
TRANSFORM_DIR = ROOT / "transform"


# ---------------------------------------------------------------------------
# Airbyte tasks
# ---------------------------------------------------------------------------

@task(name="trigger-airbyte-sync", retries=2, retry_delay_seconds=30)
def trigger_airbyte_sync(connection_id: str, label: str, bearer_token: str | None = None) -> int:
    """
    Trigger an Airbyte connection sync and return the job ID.

    Airbyte 1.x uses OAuth2 Bearer tokens. Pass AIRBYTE_BEARER_TOKEN in .env,
    or leave unset to skip the trigger (Airbyte will run on its own schedule).
    """
    logger = get_run_logger()
    headers = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    resp = requests.post(
        f"{AIRBYTE_URL}/api/v1/connections/sync",
        json={"connectionId": connection_id},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    job_id = resp.json()["job"]["id"]
    logger.info(f"[{label}] triggered — job {job_id}")
    return job_id


@task(name="wait-for-airbyte-job", retries=3, retry_delay_seconds=60)
def wait_for_airbyte_job(job_id: int, label: str, bearer_token: str | None = None, poll_seconds: int = 10) -> None:
    """Poll an Airbyte job until it succeeds or raises on failure."""
    logger = get_run_logger()
    headers = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    while True:
        resp = requests.get(
            f"{AIRBYTE_URL}/api/v1/jobs/get",
            json={"id": job_id},
            headers=headers,
            timeout=15,
        )
        resp.raise_for_status()
        status = resp.json()["job"]["status"]
        logger.info(f"[{label}] job {job_id} status: {status}")
        if status == "succeeded":
            return
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte job {job_id} ({label}) ended with status: {status}")
        time.sleep(poll_seconds)


# ---------------------------------------------------------------------------
# dbt task
# ---------------------------------------------------------------------------

@task(name="run-dbt", retries=1, retry_delay_seconds=10)
def run_dbt(full_refresh: bool = False) -> None:
    """Run dbt models (incremental Silver + full rebuild Gold)."""
    logger = get_run_logger()
    env = {**os.environ}

    cmd = ["dbt", "run", "--no-partial-parse"]
    if full_refresh:
        cmd.append("--full-refresh")

    logger.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    logger.info(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("dbt run failed — check logs above")


# ---------------------------------------------------------------------------
# Delta Lake write task
# ---------------------------------------------------------------------------

@task(name="write-delta-lake", retries=1, retry_delay_seconds=15)
def write_delta_lake() -> None:
    """MERGE Silver tables and overwrite Gold aggregates in R2."""
    logger = get_run_logger()
    env = {**os.environ, "PYTHONUTF8": "1"}
    result = subprocess.run(
        ["python", "write_delta.py"],
        cwd=TRANSFORM_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("write_delta.py failed — check logs above")


# ---------------------------------------------------------------------------
# BigQuery load task
# ---------------------------------------------------------------------------

@task(name="normalize-merchants", retries=1, retry_delay_seconds=15)
def normalize_merchants() -> None:
    """Resolve raw merchant names to canonical names via pgvector + embeddings."""
    logger = get_run_logger()
    env = {**os.environ, "PYTHONUTF8": "1", "POSTGRES_HOST": "localhost"}
    result = subprocess.run(
        ["python", "normalize_merchants.py"],
        cwd=TRANSFORM_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("normalize_merchants.py failed — check logs above")


@task(name="load-bigquery", retries=1, retry_delay_seconds=30)
def load_bigquery() -> None:
    """Batch-load Gold tables from DuckDB into BigQuery with clean merchants (uses ADC)."""
    logger = get_run_logger()
    env = {**os.environ, "PYTHONUTF8": "1", "POSTGRES_HOST": "localhost"}
    result = subprocess.run(
        ["python", "load_to_bigquery.py"],
        cwd=TRANSFORM_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError("load_to_bigquery.py failed — check logs above")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="payflow-pipeline", log_prints=True)
def payflow_pipeline(full_refresh: bool = False) -> None:
    """
    End-to-end PayFlow data pipeline.

    Args:
        full_refresh: Pass True on first run or after schema changes.
                      Forces dbt --full-refresh and rebuilds all Silver tables.
    """
    logger = get_run_logger()
    logger.info(f"Starting PayFlow pipeline (full_refresh={full_refresh})")

    # 1. Trigger Airbyte syncs if a bearer token is available.
    #    Airbyte 1.x requires OAuth2; set AIRBYTE_BEARER_TOKEN in .env to enable.
    #    Without it, Airbyte runs on its own schedule and this step is skipped.
    bearer = os.environ.get("AIRBYTE_BEARER_TOKEN")
    if bearer:
        pg_conn_id = os.environ["AIRBYTE_POSTGRES_CONNECTION_ID"]
        mongo_conn_id = os.environ["AIRBYTE_MONGO_CONNECTION_ID"]
        fx_conn_id = os.environ["AIRBYTE_FX_CONNECTION_ID"]

        pg_job = trigger_airbyte_sync(pg_conn_id, label="postgres", bearer_token=bearer)
        mongo_job = trigger_airbyte_sync(mongo_conn_id, label="mongodb", bearer_token=bearer)
        fx_job = trigger_airbyte_sync(fx_conn_id, label="fx-api", bearer_token=bearer)

        wait_for_airbyte_job(pg_job, label="postgres", bearer_token=bearer)
        wait_for_airbyte_job(mongo_job, label="mongodb", bearer_token=bearer)
        wait_for_airbyte_job(fx_job, label="fx-api", bearer_token=bearer)
    else:
        logger.info("AIRBYTE_BEARER_TOKEN not set — skipping Airbyte trigger (syncs run on Airbyte schedule).")

    # 2. Transform: incremental Silver + full rebuild Gold
    run_dbt(full_refresh=full_refresh)

    # 3. Persist to R2
    write_delta_lake()

    # 4. Normalize merchant names (pgvector) before loading the warehouse
    normalize_merchants()

    # 5. Load Gold to BigQuery with clean merchants
    load_bigquery()

    logger.info("PayFlow pipeline complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Run once immediately, then every hour.
    # To run a single time: payflow_pipeline()
    payflow_pipeline.serve(
        name="payflow-hourly",
        cron="0 * * * *",
        parameters={"full_refresh": False},
    )
