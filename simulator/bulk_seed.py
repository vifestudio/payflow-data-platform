"""
One-shot bulk seed: generates N transactions distributed across the last 90 days.
Run once to bootstrap the ML training dataset.

Usage:
    docker compose exec simulator python bulk_seed.py --rows 50000
"""
import os
import sys
import random
import logging
import argparse
from datetime import datetime, timedelta, timezone

import numpy as np
from faker import Faker
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from pymongo import MongoClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()
rng = np.random.default_rng()

POSTGRES_DSN = (
    f"host=postgres port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASSWORD')}"
)
MONGO_URI = (
    f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}"
    f"@mongo:{os.getenv('MONGO_PORT', '27017')}/?authSource=admin"
)

FRAUD_RATE   = float(os.getenv("SIM_FRAUD_RATE", "0.015"))
FRAUD_PATTERNS = ["card_testing", "geo_impossible", "amount_anomaly"]
MERCHANTS    = ["Amazon", "Uber", "Netflix", "Spotify", "Apple",
                "Walmart", "McDonald's", "Shell", "Airbnb", "Steam"]
CURRENCIES   = ["USD", "EUR", "GBP", "CRC", "MXN"]
DAYS_BACK    = 90
BATCH_SIZE   = 500


def random_timestamp():
    offset_seconds = random.randint(0, DAYS_BACK * 24 * 3600)
    return datetime.now(timezone.utc) - timedelta(seconds=offset_seconds)


def generate_amount(is_fraud, pattern):
    if is_fraud and pattern == "amount_anomaly":
        return round(float(rng.lognormal(mean=8.0, sigma=0.5)), 2)
    return round(float(rng.lognormal(mean=4.5, sigma=1.2)), 2)


def seed(total_rows):
    pg_conn = psycopg2.connect(POSTGRES_DSN)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[os.getenv("POSTGRES_DB", "payflow")]

    inserted = 0
    fraud_total = 0

    while inserted < total_rows:
        batch = min(BATCH_SIZE, total_rows - inserted)
        transactions, telemetry, fraud_labels = [], [], []

        for _ in range(batch):
            txn_id    = fake.uuid4()
            account_id = fake.uuid4()
            is_fraud  = random.random() < FRAUD_RATE
            pattern   = random.choice(FRAUD_PATTERNS) if is_fraud else None
            amount    = generate_amount(is_fraud, pattern)
            merchant  = random.choice(MERCHANTS)
            currency  = random.choice(CURRENCIES)
            created   = random_timestamp()

            if is_fraud and pattern == "geo_impossible":
                lat = float(rng.uniform(-90, 90))
                lon = float(rng.uniform(-180, 180))
            else:
                lat = float(rng.uniform(8.0, 11.5))
                lon = float(rng.uniform(-85.9, -82.5))

            transactions.append((txn_id, account_id, merchant, amount, currency, "confirmed", created))
            telemetry.append({
                "transaction_id": txn_id,
                "device_type":    random.choice(["mobile", "desktop", "tablet"]),
                "ip_address":     fake.ipv4_public(),
                "user_agent":     fake.user_agent(),
                "latitude":       lat,
                "longitude":      lon,
                "latency_ms":     int(rng.integers(80, 2000)),
                "timestamp":      created,
            })
            fraud_labels.append((txn_id, is_fraud, pattern, created))
            if is_fraud:
                fraud_total += 1

        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO accounts (account_id, holder_name, email, country)
                VALUES %s ON CONFLICT (account_id) DO NOTHING
            """, [(t[1], fake.name(), fake.email(), "CR") for t in transactions])

            psycopg2.extras.execute_values(cur, """
                INSERT INTO transactions
                  (transaction_id, account_id, merchant, amount, currency, status, created_at)
                VALUES %s ON CONFLICT (transaction_id) DO NOTHING
            """, transactions)

            psycopg2.extras.execute_values(cur, """
                INSERT INTO fraud_labels (transaction_id, is_fraud, fraud_pattern, labeled_at)
                VALUES %s ON CONFLICT (transaction_id) DO NOTHING
            """, fraud_labels)

        pg_conn.commit()
        mongo_db.telemetry_events.insert_many(telemetry)

        inserted += batch
        log.info(f"Progress: {inserted}/{total_rows} rows inserted ({fraud_total} fraud so far)")

    pg_conn.close()
    log.info(f"Done. {total_rows} transactions seeded, {fraud_total} fraud ({fraud_total/total_rows:.1%})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=50000)
    args = parser.parse_args()
    seed(args.rows)
