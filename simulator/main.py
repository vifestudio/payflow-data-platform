import os
import time
import random
import logging
from datetime import datetime, timezone
from decimal import Decimal

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
FRAUD_RATE = float(os.getenv("SIM_FRAUD_RATE", "0.015"))
INTERVAL = int(os.getenv("SIM_INTERVAL_SECONDS", "300"))

FRAUD_PATTERNS = ["card_testing", "geo_impossible", "amount_anomaly"]

MERCHANT_VARIANTS = {
    "Amazon":      ["Amazon", "AMAZON.COM", "Amazon.com", "AMZN Marketplace",
                    "Amazon Prime", "amazon.com*Digital", "AMAZON MKTPLACE"],
    "Uber":        ["Uber", "UBER*TRIP", "Uber Technologies", "UBER TRIP CR",
                    "UBER EATS", "Uber Eats CR"],
    "Netflix":     ["Netflix", "NETFLIX.COM", "Netflix.com", "NETFLIX INC",
                    "NETFLIX STREAMING", "Netflix Subscription"],
    "Spotify":     ["Spotify", "SPOTIFY AB", "Spotify Premium", "SPOTIFY USA",
                    "spotify.com", "Spotify Music"],
    "Apple":       ["Apple", "APPLE.COM/BILL", "Apple iTunes", "APPLE SERVICES",
                    "iTunes Store", "Apple Store Online"],
    "Walmart":     ["Walmart", "WALMART SUPERCENTER", "Walmart.com", "WAL-MART",
                    "WALMART #1842", "Walmart Grocery"],
    "McDonald's":  ["McDonald's", "MCDONALDS", "MC DONALDS #101", "McDONALD'S #247",
                    "MCDONALD'S REST", "Mcdonald s CR"],
    "Shell":       ["Shell", "SHELL OIL", "SHELL SERVICE STA", "SHELL GAS STATION",
                    "Shell #789 CR", "SHELL COMBUSTIBLE"],
    "Airbnb":      ["Airbnb", "AIRBNB INC", "Airbnb Payments", "AIRBNB * HM48XZ",
                    "airbnb.com", "AIRBNB BOOKING"],
    "Steam":       ["Steam", "STEAM GAMES", "Valve Steam", "STEAM PURCHASE",
                    "store.steampowered", "Steam Digital"],
}

CURRENCIES = ["USD", "EUR", "GBP", "CRC", "MXN"]

STATUS_CHOICES = ["confirmed", "declined", "pending"]
STATUS_PROBS   = [0.85, 0.10, 0.05]


def hourly_multiplier():
    hour = datetime.now(timezone.utc).hour
    # Peak hours 9-11am and 7-9pm UTC
    if 9 <= hour <= 11 or 19 <= hour <= 21:
        return 2.5
    if 0 <= hour <= 5:
        return 0.3
    return 1.0


def generate_amount(is_fraud, pattern):
    if is_fraud and pattern == "amount_anomaly":
        return round(float(rng.lognormal(mean=8.0, sigma=0.5)), 2)
    return round(float(rng.lognormal(mean=4.5, sigma=1.2)), 2)


def generate_batch(pg_conn, mongo_db, batch_size):
    now = datetime.now(timezone.utc)
    transactions = []
    telemetry = []
    fraud_labels = []

    for _ in range(batch_size):
        txn_id = fake.uuid4()
        account_id = fake.uuid4()
        is_fraud = random.random() < FRAUD_RATE
        pattern = random.choice(FRAUD_PATTERNS) if is_fraud else None
        amount = generate_amount(is_fraud, pattern)
        canonical = random.choice(list(MERCHANT_VARIANTS.keys()))
        merchant = random.choice(MERCHANT_VARIANTS[canonical])
        currency = random.choice(CURRENCIES)
        status = random.choices(STATUS_CHOICES, weights=STATUS_PROBS, k=1)[0]

        if is_fraud and pattern == "geo_impossible":
            lat = float(rng.uniform(-90, 90))
            lon = float(rng.uniform(-180, 180))
        else:
            lat = float(rng.uniform(8.0, 11.5))   # Costa Rica bounding box
            lon = float(rng.uniform(-85.9, -82.5))

        transactions.append({
            "transaction_id": txn_id,
            "account_id": account_id,
            "merchant": merchant,
            "amount": amount,
            "currency": currency,
            "status": status,
            "created_at": now,
        })

        telemetry.append({
            "transaction_id": txn_id,
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "ip_address": fake.ipv4_public(),
            "user_agent": fake.user_agent(),
            "latitude": lat,
            "longitude": lon,
            "latency_ms": int(rng.integers(80, 2000)),
            "timestamp": now,
        })

        fraud_labels.append({
            "transaction_id": txn_id,
            "is_fraud": is_fraud,
            "fraud_pattern": pattern,
            "labeled_at": now,
        })

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO accounts
              (account_id, holder_name, email, country)
            VALUES %s
            ON CONFLICT (account_id) DO NOTHING
            """,
            [(t["account_id"], fake.name(), fake.email(), "CR")
             for t in transactions],
        )
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO transactions
              (transaction_id, account_id, merchant, amount, currency, status, created_at)
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [(t["transaction_id"], t["account_id"], t["merchant"],
              t["amount"], t["currency"], t["status"], t["created_at"])
             for t in transactions],
        )
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO fraud_labels
              (transaction_id, is_fraud, fraud_pattern, labeled_at)
            VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [(f["transaction_id"], f["is_fraud"], f["fraud_pattern"], f["labeled_at"])
             for f in fraud_labels],
        )
    pg_conn.commit()

    mongo_db.telemetry_events.insert_many(telemetry)

    fraud_count = sum(1 for f in fraud_labels if f["is_fraud"])
    log.info(f"Batch inserted: {batch_size} transactions, {fraud_count} fraud ({fraud_count/batch_size:.1%})")


def wait_for_postgres(dsn, retries=10, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            log.info("Postgres ready")
            return conn
        except psycopg2.OperationalError:
            log.warning(f"Postgres not ready, retry {i+1}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")


def main():
    pg_conn = wait_for_postgres(POSTGRES_DSN)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[os.getenv("POSTGRES_DB", "techflow")]

    log.info(f"Simulator started. Interval: {INTERVAL}s, Fraud rate: {FRAUD_RATE:.1%}")

    while True:
        batch_size = max(1, int(10 * hourly_multiplier()))
        generate_batch(pg_conn, mongo_db, batch_size)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()