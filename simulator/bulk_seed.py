"""
One-shot bulk seed: generates N transactions distributed across the last 90 days.
Realistic fintech patterns for ML training and pgvector merchant deduplication.

Fraud patterns implemented:
  card_testing   - burst of 10-20 small transactions from same account in <5 min
  geo_impossible - same account transacts in CR and far-away country within 15 min
  amount_anomaly - single outlier amount via lognormal high-mean distribution

Usage:
    docker compose exec simulator python bulk_seed.py --rows 100000
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

FRAUD_RATE  = float(os.getenv("SIM_FRAUD_RATE", "0.015"))
DAYS_BACK   = 365
BATCH_SIZE  = 500
ACCOUNT_POOL_SIZE = 3000

ACCOUNT_TYPES   = ["personal", "business", "premium"]
ACCOUNT_WEIGHTS = [0.70,       0.20,       0.10]
CREDIT_LIMITS   = {
    "personal": (500,    5_000),
    "business": (5_000,  50_000),
    "premium":  (10_000, 100_000),
}

# Each key is the canonical name pgvector will later normalize to.
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

MERCHANT_CATEGORY = {
    "Amazon":      "retail",
    "Uber":        "transport",
    "Netflix":     "streaming",
    "Spotify":     "streaming",
    "Apple":       "tech",
    "Walmart":     "retail",
    "McDonald's":  "food",
    "Shell":       "gas",
    "Airbnb":      "travel",
    "Steam":       "gaming",
}

# (lat, lon) for geo_impossible second transaction - physically far from Costa Rica
GEO_IMPOSSIBLE_LOCATIONS = [
    (51.509865, -0.118092),   # London
    (55.755825, 37.617298),   # Moscow
    (35.689487, 139.691711),  # Tokyo
    (-33.868820, 151.209290), # Sydney
    (40.712776, -74.005974),  # New York
    (48.856613, 2.352222),    # Paris
    (1.352083, 103.819836),   # Singapore
    (-23.550520, -46.633308), # Sao Paulo
]

CURRENCIES = ["USD", "EUR", "GBP", "CRC", "MXN"]

# Status distribution: 85% confirmed, 10% declined, 5% pending
STATUS_WEIGHTS = [("confirmed", 0.85), ("declined", 0.10), ("pending", 0.05)]
STATUS_CHOICES, STATUS_PROBS = zip(*STATUS_WEIGHTS)


def pick_status():
    return random.choices(STATUS_CHOICES, weights=STATUS_PROBS, k=1)[0]


def pick_dirty_merchant():
    canonical = random.choice(list(MERCHANT_VARIANTS.keys()))
    dirty = random.choice(MERCHANT_VARIANTS[canonical])
    return canonical, dirty


def random_timestamp_with_peaks():
    """Distribute over last 90 days with realistic daily peaks."""
    day_offset = random.randint(0, DAYS_BACK - 1)
    base = datetime.now(timezone.utc) - timedelta(days=day_offset)

    # Build weighted hour bucket: peaks at 9-11am and 7-9pm UTC
    hour_weights = []
    for h in range(24):
        if 9 <= h <= 11 or 19 <= h <= 21:
            hour_weights.append(3.0)
        elif 0 <= h <= 5:
            hour_weights.append(0.3)
        else:
            hour_weights.append(1.0)

    hour = random.choices(range(24), weights=hour_weights, k=1)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base.replace(hour=hour, minute=minute, second=second, microsecond=0)


def generate_amount(is_fraud, pattern):
    if is_fraud and pattern == "amount_anomaly":
        return round(float(rng.lognormal(mean=8.0, sigma=0.5)), 2)
    # Normal: lognormal centered around ~$90 (mean=4.5 on log scale)
    return round(float(rng.lognormal(mean=4.5, sigma=1.2)), 2)


def build_account_pool(pg_conn, size):
    """Pre-generate a pool of persistent accounts."""
    accounts = []
    for _ in range(size):
        days_ago = random.randint(0, 730)
        created = datetime.now(timezone.utc) - timedelta(days=days_ago)
        acct_type = random.choices(ACCOUNT_TYPES, weights=ACCOUNT_WEIGHTS, k=1)[0]
        lo, hi = CREDIT_LIMITS[acct_type]
        credit_limit = round(float(rng.uniform(lo, hi)), 2)
        accounts.append({
            "account_id": fake.uuid4(),
            "holder_name": fake.name(),
            "email": fake.email(),
            "country": "CR",
            "account_type": acct_type,
            "credit_limit": credit_limit,
            "created_at": created,
        })

    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO accounts
              (account_id, holder_name, email, country, account_type, credit_limit, created_at)
            VALUES %s ON CONFLICT (account_id) DO NOTHING
        """, [(a["account_id"], a["holder_name"], a["email"], a["country"],
               a["account_type"], a["credit_limit"], a["created_at"])
              for a in accounts])
    pg_conn.commit()
    log.info(f"Account pool of {size} accounts created")
    return [a["account_id"] for a in accounts]


def build_normal_transaction(account_id, is_fraud, pattern):
    canonical, dirty = pick_dirty_merchant()
    base_time = random_timestamp_with_peaks()
    status = pick_status()

    if is_fraud and pattern == "geo_impossible":
        lat = float(rng.uniform(8.0, 11.5))
        lon = float(rng.uniform(-85.9, -82.5))
    else:
        lat = float(rng.uniform(8.0, 11.5))
        lon = float(rng.uniform(-85.9, -82.5))

    txn_id = fake.uuid4()
    amount = generate_amount(is_fraud, pattern)
    currency = random.choice(CURRENCIES)

    return {
        "txn": (txn_id, account_id, dirty, amount, currency, status, base_time),
        "tel": {
            "transaction_id": txn_id,
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "ip_address": fake.ipv4_public(),
            "user_agent": fake.user_agent(),
            "latitude": lat,
            "longitude": lon,
            "latency_ms": int(rng.integers(80, 2000)),
            "timestamp": base_time,
        },
        "fraud": (txn_id, is_fraud, pattern, base_time),
    }


def build_card_testing_burst(account_id):
    """10-20 small transactions from same account within 5 minutes."""
    base_time = random_timestamp_with_peaks()
    n = random.randint(10, 20)
    rows = []
    for _ in range(n):
        txn_id = fake.uuid4()
        offset = timedelta(seconds=random.randint(0, 300))
        ts = base_time + offset
        amount = round(random.uniform(0.50, 5.00), 2)
        canonical, dirty = pick_dirty_merchant()
        rows.append({
            "txn": (txn_id, account_id, dirty, amount, random.choice(CURRENCIES), "confirmed", ts),
            "tel": {
                "transaction_id": txn_id,
                "device_type": "mobile",
                "ip_address": fake.ipv4_public(),
                "user_agent": fake.user_agent(),
                "latitude": float(rng.uniform(8.0, 11.5)),
                "longitude": float(rng.uniform(-85.9, -82.5)),
                "latency_ms": int(rng.integers(80, 500)),
                "timestamp": ts,
            },
            "fraud": (txn_id, True, "card_testing", ts),
        })
    return rows


def build_geo_impossible_pair(account_id):
    """Same account: CR transaction, then 15 min later a far-away transaction."""
    base_time = random_timestamp_with_peaks()
    second_time = base_time + timedelta(minutes=15)
    far_lat, far_lon = random.choice(GEO_IMPOSSIBLE_LOCATIONS)

    txn1 = fake.uuid4()
    txn2 = fake.uuid4()
    canonical1, dirty1 = pick_dirty_merchant()
    canonical2, dirty2 = pick_dirty_merchant()
    currency = random.choice(CURRENCIES)
    amount1 = generate_amount(False, None)
    amount2 = generate_amount(True, "geo_impossible")

    return [
        {
            "txn": (txn1, account_id, dirty1, amount1, currency, pick_status(), base_time),
            "tel": {
                "transaction_id": txn1,
                "device_type": random.choice(["mobile", "desktop"]),
                "ip_address": fake.ipv4_public(),
                "user_agent": fake.user_agent(),
                "latitude": float(rng.uniform(8.0, 11.5)),
                "longitude": float(rng.uniform(-85.9, -82.5)),
                "latency_ms": int(rng.integers(80, 2000)),
                "timestamp": base_time,
            },
            "fraud": (txn1, False, None, base_time),
        },
        {
            "txn": (txn2, account_id, dirty2, amount2, currency, "confirmed", second_time),
            "tel": {
                "transaction_id": txn2,
                "device_type": "mobile",
                "ip_address": fake.ipv4_public(),
                "user_agent": fake.user_agent(),
                "latitude": far_lat,
                "longitude": far_lon,
                "latency_ms": int(rng.integers(80, 2000)),
                "timestamp": second_time,
            },
            "fraud": (txn2, True, "geo_impossible", second_time),
        },
    ]


def flush_batch(pg_conn, mongo_db, transactions, telemetry, fraud_labels):
    with pg_conn.cursor() as cur:
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
    if telemetry:
        mongo_db.telemetry_events.insert_many(telemetry)


def seed(total_rows):
    pg_conn = psycopg2.connect(POSTGRES_DSN)
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[os.getenv("POSTGRES_DB", "payflow")]

    account_pool = build_account_pool(pg_conn, ACCOUNT_POOL_SIZE)

    inserted = 0
    fraud_total = 0
    transactions, telemetry, fraud_labels = [], [], []

    while inserted < total_rows:
        remaining = total_rows - inserted
        account_id = random.choice(account_pool)
        is_fraud = random.random() < FRAUD_RATE

        if is_fraud:
            pattern = random.choice(["card_testing", "geo_impossible", "amount_anomaly"])

            if pattern == "card_testing" and remaining >= 10:
                rows = build_card_testing_burst(account_id)
            elif pattern == "geo_impossible" and remaining >= 2:
                rows = build_geo_impossible_pair(account_id)
            else:
                pattern = "amount_anomaly"
                rows = [build_normal_transaction(account_id, True, pattern)]
        else:
            rows = [build_normal_transaction(account_id, False, None)]

        for r in rows:
            transactions.append(r["txn"])
            telemetry.append(r["tel"])
            fraud_labels.append(r["fraud"])
            if r["fraud"][1]:
                fraud_total += 1
            inserted += 1
            if inserted >= total_rows:
                break

        if len(transactions) >= BATCH_SIZE:
            flush_batch(pg_conn, mongo_db, transactions, telemetry, fraud_labels)
            log.info(f"Progress: {inserted}/{total_rows} ({fraud_total} fraud)")
            transactions, telemetry, fraud_labels = [], [], []

    if transactions:
        flush_batch(pg_conn, mongo_db, transactions, telemetry, fraud_labels)

    pg_conn.close()
    log.info(f"Done. {inserted} transactions, {fraud_total} fraud ({fraud_total/inserted:.1%})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    seed(args.rows)
