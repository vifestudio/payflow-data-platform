import json
import os
import random
import time
import logging
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
from faker import Faker
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()
rng = np.random.default_rng()

MONGO_URI = (
    f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}"
    f"@mongo:{os.getenv('MONGO_PORT', '27017')}/?authSource=admin"
)
FRAUD_RATE = float(os.getenv("SIM_FRAUD_RATE", "0.015"))
INTERVAL = int(os.getenv("SIM_INTERVAL_SECONDS", "300"))
KAFKA_TOPIC = "transactions"

FRAUD_PATTERNS = ["card_testing", "geo_impossible", "amount_anomaly"]

ACCOUNT_TYPES   = ["personal", "business", "premium"]
ACCOUNT_WEIGHTS = [0.70,       0.20,       0.10]

CREDIT_LIMITS = {
    "personal": (500,    5_000),
    "business": (5_000,  50_000),
    "premium":  (10_000, 100_000),
}

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


def generate_batch(mongo_db, batch_size, kafka_producer=None):
    now = datetime.now(timezone.utc)
    telemetry = []
    events = []
    fraud_count = 0

    for _ in range(batch_size):
        txn_id = str(fake.uuid4())
        account_id = str(fake.uuid4())
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

        acct_type = random.choices(ACCOUNT_TYPES, weights=ACCOUNT_WEIGHTS, k=1)[0]
        lo, hi = CREDIT_LIMITS[acct_type]
        credit_limit = round(float(rng.uniform(lo, hi)), 2)

        device_type = random.choice(["mobile", "desktop", "tablet"])
        latency_ms = int(rng.integers(80, 2000))
        account_age_days = int(rng.integers(30, 1500))
        if is_fraud:
            fraud_count += 1

        telemetry.append({
            "transaction_id": txn_id,
            "device_type": device_type,
            "ip_address": fake.ipv4_public(),
            "user_agent": fake.user_agent(),
            "latitude": lat,
            "longitude": lon,
            "latency_ms": latency_ms,
            "timestamp": now,
        })

        # Full event: everything the consumer needs to upsert the account, record
        # the transaction and its label, score it, and persist the decision.
        events.append({
            "transaction_id": txn_id,
            "account_id": account_id,
            "merchant": merchant,
            "amount": amount,
            "currency": currency,
            "status": status,
            "created_at": now.isoformat(),
            "holder_name": fake.name(),
            "email": fake.email(),
            "country": "CR",
            "account_type": acct_type,
            "credit_limit": credit_limit,
            "is_fraud": is_fraud,
            "fraud_pattern": pattern,
            "amount_usd": amount,
            "hour_of_day": now.hour,
            "day_of_week": now.weekday(),
            "is_weekend": now.weekday() >= 5,
            "account_age_days": account_age_days,
            "latency_ms": float(latency_ms),
            "account_country": "CR",
            "device_type": device_type,
        })

    # Telemetry goes straight to MongoDB (operational event store, a batch source).
    mongo_db.telemetry_events.insert_many(telemetry)

    # Everything that reaches Postgres flows through Kafka -> consumer -> ML.
    if kafka_producer:
        for event in events:
            kafka_producer.send(KAFKA_TOPIC, value=event)
        kafka_producer.flush()

    log.info(f"Batch published: {batch_size} transactions, {fraud_count} fraud ({fraud_count/batch_size:.1%})")


def wait_for_kafka(bootstrap, retries=15, delay=5):
    from kafka import KafkaProducer
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            log.info("Kafka ready")
            return producer
        except Exception as e:
            log.warning(f"Kafka not ready, retry {i+1}/{retries}: {e}")
            time.sleep(delay)
    log.warning("Could not connect to Kafka — running without streaming")
    return None


def main():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[os.getenv("POSTGRES_DB", "techflow")]

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    kafka_producer = wait_for_kafka(kafka_bootstrap) if kafka_bootstrap else None
    if not kafka_producer:
        log.warning("KAFKA_BOOTSTRAP_SERVERS not set — no transactions will be published")

    log.info(f"Simulator started. Interval: {INTERVAL}s, Fraud rate: {FRAUD_RATE:.1%}")

    while True:
        batch_size = max(1, int(10 * hourly_multiplier()))
        generate_batch(mongo_db, batch_size, kafka_producer)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()