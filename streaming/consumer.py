"""
Kafka consumer that scores every transaction in real time.

This is the only writer to Postgres. Each event carries the full transaction and
its account, so after scoring the consumer upserts the account and writes the
transaction, its label and the fraud score together in one atomic step.

Flow:
  1. Read event from topic "transactions"
  2. POST it to ml-service /predict
  3. Upsert account + insert transaction + label + score in one transaction
"""
import json
import logging
import os
import time

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prometheus_client import Counter, Gauge, start_http_server

load_dotenv()

# Prometheus metrics scraped at :9108/metrics (Grafana streaming dashboard).
METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "9108"))
MESSAGES_PROCESSED = Counter(
    "payflow_messages_processed_total", "Transactions scored and persisted", ["decision"]
)
CONSUMER_ERRORS = Counter("payflow_consumer_errors_total", "Errors while processing", ["kind"])
CONSUMER_LAG = Gauge("payflow_consumer_lag", "Total unconsumed offsets across partitions")


def update_lag(consumer):
    """Set the lag gauge to end_offset - position summed over assigned partitions."""
    try:
        parts = consumer.assignment()
        if not parts:
            return
        ends = consumer.end_offsets(list(parts))
        lag = sum(ends[tp] - consumer.position(tp) for tp in parts)
        CONSUMER_LAG.set(max(lag, 0))
    except Exception:
        pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = "transactions"
KAFKA_GROUP = "fraud-scorer"
ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://ml-service:8010")

POSTGRES_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'postgres')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASSWORD')}"
)

UPSERT_ACCOUNT = """
    INSERT INTO accounts (account_id, holder_name, email, country, account_type, credit_limit)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (account_id) DO NOTHING
"""

INSERT_TXN = """
    INSERT INTO transactions (transaction_id, account_id, merchant, amount, currency, status, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""

INSERT_LABEL = """
    INSERT INTO fraud_labels (transaction_id, is_fraud, fraud_pattern)
    VALUES (%s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""

INSERT_SCORE = """
    INSERT INTO fraud_scores (transaction_id, is_fraud, decision, probability, model_version)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def wait_for_postgres(dsn, retries=15, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(dsn)
            log.info("Postgres ready")
            return conn
        except psycopg2.OperationalError:
            log.warning(f"Postgres not ready, retry {i+1}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres")


def wait_for_ml(url, retries=20, delay=5):
    for i in range(retries):
        try:
            resp = requests.get(f"{url}/health", timeout=5)
            if resp.status_code == 200:
                log.info("ML service ready")
                return
        except requests.RequestException:
            pass
        log.warning(f"ML service not ready, retry {i+1}/{retries}")
        time.sleep(delay)
    raise RuntimeError("Could not reach ML service")


def wait_for_kafka(bootstrap, retries=20, delay=5):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=bootstrap,
                group_id=KAFKA_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=-1,
            )
            log.info("Kafka consumer connected")
            return consumer
        except Exception as e:
            log.warning(f"Kafka not ready, retry {i+1}/{retries}: {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka")


def main():
    pg_conn = wait_for_postgres(POSTGRES_DSN)
    wait_for_ml(ML_SERVICE_URL)
    consumer = wait_for_kafka(KAFKA_BOOTSTRAP)

    start_http_server(METRICS_PORT)
    log.info(f"Prometheus metrics on :{METRICS_PORT}/metrics")
    log.info(f"Listening on topic '{KAFKA_TOPIC}' (group '{KAFKA_GROUP}')...")

    processed = 0
    for message in consumer:
        event = message.value
        txn_id = event.get("transaction_id", "?")
        try:
            resp = requests.post(
                f"{ML_SERVICE_URL}/predict",
                json=event,
                timeout=5,
            )
            resp.raise_for_status()
            result = resp.json()

            # Single atomic write: account, transaction, label and score land
            # together, so a score never exists without its transaction.
            with pg_conn.cursor() as cur:
                cur.execute(UPSERT_ACCOUNT, (
                    event["account_id"], event["holder_name"], event["email"],
                    event["country"], event["account_type"], event["credit_limit"],
                ))
                cur.execute(INSERT_TXN, (
                    event["transaction_id"], event["account_id"], event["merchant"],
                    event["amount"], event["currency"], event["status"], event["created_at"],
                ))
                cur.execute(INSERT_LABEL, (
                    event["transaction_id"], event["is_fraud"], event["fraud_pattern"],
                ))
                cur.execute(INSERT_SCORE, (
                    result["transaction_id"], result["is_fraud"],
                    result["decision"], result["probability"], result["model_version"],
                ))
            pg_conn.commit()

            MESSAGES_PROCESSED.labels(decision=result["decision"]).inc()
            processed += 1
            if processed % 20 == 0:
                update_lag(consumer)

            log.info(f"[{result['decision']}] {result['transaction_id']}  p={result['probability']:.4f}")

        except requests.RequestException as e:
            log.error(f"ML service error for {txn_id}: {e}")
            CONSUMER_ERRORS.labels(kind="ml").inc()
            pg_conn.rollback()
        except psycopg2.Error as e:
            log.error(f"DB error for {txn_id}: {e}")
            CONSUMER_ERRORS.labels(kind="db").inc()
            pg_conn.rollback()
            # Reconnect on broken pipe
            try:
                pg_conn = psycopg2.connect(POSTGRES_DSN)
            except Exception:
                pass
        except Exception as e:
            log.error(f"Unexpected error for {txn_id}: {e}")
            CONSUMER_ERRORS.labels(kind="other").inc()
            pg_conn.rollback()


if __name__ == "__main__":
    main()
