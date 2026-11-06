"""
Merchant name normalization with pgvector + sentence-transformers.

Raw merchant names are dirty: "Amazon", "AMAZON.COM", "AMZN Marketplace" and
"amazon.com*Digital" are all the same merchant. This step embeds each raw name
with a sentence-transformer, stores canonical merchant embeddings in pgvector,
and resolves every raw name to its nearest canonical merchant by cosine
similarity. The raw -> canonical mapping lands in Postgres (merchant_normalization),
which load_to_bigquery.py joins so BigQuery dashboards show clean names.

Runs in the Prefect pipeline after dbt builds Gold and before the BigQuery load.

Env (from project .env):
  POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DB / POSTGRES_USER / POSTGRES_PASSWORD
"""
import logging
import os
from pathlib import Path

import duckdb
import psycopg2
from dotenv import load_dotenv
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DUCKDB_PATH = ROOT / "transform" / "payflow" / "dev.duckdb"
MODEL_NAME = "all-MiniLM-L6-v2"
EMBED_DIM = 384

# Minimum cosine similarity to accept a raw name as a variant of a known
# merchant. Below this, the raw name is treated as a NEW merchant and kept under
# its own name instead of being forced into the nearest existing bucket. Known
# variants in this dataset score >= 0.47; unrelated new merchants score <= 0.27,
# so 0.40 separates them with margin. Tune via SIMILARITY_THRESHOLD.
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.40"))

# Seed aliases per canonical merchant. A few representative surface forms
# (including the tricky abbreviations embeddings miss on their own) anchor each
# merchant; the embedding model generalizes to any unseen variant by nearest
# neighbour. This mirrors production merchant normalization: a small seed
# dictionary plus semantic matching for the long tail.
MERCHANT_ALIASES = {
    "Amazon":     ["Amazon", "AMZN", "amazon.com", "Amazon Marketplace", "Amazon Prime"],
    "Spotify":    ["Spotify", "spotify.com"],
    "Steam":      ["Steam", "Valve Steam", "store.steampowered.com"],
    "McDonald's": ["McDonald's", "McDonalds", "Mc Donalds"],
    "Uber":       ["Uber", "Uber Eats", "Uber Trip"],
    "Airbnb":     ["Airbnb", "Airbnb Payments"],
    "Apple":      ["Apple", "iTunes", "Apple Store", "App Store", "apple.com"],
    "Netflix":    ["Netflix", "netflix.com"],
    "Shell":      ["Shell", "Shell Gas Station", "Shell Oil"],
    "Walmart":    ["Walmart", "Walmart Supercenter"],
}
CANONICAL_MERCHANTS = list(MERCHANT_ALIASES.keys())

PG_DSN = {
    "host":     os.getenv("POSTGRES_HOST",     "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def distinct_raw_merchants() -> list[str]:
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    rows = con.execute(
        "SELECT DISTINCT merchant FROM main.gold_fraud_features WHERE merchant IS NOT NULL"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def main() -> None:
    log.info(f"Loading embedding model {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)

    raw = distinct_raw_merchants()
    log.info(f"{len(raw)} distinct raw merchant names from Gold")

    conn = psycopg2.connect(**PG_DSN)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    register_vector(conn)

    # Alias embeddings table: every seed alias tagged with its canonical merchant
    aliases = [(alias, canonical) for canonical, al in MERCHANT_ALIASES.items() for alias in al]
    alias_emb = model.encode([a for a, _ in aliases], normalize_embeddings=True)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS merchant_canonical")
        cur.execute(
            f"CREATE TABLE merchant_canonical ("
            f"  alias TEXT PRIMARY KEY,"
            f"  canonical TEXT NOT NULL,"
            f"  embedding vector({EMBED_DIM}) NOT NULL)"
        )
        for (alias, canonical), emb in zip(aliases, alias_emb):
            cur.execute(
                "INSERT INTO merchant_canonical (alias, canonical, embedding) VALUES (%s, %s, %s)",
                (alias, canonical, emb.tolist()),
            )
    conn.commit()
    log.info(f"{len(aliases)} aliases for {len(CANONICAL_MERCHANTS)} canonical merchants embedded into pgvector")

    # Nearest alias -> its canonical for every raw name (cosine distance via <=>).
    # Below the threshold the name is an unknown merchant and keeps its own name.
    raw_emb = model.encode(raw, normalize_embeddings=True)
    new_merchants = 0
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS merchant_normalization")
        cur.execute(
            "CREATE TABLE merchant_normalization ("
            "  raw_merchant TEXT PRIMARY KEY,"
            "  canonical_merchant TEXT NOT NULL,"
            "  similarity REAL NOT NULL,"
            "  is_new BOOLEAN NOT NULL DEFAULT false)"
        )
        for name, emb in zip(raw, raw_emb):
            cur.execute(
                "SELECT canonical, 1 - (embedding <=> %s::vector) AS similarity "
                "FROM merchant_canonical ORDER BY embedding <=> %s::vector LIMIT 1",
                (emb.tolist(), emb.tolist()),
            )
            canonical, similarity = cur.fetchone()
            is_new = similarity < SIMILARITY_THRESHOLD
            if is_new:
                canonical = name  # unknown merchant: keep its own identity
                new_merchants += 1
            cur.execute(
                "INSERT INTO merchant_normalization "
                "(raw_merchant, canonical_merchant, similarity, is_new) VALUES (%s, %s, %s, %s)",
                (name, canonical, float(similarity), is_new),
            )
    conn.commit()
    if new_merchants:
        log.info(f"{new_merchants} raw names below threshold {SIMILARITY_THRESHOLD} kept as new merchants")

    # Report
    with conn.cursor() as cur:
        cur.execute(
            "SELECT canonical_merchant, count(*), round(avg(similarity)::numeric, 3) "
            "FROM merchant_normalization GROUP BY 1 ORDER BY 2 DESC"
        )
        log.info("Raw -> canonical mapping:")
        for canonical, n, avg_sim in cur.fetchall():
            log.info(f"  {canonical:<12} <- {n:>2} variants (avg sim {avg_sim})")

    conn.close()
    log.info(f"Mapping written to merchant_normalization ({len(raw)} rows).")


if __name__ == "__main__":
    main()
