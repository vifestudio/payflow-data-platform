# PayFlow Data Platform

End-to-end data engineering platform for **PayFlow**, a fictional digital-payments fintech. Built on synthetic data and a 100% free, open-source stack. Covers the full data lifecycle: real-time ingestion, a cloud lakehouse with Medallion architecture, ML-powered fraud detection, pipeline orchestration, and a streaming layer for millisecond-latency decisions.

## Architecture

A real fintech runs two parallel pipelines. PayFlow implements both.

```mermaid
flowchart TD
    subgraph SIM["Data Generation"]
        S["Simulator\nPython · Faker · NumPy\nevery 5 min"]
    end

    subgraph STREAMING["Streaming Pipeline · ms latency"]
        KP["Kafka Producer"]
        KB["Kafka Broker\nApache Kafka"]
        KC["Fraud Consumer\nkafka-python"]
        API2["FastAPI /predict\nfraud scoring service"]
    end

    subgraph SOURCES["Operational Sources"]
        PG[(PostgreSQL\ntransactions · accounts\nfraud_labels · fraud_scores)]
        MG[(MongoDB\ntelemetry_events)]
        FX["FX Rates API\nFastAPI :8001"]
    end

    subgraph BATCH["Batch Pipeline · hourly"]
        AB["Airbyte\nincremental · full-refresh"]
        subgraph LAKE["Cloudflare R2 · Delta Lakehouse"]
            B["Bronze\nRaw Parquet"]
            SI["Silver · Delta Lake\ncleaned · typed · deduplicated"]
            G["Gold · Delta Lake\naggregated · ML-ready features"]
            B --> SI --> G
        end
        DBT["dbt + DuckDB"]
        VEC["pgvector\nmerchant name normalization\nsentence-transformers embeddings"]
        ML["XGBoost + MLflow\nfraud detection model"]
    end

    subgraph ANALYTICS["Analytics Layer"]
        BQ[("BigQuery\npayflow_analytics\ngold tables · marts · clean merchants")]
    end

    LS["Looker Studio"]
    TB["Tableau"]
    PB["Power BI"]
    NLQ["NL query · Gemini in BigQuery"]
    GRAF["Grafana\nobservability · dashboards"]

    S -->|"publishes event"| KP --> KB --> KC
    S -->|"telemetry"| MG
    KC -->|"POST /predict"| API2
    KC -->|"writes account + txn + label + score"| PG

    PG & MG & FX --> AB --> B
    B --> DBT --> SI --> DBT --> G

    G -->|"trains on historical features"| ML
    ML -->|"serves latest model"| API2

    G -->|"normalize merchants"| VEC -->|"clean merchants"| BQ
    BQ --> LS & TB & PB & NLQ

    PG & KC -.->|"metrics"| GRAF

    PREF["Prefect · hourly orchestration"] -. "triggers" .-> AB & DBT & ML & VEC
```

The batch pipeline trains the model on historical Gold features. The streaming pipeline scores each transaction in real time using that model before the payment clears. The fraud score is written back to PostgreSQL, where Airbyte picks it up in the next sync cycle — that feedback becomes part of the next training run, closing the loop.

Gold data is loaded into BigQuery as the analytical warehouse after merchant names are normalized via pgvector. Looker Studio connects natively to BigQuery for dashboards and reporting.

## Stack

| Layer | Tool |
| --- | --- |
| Data simulation | Python, Faker, NumPy |
| Operational sources | PostgreSQL 16, MongoDB 7, FastAPI |
| Ingestion | Airbyte (incremental + full-refresh) |
| Lakehouse storage | Cloudflare R2 (Parquet + Delta Lake) |
| Transform / compute | dbt + DuckDB |
| Orchestration | Prefect |
| Machine learning | XGBoost + scikit-learn + MLflow |
| Real-time serving | Apache Kafka + FastAPI |
| Merchant normalization | pgvector + sentence-transformers |
| Analytical warehouse | BigQuery |
| BI | Looker Studio |
| Observability | Grafana |
| Data quality | dbt tests |
| CI | GitHub Actions |

## Repository layout

| Path | Role |
| --- | --- |
| `simulator/` | Synthetic transaction and fraud generator |
| `sources/postgres/` | Schema: accounts, transactions, fraud_labels |
| `sources/fx-api/` | FX rates microservice (FastAPI, port 8001) |
| `ingestion/` | Airbyte connection definitions |
| `transform/` | dbt project: Bronze → Silver → Gold |
| `orchestration/` | Prefect flows |
| `serving/` | FastAPI: /predict, /search, vector search |
| `streaming/` | Kafka producer and fraud consumer |
| `infra/` | docker-compose, env templates |

## Running locally

Requires Docker Desktop.

```bash
cp .env.example .env
# fill in R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY in .env
docker compose up -d
```

The simulator starts immediately and writes a batch every 5 minutes. FX rates are available at `http://localhost:8001/rates`. Airbyte runs at `http://localhost:8000`.

## Design decisions

**fraud_labels lives in a separate table.**
In production, fraud truth arrives days or weeks after the transaction via chargebacks and analyst review, never at payment time. Embedding it in `transactions` would create data leakage for any downstream ML model and misrepresents how fintech data actually flows.

**Incremental sync for transactional tables, full refresh for telemetry.**
`accounts`, `transactions`, and `fraud_labels` use incremental append in Airbyte: each sync adds only new records, so Bronze accumulates history. `telemetry_events` uses full refresh because MongoDB overwrites the collection on each run and historical event logs have no business value at this scale.

**FX rates ingested via Airbyte Connector Builder, not written to Postgres.**
The FX API simulates an external data provider. In production you would never ask a third-party API to write directly to your database; you build a connector that pulls from it on a schedule. The Connector Builder approach reflects that boundary correctly and demonstrates custom connector development.

**Cloudflare R2 as the lakehouse storage layer.**
R2 is S3-compatible with no egress fees, making it the right choice for a portfolio project that needs to demonstrate cloud lakehouse patterns without incurring costs. In production the same architecture runs on S3, GCS, or ADLS with no changes to the dbt or DuckDB layer.

**DuckDB for compute, not Spark.**
The dataset is synthetic and small. DuckDB reads Parquet directly from R2 via the httpfs extension, executes dbt models, and writes Silver and Gold back to R2 without a cluster. For a production fintech at scale, the same Medallion models run on Spark or BigQuery with minimal changes to the SQL.

**Data lake and analytical warehouse are separate layers.**
Cloudflare R2 holds the full history as Delta Lake files — cheap, durable, queryable by DuckDB. BigQuery holds only the Gold tables, optimized for fast analytical queries and BI tools. This is the standard lakehouse pattern: the lake owns the data, the warehouse owns the queries. R2 is equivalent to S3 or GCS in this role; swapping it out requires no changes to the dbt or DuckDB layer.

**pgvector for merchant normalization, not for the fraud model.**
Merchant names in the raw data are dirty: "Amazon", "AMAZON.COM", "AMZN Marketplace", and "amazon.com*Digital" are the same merchant. pgvector generates sentence-transformer embeddings for each raw name and finds the nearest canonical name by cosine similarity. This normalization runs in the Prefect pipeline before Gold is loaded into BigQuery, so Looker Studio dashboards show clean merchant names. The fraud model deliberately excludes merchant as a feature — dirty names at training time would not generalize to unseen variants at inference time.

**Model bootstrapped on a 100k historical seed.**
A fraud model needs enough labeled history to learn from before it can score anything. The first model is trained on a one-time seed of 100,000 transactions spanning 365 days (`simulator/bulk_seed.py`), which gives it realistic fraud patterns and class balance from day one. This seed is a training bootstrap, not part of the runtime architecture: in steady state every transaction reaches Postgres only through the streaming path (Kafka to the fraud consumer), and scheduled retrains use the history that accumulates there.

**Two pipelines, one model.**
The batch pipeline trains the fraud model on historical Gold features. The streaming pipeline serves that same model for real-time decisions. MLflow is the bridge: batch registers a new version, FastAPI promotes it without redeployment. This is the standard MLOps pattern at any fintech operating at scale.

## Evidence

### Bronze layer live in Cloudflare R2

22 MB across three source folders after the first Airbyte sync. All streams landed as Parquet: accounts, transactions, fraud_labels from Postgres; telemetry_events from MongoDB; fx_rates from the custom CDK connector.

![R2 Bronze layer](docs/screenshots/bronze-r2-phase2.png)

### Airbyte connections all green

Three connections running every 6 hours: MongoDB, PayFlow FX Rates API (custom CDK connector), Postgres. All enabled, all healthy.

![Airbyte connections](docs/screenshots/airbyte-connections-phase2.png)
