"""
FastAPI fraud scoring service.

Loads the trained pipeline once at startup and serves predictions in-memory.
Called by the Kafka consumer for real-time scoring of every transaction.

Endpoints:
  POST /predict:  returns fraud probability for a single transaction
  GET  /health:   liveness check
  GET  /model:    model metadata
"""
import os
from contextlib import asynccontextmanager
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field, field_validator

# Prometheus metrics scraped at /metrics (Grafana streaming dashboard).
PREDICT_LATENCY = Histogram(
    "payflow_predict_latency_seconds",
    "Latency of the /predict fraud-scoring call",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
PREDICTIONS = Counter(
    "payflow_predictions_total", "Predictions served, by decision", ["decision"]
)
FRAUD_PROBABILITY = Histogram(
    "payflow_fraud_probability",
    "Distribution of model fraud probabilities",
    buckets=(0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.0),
)

ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = ROOT / "ml" / "model" / "fraud_pipeline.joblib"

FRAUD_THRESHOLD = float(os.environ.get("FRAUD_THRESHOLD", "0.5"))
REVIEW_THRESHOLD = float(os.environ.get("REVIEW_THRESHOLD", "0.30"))
DECLINE_THRESHOLD = float(os.environ.get("DECLINE_THRESHOLD", "0.70"))


def decide(probability: float) -> str:
    if probability >= DECLINE_THRESHOLD:
        return "decline"
    if probability >= REVIEW_THRESHOLD:
        return "review"
    return "approve"


# ---------------------------------------------------------------------------
# Model loaded once at startup, never on every request
# ---------------------------------------------------------------------------
_pipeline = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pipeline
    if not MODEL_PATH.exists():
        raise RuntimeError(f"Model not found at {MODEL_PATH}. Run ml/train.py first.")
    _pipeline = joblib.load(MODEL_PATH)
    yield


app = FastAPI(title="PayFlow Fraud Scoring API", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class TransactionIn(BaseModel):
    transaction_id: str
    amount_usd: float = Field(..., gt=0)
    hour_of_day: int = Field(..., ge=0, le=23)
    day_of_week: int = Field(..., ge=0, le=6)
    is_weekend: bool
    account_age_days: int = Field(..., ge=0)
    latency_ms: float = Field(..., ge=0)
    account_country: str = Field(..., min_length=2, max_length=3)
    device_type: str
    currency: str = Field(..., min_length=3, max_length=3)

    @field_validator("currency")
    @classmethod
    def currency_upper(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("account_country")
    @classmethod
    def country_upper(cls, v: str) -> str:
        return v.upper().strip()

    @field_validator("device_type")
    @classmethod
    def device_lower(cls, v: str) -> str:
        return v.lower().strip()


class PredictionOut(BaseModel):
    transaction_id: str
    is_fraud: bool
    decision: str
    probability: float
    model_version: str = "1.0.0"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": _pipeline is not None}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/model")
def model_info():
    return {
        "model_path": str(MODEL_PATH),
        "fraud_threshold": FRAUD_THRESHOLD,
        "review_threshold": REVIEW_THRESHOLD,
        "decline_threshold": DECLINE_THRESHOLD,
        "features": [
            "amount_usd", "hour_of_day", "day_of_week", "is_weekend",
            "account_age_days", "latency_ms",
            "account_country", "device_type", "currency",
        ],
    }


@app.post("/predict", response_model=PredictionOut)
def predict(txn: TransactionIn):
    if _pipeline is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    row = pd.DataFrame([{
        "amount_usd":       txn.amount_usd,
        "hour_of_day":      txn.hour_of_day,
        "day_of_week":      txn.day_of_week,
        "is_weekend":       int(txn.is_weekend),
        "account_age_days": txn.account_age_days,
        "latency_ms":       txn.latency_ms,
        "account_country":  txn.account_country,
        "device_type":      txn.device_type,
        "currency":         txn.currency,
    }])

    with PREDICT_LATENCY.time():
        probability = float(_pipeline.predict_proba(row)[0, 1])
    decision = decide(probability)
    is_fraud = probability >= FRAUD_THRESHOLD
    PREDICTIONS.labels(decision=decision).inc()
    FRAUD_PROBABILITY.observe(probability)

    return PredictionOut(
        transaction_id=txn.transaction_id,
        is_fraud=is_fraud,
        decision=decision,
        probability=round(probability, 6),
    )
