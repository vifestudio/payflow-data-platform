"""Scoring API: decision thresholds, validation, model loading, metrics."""
import joblib
import pytest
from fastapi.testclient import TestClient

from tests.conftest import load_module
from tests.ml.test_train import synthetic_frame


@pytest.fixture(scope="module")
def train():
    return load_module("train", "ml/train.py")


@pytest.fixture(scope="module")
def serve():
    return load_module("serve", "ml/serve.py")


@pytest.fixture(scope="module")
def client(train, serve, tmp_path_factory):
    df = synthetic_frame(train)
    y = df["is_fraud"].astype(int)
    pipe = train.build_pipeline(scale_pos_weight=(y == 0).sum() / (y == 1).sum())
    pipe.fit(df[train.ALL_FEATURES], y)

    model_path = tmp_path_factory.mktemp("model") / "fraud_pipeline.joblib"
    joblib.dump(pipe, model_path)

    original = serve.MODEL_PATH
    serve.MODEL_PATH = model_path
    with TestClient(serve.app) as c:
        yield c
    serve.MODEL_PATH = original


VALID = {
    "transaction_id": "txn-001",
    "amount_usd": 12.5,
    "hour_of_day": 3,
    "day_of_week": 2,
    "is_weekend": False,
    "account_age_days": 20,
    "latency_ms": 15.0,
    "account_country": "CR",
    "device_type": "mobile",
    "currency": "USD",
}


# --- decision thresholds ---------------------------------------------------

@pytest.mark.parametrize("probability,expected", [
    (0.0, "approve"),
    (0.2999, "approve"),
    (0.30, "review"),
    (0.6999, "review"),
    (0.70, "decline"),
    (1.0, "decline"),
])
def test_decide_boundaries(serve, probability, expected):
    assert serve.decide(probability) == expected


def test_decide_follows_configured_thresholds(serve, monkeypatch):
    monkeypatch.setattr(serve, "REVIEW_THRESHOLD", 0.10)
    monkeypatch.setattr(serve, "DECLINE_THRESHOLD", 0.20)
    assert serve.decide(0.05) == "approve"
    assert serve.decide(0.15) == "review"
    assert serve.decide(0.25) == "decline"


# --- endpoints -------------------------------------------------------------

def test_health_reports_model_loaded(client):
    assert client.get("/health").json() == {"status": "ok", "model_loaded": True}


def test_model_endpoint_matches_training_features(client, train):
    body = client.get("/model").json()
    assert body["features"] == train.ALL_FEATURES
    assert body["review_threshold"] < body["decline_threshold"]


def test_predict_returns_consistent_decision(client, serve):
    r = client.post("/predict", json=VALID)
    assert r.status_code == 200
    body = r.json()
    assert body["transaction_id"] == "txn-001"
    assert 0 <= body["probability"] <= 1
    assert body["decision"] == serve.decide(body["probability"])
    assert body["is_fraud"] == (body["probability"] >= serve.FRAUD_THRESHOLD)


def test_predict_separates_fraud_like_from_legit_like(client):
    fraud_like = {**VALID, "amount_usd": 1.0, "latency_ms": 10.0}
    legit_like = {**VALID, "amount_usd": 150.0, "latency_ms": 250.0}
    p_fraud = client.post("/predict", json=fraud_like).json()["probability"]
    p_legit = client.post("/predict", json=legit_like).json()["probability"]
    assert p_fraud > p_legit


def test_predict_normalizes_input_casing(client):
    payload = {**VALID, "currency": " usd ", "account_country": "cr", "device_type": "MOBILE"}
    assert client.post("/predict", json=payload).status_code == 200


@pytest.mark.parametrize("field,value", [
    ("amount_usd", 0),
    ("amount_usd", -5),
    ("hour_of_day", 24),
    ("hour_of_day", -1),
    ("day_of_week", 7),
    ("account_age_days", -1),
    ("latency_ms", -0.1),
    ("currency", "US"),
    ("account_country", "C"),
])
def test_predict_rejects_invalid_input(client, field, value):
    assert client.post("/predict", json={**VALID, field: value}).status_code == 422


def test_predict_rejects_missing_field(client):
    payload = {k: v for k, v in VALID.items() if k != "latency_ms"}
    assert client.post("/predict", json=payload).status_code == 422


def test_predict_returns_503_when_model_not_loaded(client, serve, monkeypatch):
    monkeypatch.setattr(serve, "_pipeline", None)
    assert client.post("/predict", json=VALID).status_code == 503


def test_metrics_expose_prediction_counters(client):
    client.post("/predict", json=VALID)
    text = client.get("/metrics").text
    assert "payflow_predictions_total" in text
    assert "payflow_predict_latency_seconds" in text


def test_startup_fails_fast_when_model_file_is_missing(serve, tmp_path, monkeypatch):
    monkeypatch.setattr(serve, "MODEL_PATH", tmp_path / "does_not_exist.joblib")
    with pytest.raises(RuntimeError, match="Model not found"):
        with TestClient(serve.app):
            pass
