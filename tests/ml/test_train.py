"""Training code: feature contract, time-based split, pipeline behaviour."""
import numpy as np
import pandas as pd
import pytest

from tests.conftest import load_module


@pytest.fixture(scope="module")
def train():
    return load_module("train", "ml/train.py")


def synthetic_frame(train, n=400, seed=0):
    """Small frame with a learnable signal: fraud = tiny amount + low latency."""
    rng = np.random.default_rng(seed)
    fraud = rng.random(n) < 0.2
    df = pd.DataFrame({
        "transaction_date": pd.date_range("2026-01-01", periods=n, freq="h"),
        "amount_usd": np.where(fraud, rng.uniform(0.5, 5, n), rng.lognormal(4.5, 1.0, n)),
        "hour_of_day": rng.integers(0, 24, n),
        "day_of_week": rng.integers(0, 7, n),
        "is_weekend": rng.integers(0, 2, n),
        "account_age_days": rng.integers(1, 1000, n),
        "latency_ms": np.where(fraud, rng.uniform(5, 40, n), rng.uniform(80, 400, n)),
        "account_country": rng.choice(["CR", "US", "MX"], n),
        "device_type": rng.choice(["mobile", "desktop", "tablet"], n),
        "currency": rng.choice(["USD", "EUR", "CRC"], n),
        "is_fraud": fraud,
    })
    return df


def test_feature_contract(train):
    assert train.ALL_FEATURES == train.NUMERIC_FEATURES + train.CATEGORICAL_FEATURES
    assert len(set(train.ALL_FEATURES)) == len(train.ALL_FEATURES)


@pytest.mark.parametrize("forbidden", [
    "is_fraud", "fraud_pattern", "merchant", "merchant_clean",
    "mcc_category", "risk_tier", "email", "holder_name",
])
def test_no_leakage_or_pii_features(train, forbidden):
    """The model must not see the label, merchant names, MCC labels or PII."""
    assert forbidden not in train.ALL_FEATURES


def test_time_split_is_chronological_and_disjoint(train):
    df = synthetic_frame(train, n=100)
    tr, te = train.time_split(df)
    assert len(tr) == 80 and len(te) == 20
    assert tr["transaction_date"].max() <= te["transaction_date"].min()
    assert set(tr.index).isdisjoint(te.index)


def test_time_split_respects_ratio(train):
    df = synthetic_frame(train, n=200)
    tr, te = train.time_split(df, train_ratio=0.5)
    assert len(tr) == len(te) == 100


def test_pipeline_fits_and_scores(train):
    df = synthetic_frame(train)
    y = df["is_fraud"].astype(int)
    pipe = train.build_pipeline(scale_pos_weight=(y == 0).sum() / (y == 1).sum())
    pipe.fit(df[train.ALL_FEATURES], y)

    proba = pipe.predict_proba(df[train.ALL_FEATURES])[:, 1]
    assert proba.shape == (len(df),)
    assert ((proba >= 0) & (proba <= 1)).all()
    # The signal is easy: fraud rows should score clearly higher than legit ones.
    assert proba[y == 1].mean() > proba[y == 0].mean() + 0.3


def test_pipeline_handles_unseen_categories(train):
    """Inference must not crash on a country, device or currency never seen in training."""
    df = synthetic_frame(train)
    y = df["is_fraud"].astype(int)
    pipe = train.build_pipeline(scale_pos_weight=4.0)
    pipe.fit(df[train.ALL_FEATURES], y)

    row = df[train.ALL_FEATURES].head(1).copy()
    row["account_country"] = "ZZ"
    row["device_type"] = "smart_fridge"
    row["currency"] = "XXX"
    proba = pipe.predict_proba(row)[0, 1]
    assert 0 <= proba <= 1
