"""
Train a fraud detection model on gold_fraud_features.

Features (all behavioral, no PII, no merchant name):
  amount_usd, hour_of_day, day_of_week, is_weekend,
  account_age_days, latency_ms, account_country, device_type, currency

Split: time-based (oldest 80% = train, newest 20% = test) to prevent leakage.
Model: XGBoost inside a sklearn Pipeline (OrdinalEncoder for categoricals).
Tracking: MLflow (local ./mlruns). Run `mlflow ui` to explore.
Output: ml/model/fraud_pipeline.joblib
"""
import os
import sys
from pathlib import Path

import duckdb
import joblib
import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from xgboost import XGBClassifier

ROOT = Path(__file__).resolve().parent.parent
DBT_DUCKDB = ROOT / "transform" / "payflow" / "dev.duckdb"
MODEL_DIR = ROOT / "ml" / "model"
MODEL_PATH = MODEL_DIR / "fraud_pipeline.joblib"

NUMERIC_FEATURES = [
    "amount_usd",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "account_age_days",
    "latency_ms",
]
CATEGORICAL_FEATURES = ["account_country", "device_type", "currency"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = "is_fraud"


def load_data():
    con = duckdb.connect(str(DBT_DUCKDB), read_only=True)
    df = con.execute(f"""
        SELECT
            transaction_date,
            {', '.join(ALL_FEATURES)},
            {TARGET}
        FROM main.gold_fraud_features
        WHERE amount_usd IS NOT NULL
          AND latency_ms IS NOT NULL
          AND account_age_days IS NOT NULL
        ORDER BY transaction_date
    """).df()
    con.close()
    return df


def time_split(df, train_ratio=0.8):
    split = int(len(df) * train_ratio)
    return df.iloc[:split].copy(), df.iloc[split:].copy()


def build_pipeline(scale_pos_weight: float) -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CATEGORICAL_FEATURES),
        ],
        remainder="passthrough",
    )
    clf = XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="auc",
        random_state=42,
        n_jobs=-1,
    )
    return Pipeline([("prep", preprocessor), ("clf", clf)])


def main():
    print("Loading data from DuckDB...")
    df = load_data()
    print(f"  {len(df):,} rows loaded. Fraud rate: {df[TARGET].mean():.2%}")

    train, test = time_split(df)
    print(f"  Train: {len(train):,} rows | Test: {len(test):,} rows")

    X_train = train[ALL_FEATURES]
    y_train = train[TARGET].astype(int)
    X_test = test[ALL_FEATURES]
    y_test = test[TARGET].astype(int)

    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    scale_pos_weight = neg / pos
    print(f"  Class balance: neg={neg:,} pos={pos:,} (scale_pos_weight: {scale_pos_weight:.1f})")

    mlflow.set_tracking_uri(f"sqlite:///{ROOT / 'ml' / 'mlruns.db'}")
    mlflow.set_experiment("payflow-fraud-detection")

    with mlflow.start_run():
        mlflow.log_param("model", "xgboost")
        mlflow.log_param("features", ALL_FEATURES)
        mlflow.log_param("train_rows", len(train))
        mlflow.log_param("test_rows", len(test))
        mlflow.log_param("scale_pos_weight", round(scale_pos_weight, 2))
        mlflow.log_param("split", "time-based 80/20")

        print("\nTraining XGBoost pipeline...")
        pipeline = build_pipeline(scale_pos_weight)
        pipeline.fit(X_train, y_train)

        y_pred = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)[:, 1]

        acc = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_proba)

        mlflow.log_metric("accuracy", round(acc, 4))
        mlflow.log_metric("roc_auc", round(auc, 4))

        report = classification_report(y_test, y_pred, target_names=["legit", "fraud"], output_dict=True)
        mlflow.log_metric("fraud_precision", round(report["fraud"]["precision"], 4))
        mlflow.log_metric("fraud_recall",    round(report["fraud"]["recall"], 4))
        mlflow.log_metric("fraud_f1",        round(report["fraud"]["f1-score"], 4))

        # Feature importance (XGBoost native, after preprocessing, features are reordered)
        booster = pipeline.named_steps["clf"]
        feat_names = CATEGORICAL_FEATURES + NUMERIC_FEATURES  # ColumnTransformer puts cats first
        importance = dict(zip(feat_names, booster.feature_importances_))
        for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
            mlflow.log_metric(f"importance_{feat}", round(float(imp), 4))

        mlflow.sklearn.log_model(
            pipeline,
            "fraud_pipeline",
            skops_trusted_types=["xgboost.core.Booster", "xgboost.sklearn.XGBClassifier"],
        )

        print(f"\n  Accuracy : {acc:.4f}")
        print(f"  ROC-AUC  : {auc:.4f}")
        print(f"  Fraud P/R/F1: {report['fraud']['precision']:.3f} / {report['fraud']['recall']:.3f} / {report['fraud']['f1-score']:.3f}")
        print("\n  Feature importances:")
        for feat, imp in sorted(importance.items(), key=lambda x: -x[1]):
            bar = "=" * int(imp * 40)
            print(f"    {feat:<20} {imp:.4f}  {bar}")

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        joblib.dump(pipeline, MODEL_PATH)
        print(f"\nModel saved to {MODEL_PATH}")
        print("Run `mlflow ui --backend-store-uri sqlite:///ml/mlruns.db` to explore metrics.")


if __name__ == "__main__":
    main()
