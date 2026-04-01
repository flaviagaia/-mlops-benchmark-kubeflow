from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import LinearSVC

from src.data_factory import ensure_raw_dataset


os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")

NUMERIC_COLUMNS = [
    "tenure_months",
    "monthly_spend",
    "support_calls",
    "auto_pay",
    "digital_engagement",
    "outage_exposure",
]
CATEGORICAL_COLUMNS = ["contract_type"]
TARGET_COLUMN = "target"


def ingest_component(base_dir: str | Path) -> Path:
    return ensure_raw_dataset(base_dir)


def validate_component(dataset_path: str | Path) -> dict:
    dataframe = pd.read_csv(dataset_path)
    return {
        "row_count": int(len(dataframe)),
        "null_count": int(dataframe.isna().sum().sum()),
        "positive_rate": round(float(dataframe[TARGET_COLUMN].mean()), 4),
        "feature_count": int(len(dataframe.columns) - 2),
    }


def prepare_component(dataset_path: str | Path, base_dir: str | Path) -> dict:
    dataframe = pd.read_csv(dataset_path)
    train_df, test_df = train_test_split(
        dataframe,
        test_size=0.25,
        stratify=dataframe[TARGET_COLUMN],
        random_state=42,
    )
    processed_dir = Path(base_dir) / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    train_path = processed_dir / "train.csv"
    test_path = processed_dir / "test.csv"
    train_df.to_csv(train_path, index=False)
    test_df.to_csv(test_path, index=False)
    return {"train_path": str(train_path), "test_path": str(test_path)}


def _build_preprocessor() -> ColumnTransformer:
    numeric = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("encoder", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    return ColumnTransformer(
        [
            ("numeric", numeric, NUMERIC_COLUMNS),
            ("categorical", categorical, CATEGORICAL_COLUMNS),
        ]
    )


def train_component(train_path: str | Path) -> dict[str, Pipeline]:
    train_df = pd.read_csv(train_path)
    X_train = train_df[NUMERIC_COLUMNS + CATEGORICAL_COLUMNS]
    y_train = train_df[TARGET_COLUMN]

    candidates = {
        "logistic_regression": Pipeline(
            [
                ("preprocessor", _build_preprocessor()),
                ("model", LogisticRegression(max_iter=1500, random_state=42)),
            ]
        ),
        "random_forest": Pipeline(
            [
                ("preprocessor", _build_preprocessor()),
                ("model", RandomForestClassifier(n_estimators=220, random_state=42)),
            ]
        ),
        "linear_svc": Pipeline(
            [
                ("preprocessor", _build_preprocessor()),
                ("model", LinearSVC()),
            ]
        ),
    }

    trained = {}
    for name, pipeline in candidates.items():
        pipeline.fit(X_train, y_train)
        trained[name] = pipeline
    return trained


def evaluate_component(test_path: str | Path, trained_models: dict[str, Pipeline], base_dir: str | Path) -> dict:
    test_df = pd.read_csv(test_path)
    X_test = test_df[NUMERIC_COLUMNS + CATEGORICAL_COLUMNS]
    y_test = test_df[TARGET_COLUMN]

    evaluations = {}
    best_name = ""
    best_score = -1.0
    best_model = None

    for name, pipeline in trained_models.items():
        if hasattr(pipeline, "predict_proba"):
            scores = pipeline.predict_proba(X_test)[:, 1]
        else:
            decision = pipeline.decision_function(X_test)
            scores = (decision - decision.min()) / (decision.max() - decision.min() + 1e-9)
        predictions = (scores >= 0.5).astype(int)
        roc_auc = roc_auc_score(y_test, scores)
        average_precision = average_precision_score(y_test, scores)
        f1 = f1_score(y_test, predictions)
        evaluations[name] = {
            "roc_auc": round(float(roc_auc), 4),
            "average_precision": round(float(average_precision), 4),
            "f1": round(float(f1), 4),
        }
        if average_precision > best_score:
            best_score = average_precision
            best_name = name
            best_model = pipeline

    assert best_model is not None

    artifacts_dir = Path(base_dir) / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifacts_dir / "best_model.joblib"
    metrics_path = artifacts_dir / "benchmark_metrics.json"
    leaderboard_path = artifacts_dir / "leaderboard.json"

    joblib.dump(best_model, model_path)
    leaderboard = {
        "selected_model": best_name,
        "candidate_results": evaluations,
        "model_artifact": str(model_path),
        "metrics_artifact": str(metrics_path),
    }
    metrics_path.write_text(json.dumps(leaderboard, ensure_ascii=False, indent=2), encoding="utf-8")
    leaderboard_path.write_text(json.dumps(leaderboard, ensure_ascii=False, indent=2), encoding="utf-8")
    return {**leaderboard, "leaderboard_artifact": str(leaderboard_path)}


def register_component(validation: dict, evaluation: dict, base_dir: str | Path) -> dict:
    processed_dir = Path(base_dir) / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    report_path = processed_dir / "mlops_benchmark_report.json"
    summary = {
        "runtime_mode": "local_kubeflow_style_benchmark",
        "validation": validation,
        **evaluation,
        "report_artifact": str(report_path),
    }
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary
