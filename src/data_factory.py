from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd


def build_dataset(seed: int = 42, rows: int = 1400) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    tenure = rng.integers(1, 84, rows)
    monthly_spend = rng.normal(180, 55, rows).clip(20, 450)
    support_calls = rng.poisson(2.1, rows)
    contract_type = rng.choice(["monthly", "annual", "biennial"], rows, p=[0.58, 0.28, 0.14])
    auto_pay = rng.choice([0, 1], rows, p=[0.45, 0.55])
    digital_engagement = rng.normal(5.4, 1.9, rows).clip(0.5, 10)
    outage_exposure = rng.poisson(0.7, rows)

    risk_logit = (
        1.6 * (contract_type == "monthly").astype(float)
        + 0.22 * support_calls
        + 0.5 * (1 - auto_pay)
        + 0.018 * monthly_spend
        + 0.14 * outage_exposure
        - 0.03 * tenure
        - 0.18 * digital_engagement
    )
    probability = 1 / (1 + np.exp(-(risk_logit - 2.75)))
    target = rng.binomial(1, probability)

    return pd.DataFrame(
        {
            "customer_id": [f"BM-{1000 + idx}" for idx in range(rows)],
            "tenure_months": tenure,
            "monthly_spend": monthly_spend.round(2),
            "support_calls": support_calls,
            "contract_type": contract_type,
            "auto_pay": auto_pay,
            "digital_engagement": digital_engagement.round(2),
            "outage_exposure": outage_exposure,
            "target": target,
        }
    )


def ensure_raw_dataset(base_dir: str | Path) -> Path:
    base_path = Path(base_dir)
    raw_dir = base_path / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    csv_path = raw_dir / "benchmark_dataset.csv"
    dataframe = build_dataset()
    with NamedTemporaryFile("w", suffix=".csv", delete=False, dir=raw_dir, encoding="utf-8") as tmp_file:
        temp_path = Path(tmp_file.name)
    try:
        dataframe.to_csv(temp_path, index=False)
        temp_path.replace(csv_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()
    return csv_path
