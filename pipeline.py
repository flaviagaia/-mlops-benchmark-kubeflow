from __future__ import annotations

from pathlib import Path


KUBEFLOW_PIPELINE_SPEC = {
    "pipeline_name": "mlops-benchmark-kubeflow",
    "description": "Kubeflow-style MLOps benchmark with ingestion, validation, preparation, multi-model training, evaluation, and registration.",
    "components": [
        {"name": "ingest_component", "outputs": ["dataset_path"]},
        {"name": "validate_component", "inputs": ["dataset_path"], "outputs": ["validation_metrics"]},
        {"name": "prepare_component", "inputs": ["dataset_path"], "outputs": ["train_path", "test_path"]},
        {"name": "train_component", "inputs": ["train_path"], "outputs": ["trained_models"]},
        {"name": "evaluate_component", "inputs": ["test_path", "trained_models"], "outputs": ["leaderboard"]},
        {"name": "register_component", "inputs": ["validation_metrics", "leaderboard"], "outputs": ["report_artifact"]},
    ],
}


def write_pipeline_spec(base_dir: str | Path) -> Path:
    import json

    artifacts_dir = Path(base_dir) / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    spec_path = artifacts_dir / "kubeflow_pipeline_spec.json"
    spec_path.write_text(json.dumps(KUBEFLOW_PIPELINE_SPEC, ensure_ascii=False, indent=2), encoding="utf-8")
    return spec_path
