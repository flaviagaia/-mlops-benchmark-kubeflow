from __future__ import annotations

from pathlib import Path

from src.components import (
    evaluate_component,
    ingest_component,
    prepare_component,
    register_component,
    train_component,
    validate_component,
)


def run_local_pipeline(base_dir: str | Path) -> dict:
    dataset_path = ingest_component(base_dir)
    validation = validate_component(dataset_path)
    prepared = prepare_component(dataset_path, base_dir)
    trained = train_component(prepared["train_path"])
    evaluation = evaluate_component(prepared["test_path"], trained, base_dir)
    return register_component(validation, evaluation, base_dir)
