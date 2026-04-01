from __future__ import annotations

import json
from pathlib import Path

from pipeline import write_pipeline_spec
from src.pipeline_runner import run_local_pipeline


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    summary = run_local_pipeline(base_dir)
    summary["pipeline_spec_artifact"] = str(write_pipeline_spec(base_dir))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
