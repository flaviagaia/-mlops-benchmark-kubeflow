from __future__ import annotations

from pathlib import Path
import unittest

from src.pipeline_runner import run_local_pipeline


class MlopsBenchmarkKubeflowTestCase(unittest.TestCase):
    def test_pipeline_contract(self) -> None:
        result = run_local_pipeline(Path(__file__).resolve().parents[1])
        self.assertIn("selected_model", result)
        self.assertIn("candidate_results", result)
        self.assertIn("validation", result)
        self.assertGreater(result["validation"]["row_count"], 1000)
        self.assertGreater(result["candidate_results"][result["selected_model"]]["average_precision"], 0.4)


if __name__ == "__main__":
    unittest.main()
