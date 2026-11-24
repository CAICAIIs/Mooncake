import csv
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

GATE = Path(__file__).with_name("master_sglang_gate.py")
HEADER = [
    "phase",
    "rpc",
    "batch_keys",
    "rpcs",
    "items",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
]


def write_run(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(HEADER)
        writer.writerows(rows)


def rows(p99=2.0):
    return [
        ["get", "BatchGetReplicaList", 64, 100, 6400, 0.5, 1.0, p99, 3.0],
        ["put_start", "BatchPutStart", 64, 100, 100, 2.0, 4.0, p99 * 2, 6.0],
    ]


class GateTest(unittest.TestCase):
    def run_gate(self, baseline, current, *extra):
        command = [
            sys.executable,
            str(GATE),
            "--baseline-dir",
            str(baseline),
            "--current-dir",
            str(current),
            "--min-runs",
            "3",
            *extra,
        ]
        return subprocess.run(command, capture_output=True, text=True, check=False)

    def make_dirs(self, root):
        baseline = root / "baseline"
        current = root / "current"
        baseline.mkdir()
        current.mkdir()
        return baseline, current

    def test_equal_runs_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline, current = self.make_dirs(Path(tmp))
            for name in ("a", "b", "c"):
                write_run(baseline / f"{name}.csv", rows())
                write_run(current / f"{name}.csv", rows())
            result = self.run_gate(baseline, current)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertTrue(report["passed"])

    def test_p99_regression_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline, current = self.make_dirs(Path(tmp))
            for name in ("a", "b", "c"):
                write_run(baseline / f"{name}.csv", rows())
                write_run(current / f"{name}.csv", rows(p99=3.0))
            result = self.run_gate(baseline, current)
            self.assertEqual(result.returncode, 1)
            report = json.loads(result.stdout)
            self.assertFalse(report["passed"])
            self.assertTrue(
                any("p99 regressed" in failure for failure in report["failures"])
            )

    def test_missing_group_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline, current = self.make_dirs(Path(tmp))
            for name in ("a", "b", "c"):
                write_run(baseline / f"{name}.csv", rows())
                write_run(current / f"{name}.csv", rows()[:1])
            result = self.run_gate(baseline, current)
            self.assertEqual(result.returncode, 2)

    def test_changed_rpc_count_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline, current = self.make_dirs(Path(tmp))
            for name in ("a", "b", "c"):
                write_run(baseline / f"{name}.csv", rows())
                changed = rows()
                changed[0][3] = 99
                write_run(current / f"{name}.csv", changed)
            result = self.run_gate(baseline, current)
            self.assertEqual(result.returncode, 1)
            report = json.loads(result.stdout)
            self.assertTrue(
                any("counts changed" in failure for failure in report["failures"])
            )


if __name__ == "__main__":
    unittest.main()
