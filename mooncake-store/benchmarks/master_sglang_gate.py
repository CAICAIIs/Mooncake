import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path

CSV_FIELDS = [
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


class GateError(Exception):
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compare baseline and current SGLang HiCache Master benchmark "
            "CSV runs and exit nonzero when latency regresses."
        )
    )
    parser.add_argument("--baseline-dir", required=True)
    parser.add_argument("--current-dir", required=True)
    parser.add_argument("--baseline-label", default="baseline")
    parser.add_argument("--current-label", default="current")
    parser.add_argument("--baseline-commit", default="")
    parser.add_argument("--current-commit", default="")
    parser.add_argument("--max-p50-regression-pct", type=float, default=5.0)
    parser.add_argument("--max-p99-regression-pct", type=float, default=10.0)
    parser.add_argument(
        "--min-runs",
        type=int,
        default=3,
        help="Minimum CSV files required in each directory (default: 3)",
    )
    parser.add_argument("--output", default="", help="Write the JSON report here")
    args = parser.parse_args()
    if args.max_p50_regression_pct < 0 or args.max_p99_regression_pct < 0:
        parser.error("regression thresholds must be non-negative")
    if args.min_runs < 1:
        parser.error("--min-runs must be positive")
    return args


def csv_files(path_value):
    path = Path(path_value)
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise GateError(f"not a file or directory: {path}")
    files = sorted(path.glob("*.csv"))
    if not files:
        raise GateError(f"no CSV files found in {path}")
    return files


def read_run(path):
    lines = [
        line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]
    if not lines:
        raise GateError(f"empty CSV: {path}")
    reader = csv.DictReader(lines)
    if reader.fieldnames != CSV_FIELDS:
        raise GateError(f"unexpected CSV header in {path}: {reader.fieldnames}")

    groups = {}
    for row in reader:
        try:
            key = (row["phase"], row["rpc"], int(row["batch_keys"]))
            rpcs = int(row["rpcs"])
            items = int(row["items"])
            p50 = float(row["p50_ms"])
            p99 = float(row["p99_ms"])
        except (KeyError, TypeError, ValueError) as exc:
            raise GateError(f"invalid row in {path}: {exc}") from exc
        if key in groups:
            raise GateError(f"duplicate group in {path}: {key}")
        if rpcs <= 0 or items < 0:
            raise GateError(f"invalid counts in {path} for {key}")
        if not math.isfinite(p50) or not math.isfinite(p99):
            raise GateError(f"non-finite latency in {path} for {key}")
        groups[key] = {"rpcs": rpcs, "items": items, "p50_ms": p50, "p99_ms": p99}
    if not groups:
        raise GateError(f"no data rows in {path}")
    return groups


def load_runs(path_value, min_runs, role):
    files = csv_files(path_value)
    if len(files) < min_runs:
        raise GateError(
            f"{role}: found {len(files)} CSV files, need at least {min_runs}"
        )
    runs = [read_run(path) for path in files]
    reference = set(runs[0])
    for path, run in zip(files[1:], runs[1:]):
        if set(run) != reference:
            raise GateError(f"{role}: group set differs in {path}")
    return runs, files


def median(values):
    return statistics.median(values)


def compare_groups(baseline_runs, current_runs, p50_limit, p99_limit):
    keys = sorted(baseline_runs[0])
    groups = []
    failures = []

    for key in keys:
        baseline = [run[key] for run in baseline_runs]
        current = [run[key] for run in current_runs]

        baseline_counts = {(row["rpcs"], row["items"]) for row in baseline}
        current_counts = {(row["rpcs"], row["items"]) for row in current}
        if len(baseline_counts) != 1:
            failures.append(f"{key}: baseline run counts differ")
        if len(current_counts) != 1:
            failures.append(f"{key}: current run counts differ")
        if baseline_counts != current_counts:
            failures.append(f"{key}: RPC/item counts changed")

        base_p50 = median([row["p50_ms"] for row in baseline])
        base_p99 = median([row["p99_ms"] for row in baseline])
        cur_p50 = median([row["p50_ms"] for row in current])
        cur_p99 = median([row["p99_ms"] for row in current])

        if base_p50 <= 0 or base_p99 <= 0:
            p50_change = None
            p99_change = None
            p50_ok = False
            p99_ok = False
            failures.append(f"{key}: baseline latency is not positive")
        else:
            p50_change = (cur_p50 - base_p50) / base_p50 * 100.0
            p99_change = (cur_p99 - base_p99) / base_p99 * 100.0
            p50_ok = p50_change <= p50_limit
            p99_ok = p99_change <= p99_limit
            if not p50_ok:
                failures.append(
                    f"{key}: p50 regressed {p50_change:.3f}% (limit {p50_limit:.3f}%)"
                )
            if not p99_ok:
                failures.append(
                    f"{key}: p99 regressed {p99_change:.3f}% (limit {p99_limit:.3f}%)"
                )

        groups.append(
            {
                "phase": key[0],
                "rpc": key[1],
                "batch_keys": key[2],
                "baseline_rpcs": baseline[0]["rpcs"],
                "baseline_items": baseline[0]["items"],
                "baseline_p50_ms": base_p50,
                "baseline_p99_ms": base_p99,
                "current_p50_ms": cur_p50,
                "current_p99_ms": cur_p99,
                "p50_change_pct": p50_change,
                "p99_change_pct": p99_change,
                "p50_ok": p50_ok,
                "p99_ok": p99_ok,
            }
        )

    return groups, failures


def main():
    args = parse_args()
    try:
        baseline_runs, baseline_files = load_runs(
            args.baseline_dir, args.min_runs, args.baseline_label
        )
        current_runs, current_files = load_runs(
            args.current_dir, args.min_runs, args.current_label
        )
        if set(baseline_runs[0]) != set(current_runs[0]):
            raise GateError("baseline and current group sets differ")

        groups, failures = compare_groups(
            baseline_runs,
            current_runs,
            args.max_p50_regression_pct,
            args.max_p99_regression_pct,
        )
    except GateError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    report = {
        "tool": "master_sglang_gate",
        "passed": not failures,
        "baseline": {
            "label": args.baseline_label,
            "commit": args.baseline_commit,
            "runs": len(baseline_files),
            "files": [path.name for path in baseline_files],
        },
        "current": {
            "label": args.current_label,
            "commit": args.current_commit,
            "runs": len(current_files),
            "files": [path.name for path in current_files],
        },
        "thresholds": {
            "max_p50_regression_pct": args.max_p50_regression_pct,
            "max_p99_regression_pct": args.max_p99_regression_pct,
            "min_runs": args.min_runs,
        },
        "groups": groups,
        "failures": failures,
    }

    text = json.dumps(report, indent=2, sort_keys=True)
    print(text)
    if args.output:
        output = Path(args.output)
        output.write_text(text + "\n", encoding="utf-8")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
