"""Measure FailSlice on the bundled examples and write the README's benchmark table.

    uv run python benchmarks/run.py            # measure, write results.json and the README table
    uv run python benchmarks/run.py --check    # fail if the README table differs from results.json

Per scenario: input rows, output rows, the share of rows kept, predicate evaluations, the
wall time of ``failslice.reduce(..., isolate=True)`` (median of ``--repeat`` runs, data
generation excluded) and the minimality label. The numbers in the README come only from
``results.json``, which this script writes; never edit them by hand. See README.md here
for what is and is not measured.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb

import failslice

ROOT = Path(__file__).resolve().parents[1]
RESULTS = Path(__file__).resolve().parent / "results.json"
README = ROOT / "README.md"
START, END = "<!-- benchmarks:start -->", "<!-- benchmarks:end -->"
WORK = Path(__file__).resolve().parent / ".work"
# (label, example, extra make_data arguments); variants run in a copy under .work/.
SCENARIOS: tuple[tuple[str, str, dict[str, Any]], ...] = (
    ("quickstart", "quickstart", {}),
    ("tpch_fanout", "tpch_fanout", {}),
    ("tpch_fanout (sf 0.1)", "tpch_fanout", {"scale_factor": 0.1}),
    ("null_not_in", "null_not_in", {}),
    ("date_parse_crash", "date_parse_crash", {}),
    ("latest_tie", "latest_tie", {}),
)


def _examples() -> Any:
    """Load examples/run.py (the example driver) under a unique module name."""
    spec = importlib.util.spec_from_file_location(
        "failslice_examples", ROOT / "examples" / "run.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _cpu() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def _memory_gib() -> float | None:
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (AttributeError, ValueError, OSError):
        return None


def _revision() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.stdout.strip() or "unknown"


def measure(repeat: int) -> dict[str, Any]:
    examples = _examples()
    scenarios = []
    for label, name, kwargs in SCENARIOS:
        directory = ROOT / "examples" / name
        if kwargs:  # a variant: its own copy, so the example's own data stays as documented
            work = WORK / label.replace(" ", "_").replace("(", "").replace(")", "")
            shutil.rmtree(work, ignore_errors=True)
            shutil.copytree(directory, work, ignore=shutil.ignore_patterns("data", ".failslice"))
            directory = work
        examples.generate(directory, **kwargs)
        runs = [examples.reduce_example(name, directory) for _ in range(repeat)]
        first = runs[0]
        for run in runs:
            if not run.ok:
                print(run.test_output, file=sys.stderr)
                raise SystemExit(
                    f"{label}: run not successful ({run.status}, replay {run.verification}, "
                    f"generated test {'passed' if run.test_passed else 'failed'}); "
                    "not recording results"
                )
            if (run.status, run.minimality, run.sizes_after) != (
                first.status,
                first.minimality,
                first.sizes_after,
            ):
                raise SystemExit(f"{name}: repeated runs disagree; not recording results")
        scenarios.append(
            {
                "name": label,
                "example": name,
                "rows_before": first.rows_before,
                "rows_after": first.rows_after,
                "kept": first.rows_after / first.rows_before,
                "evaluations": first.evaluations,
                "wall_s": statistics.median(r.wall_s for r in runs),
                "wall_s_runs": [r.wall_s for r in runs],
                "minimality": first.minimality,
                "verification": first.verification,
                "test_passed": all(r.test_passed for r in runs),
            }
        )
        print(
            f"{label}: {first.rows_before:,} -> {first.rows_after:,}, "
            f"{first.evaluations} evaluations, "
            f"median {scenarios[-1]['wall_s']:.2f} s"
        )
    return {
        "format": 1,
        "measured_at": dt.datetime.now(dt.UTC).strftime("%Y-%m-%d"),
        "repeat": repeat,
        "environment": {
            "failslice": failslice.__version__,
            "revision": _revision(),
            "duckdb": duckdb.__version__,
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}",
            "cpu": _cpu(),
            "cpus": os.cpu_count(),
            "memory_gib": _memory_gib(),
        },
        "scenarios": scenarios,
    }


# Absolute links: the README is also the PyPI project description.
BLOB = "https://github.com/sdmhdn/failslice/blob/main"


def render(results: dict[str, Any]) -> str:
    env = results["environment"]
    memory = f", {env['memory_gib']} GiB RAM" if env.get("memory_gib") else ""
    lines = [
        START,
        "<!-- Generated by `uv run python benchmarks/run.py` from benchmarks/results.json; "
        "do not edit by hand. -->",
        "",
        "| scenario | input rows | output rows | kept | evaluations | wall time | minimality |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for s in results["scenarios"]:
        lines.append(
            f"| [{s['name']}]({BLOB}/examples/{s['example']}/) "
            f"| {s['rows_before']:,} | {s['rows_after']:,} "
            f"| {s['kept']:.4%} | {s['evaluations']:,} | {s['wall_s']:.1f} s | {s['minimality']} |"
        )
    lines += [
        "",
        f"Measured {results['measured_at']} on one machine ({env['cpu']}, {env['cpus']} logical "
        f"CPUs{memory}; {env['os']}; Python {env['python']}, DuckDB {env['duckdb']}, FailSlice "
        f"{env['failslice']} at {env['revision']}). Wall time is the median of {results['repeat']} "
        "runs of `reduce(isolate=True)` including the replay and artifacts, without data "
        "generation; it depends on the machine. "
        f"[Method]({BLOB}/benchmarks/README.md).",
        END,
    ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--check", action="store_true", help="compare README with results.json")
    args = parser.parse_args(argv)
    text = README.read_text(encoding="utf-8")
    start, end = text.index(START), text.index(END) + len(END)
    if args.check:
        table = render(json.loads(RESULTS.read_text(encoding="utf-8")))
        if text[start:end] != table:
            print("README benchmark table is stale: run benchmarks/run.py", file=sys.stderr)
            return 1
        return 0
    results = measure(args.repeat)
    RESULTS.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    README.write_text(text[:start] + render(results) + text[end:], encoding="utf-8")
    return 0


if __name__ == "__main__":  # required: reduce(isolate=True) starts a child process
    sys.exit(main())
