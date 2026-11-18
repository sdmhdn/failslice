"""Run a FailSlice example end to end: generate data, check, reduce, run the generated test.

    uv run python examples/run.py tpch_fanout          # one example
    uv run python examples/run.py all                  # every example
    uv run python examples/run.py all --update-readme  # rewrite the generated README sections
    uv run python examples/run.py all --check-readme   # fail if a README section is stale

Each example's README has a section between ``<!-- generated:start -->`` and
``<!-- generated:end -->`` that this script writes from a real run: row counts, the
minimality label, the replay status and the surviving rows. Wall time and evaluation
counts are not in it (they vary by machine); see ``benchmarks/``.
"""

from __future__ import annotations

import argparse
import runpy
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import duckdb

import failslice

EXAMPLES = Path(__file__).resolve().parent
NAMES = ("tpch_fanout", "null_not_in", "date_parse_crash", "latest_tie")
START, END = "<!-- generated:start -->", "<!-- generated:end -->"
CELL_WIDTH = 40


@dataclass
class ExampleRun:
    """What one end-to-end run of an example observed."""

    name: str
    status: str
    minimality: str
    verification: str
    sizes_before: dict[str, int]
    sizes_after: dict[str, int]
    evaluations: int
    wall_s: float
    test_passed: bool
    run_dir: str
    witness: dict[str, dict[str, Any]] = field(default_factory=dict)
    """Per table: ``columns`` and ``rows`` (as text) of the reduced table."""

    @property
    def rows_before(self) -> int:
        return sum(self.sizes_before.values())

    @property
    def rows_after(self) -> int:
        return sum(self.sizes_after.values())

    def to_json(self) -> dict[str, Any]:
        return {**asdict(self), "rows_before": self.rows_before, "rows_after": self.rows_after}


def generate(directory: Path, **kwargs: Any) -> None:
    """Run the example's ``make_data.py`` (its ``main()``, with optional arguments)."""
    namespace = runpy.run_path(str(directory / "make_data.py"), run_name="make_data")
    namespace["main"](directory / "data" / "generated", **kwargs)


def reduce_example(name: str, directory: Path) -> ExampleRun:
    """Check and reduce an example whose data exists, then run the generated test."""
    project = failslice.load_project(directory / "failslice.yaml")
    report = failslice.check(project)
    if not report.ok:
        raise SystemExit(f"{name}: check failed: {report}")
    started = time.perf_counter()
    result = failslice.reduce(project, isolate=True)
    wall_s = time.perf_counter() - started
    assert result.stats is not None, f"{name}: no statistics ({result.status})"
    test = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            str(result.run_dir / "pytest"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return ExampleRun(
        name=name,
        status=result.status,
        minimality=result.minimality_label,
        verification=result.verification.status if result.verification else "none",
        sizes_before=dict(result.stats.sizes_before),
        sizes_after=dict(result.stats.sizes_after),
        evaluations=result.stats.evaluations,
        wall_s=round(wall_s, 3),
        test_passed=test.returncode == 0,
        run_dir=str(result.run_dir),
        witness=_witness(result.run_dir, list(result.stats.sizes_after)),
    )


def run_example(name: str, directory: Path | None = None) -> ExampleRun:
    """Generate the data, then check, reduce and test (``directory`` defaults to the example)."""
    directory = directory or EXAMPLES / name
    generate(directory)
    return reduce_example(name, directory)


def _cell(value: Any) -> str:
    text = "NULL" if value is None else str(value)
    text = text.replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= CELL_WIDTH else text[: CELL_WIDTH - 1] + "…"


def _witness(run_dir: Path, tables: list[str]) -> dict[str, dict[str, Any]]:
    con = duckdb.connect()
    try:
        out: dict[str, dict[str, Any]] = {}
        for table in tables:
            path = str(run_dir / "tables" / f"{table}.parquet").replace("'", "''")
            rel = con.execute(f"SELECT * FROM read_parquet('{path}') ORDER BY ALL")
            columns = [d[0] for d in rel.description]
            out[table] = {
                "columns": columns,
                "rows": [[_cell(v) for v in row] for row in rel.fetchall()],
            }
        return out
    finally:
        con.close()


def render(run: ExampleRun) -> str:
    """The README's generated section for a run (deterministic parts only)."""
    lines = [
        START,
        "<!-- Written by `uv run python examples/run.py "
        f"{run.name} --update-readme`; do not edit by hand. -->",
        "",
        f"Result: **{run.minimality}**, replay **{run.verification}**, "
        f"{run.rows_before:,} -> {run.rows_after:,} rows; the generated pytest test "
        + ("passes (the bug is reproduced)." if run.test_passed else "FAILED."),
        "",
        "| table | rows before | rows after |",
        "|---|---:|---:|",
    ]
    lines += [
        f"| {t} | {run.sizes_before[t]:,} | {run.sizes_after.get(t, 0):,} |"
        for t in run.sizes_before
    ]
    lines += ["", "Surviving rows:"]
    for table, data in run.witness.items():
        if not data["rows"]:
            continue
        lines += ["", f"`{table}`", "", "| " + " | ".join(data["columns"]) + " |"]
        lines.append("|" + "---|" * len(data["columns"]))
        lines += ["| " + " | ".join(row) + " |" for row in data["rows"]]
    lines.append(END)
    return "\n".join(lines)


def readme_section(readme: Path) -> str:
    text = readme.read_text(encoding="utf-8")
    start, end = text.index(START), text.index(END) + len(END)
    return text[start:end]


def update_readme(readme: Path, section: str) -> None:
    text = readme.read_text(encoding="utf-8")
    start, end = text.index(START), text.index(END) + len(END)
    readme.write_text(text[:start] + section + text[end:], encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("name", choices=[*NAMES, "all"])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--update-readme", action="store_true")
    mode.add_argument("--check-readme", action="store_true")
    args = parser.parse_args(argv)
    failed = False
    for name in NAMES if args.name == "all" else (args.name,):
        run = run_example(name)
        ok = run.status == "reduced" and run.verification == "verified" and run.test_passed
        print(
            f"{name}: {run.minimality}: {run.rows_before:,} -> {run.rows_after:,} rows; "
            f"replay {run.verification}; "
            f"generated test {'passed' if run.test_passed else 'FAILED'}; "
            f"{run.evaluations} evaluations, {run.wall_s:.1f} s\n  run directory: {run.run_dir}"
        )
        failed |= not ok
        readme = EXAMPLES / name / "README.md"
        if args.update_readme:
            update_readme(readme, render(run))
        elif args.check_readme and readme_section(readme) != render(run):
            print(f"  {readme} is stale: run with --update-readme", file=sys.stderr)
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":  # required: reduce(isolate=True) starts a child process
    sys.exit(main())
