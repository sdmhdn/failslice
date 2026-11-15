"""Independent replay of an exported reproducer (``docs/design.md`` §11).

Builds a fresh DuckDB database only from the files in a run directory
(``spec.json``, ``tables/*.parquet``, ``orphans.parquet``, ``expected/``), disables
external access, evaluates the predicate (uncached) and checks referential integrity.
:func:`verify_in_subprocess` runs this in a separate Python process.

Run as ``python -m failslice.verify <run_dir>``; prints a JSON report.
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import duckdb
import yaml
from pydantic import TypeAdapter

from failslice.config import ExpectPredicate, ForeignKey, PredicateSpec, SqlPredicate
from failslice.predicate import PredicateRunner, Row, normalize_expected
from failslice.safety import validate_predicate_sql

__all__ = ["VerificationReport", "verify_in_subprocess", "verify_run_dir"]

Status = Literal["verified", "replay-failed", "integrity-failed", "unverified"]


@dataclass
class VerificationReport:
    """Result of an independent replay."""

    status: Status
    outcome: str = ""
    signature: str = ""
    detail: str = ""
    row_counts: dict[str, int] = field(default_factory=dict)
    integrity_violations: dict[str, int] = field(default_factory=dict)
    process: Literal["subprocess", "in-process"] = "in-process"

    @property
    def ok(self) -> bool:
        """Whether the reproducer was verified."""
        return self.status == "verified"

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(asdict(self), sort_keys=True)


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def verify_run_dir(run_dir: str | Path) -> VerificationReport:
    """Replay the reproducer in ``run_dir`` in a fresh in-memory DuckDB database."""
    root = Path(run_dir).resolve()
    spec: dict[str, Any] = json.loads((root / "spec.json").read_text())
    predicate: PredicateSpec = TypeAdapter(PredicateSpec).validate_python(spec["predicate"])
    query = predicate.sql if isinstance(predicate, SqlPredicate) else predicate.query
    validate_predicate_sql(query, spec["tables"].keys())
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET enable_progress_bar = false")
        counts: dict[str, int] = {}
        for name, table in spec["tables"].items():
            path = (root / table["file"]).resolve()
            if not path.is_relative_to(root):
                raise ValueError(f"table file outside the run directory: {table['file']}")
            con.execute(f"CREATE TABLE {_q(name)} AS SELECT * FROM read_parquet({_lit(str(path))})")
            got = [(str(r[0]), str(r[1])) for r in con.execute(f"DESCRIBE {_q(name)}").fetchall()]
            want = [tuple(c) for c in table["columns"]]
            if got != want:
                return VerificationReport(
                    "replay-failed", detail=f"schema of {name!r} differs: {got} != {want}"
                )
            row = con.execute(f"SELECT count(*) FROM {_q(name)}").fetchone()
            counts[name] = int(row[0]) if row else 0
        exempt = con.execute(
            f"SELECT edge, row_index FROM read_parquet({_lit(str(root / 'orphans.parquet'))})"
        ).fetchall()
        expected: list[Row] | None = None
        if isinstance(predicate, ExpectPredicate):
            if spec.get("expected_file"):
                exp_path = (root / spec["expected_file"]).resolve()
                if not exp_path.is_relative_to(root):
                    raise ValueError(
                        f"expectation file outside the run directory: {spec['expected_file']}"
                    )
                reader = (
                    "read_parquet"
                    if exp_path.suffix.lower() in {".parquet", ".pq"}
                    else ("read_csv")
                )
                expected = [
                    tuple(r)
                    for r in con.execute(
                        f"SELECT * FROM {reader}({_lit(str(exp_path))})"
                    ).fetchall()
                ]
            else:
                expected = normalize_expected(yaml.safe_load(spec["expected_yaml"]))
        con.execute("CREATE TABLE __fs_exempt(edge INTEGER, row_index BIGINT)")
        if exempt:
            con.executemany("INSERT INTO __fs_exempt VALUES (?, ?)", [list(e) for e in exempt])
        con.execute("SET enable_external_access = false")
        con.execute(f"SET threads = {int(spec['settings']['threads'])}")

        violations: dict[str, int] = {}
        for i, text in enumerate(spec["foreign_keys"]):
            fk = ForeignKey.parse(text)
            # Project only the FK column into a scratch table: its row-id pseudo column
            # is then the output row index, even if the user's table has a "rowid" column.
            con.execute(
                f"CREATE TABLE __fs_ix AS SELECT {_q(fk.child_column)} AS v FROM {_q(fk.child)}"
            )
            row = con.execute(
                f"SELECT count(*) FROM (SELECT rowid AS ri, v FROM __fs_ix) c "
                f"WHERE c.v IS NOT NULL AND NOT EXISTS "
                f"(SELECT 1 FROM {_q(fk.parent)} p WHERE p.{_q(fk.parent_column)} = c.v) "
                f"AND c.ri NOT IN (SELECT row_index FROM __fs_exempt WHERE edge = {i})"
            ).fetchone()
            con.execute("DROP TABLE __fs_ix")
            n = int(row[0]) if row else 0
            if n:
                violations[text] = n
        con.execute("DROP TABLE __fs_exempt")
        runner = PredicateRunner(predicate, expected)
        outcome = runner.run(con, float(spec["settings"]["eval_timeout_s"]))
        if violations:
            return VerificationReport(
                "integrity-failed",
                outcome.kind.value,
                outcome.signature,
                "new dangling references in the reduced data",
                counts,
                violations,
            )
        status: Status = "verified" if outcome.is_true else "replay-failed"
        return VerificationReport(
            status, outcome.kind.value, outcome.signature, outcome.detail, counts, {}
        )
    finally:
        con.close()


def verify_in_subprocess(run_dir: str | Path, *, timeout_s: float) -> VerificationReport:
    """Run :func:`verify_run_dir` in a fresh Python process with a hard timeout."""
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "failslice.verify", str(run_dir)],
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return VerificationReport(
            "unverified",
            detail=f"replay did not finish within {timeout_s:g}s",
            process="subprocess",
        )
    try:
        data = json.loads(proc.stdout.strip().splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return VerificationReport(
            "unverified",
            detail=f"replay process failed (exit {proc.returncode}): {proc.stderr.strip()[-2000:]}",
            process="subprocess",
        )
    data["process"] = "subprocess"
    return VerificationReport(**data)


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m failslice.verify <run_dir>``."""
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: python -m failslice.verify <run_dir>", file=sys.stderr)
        return 2
    report = verify_run_dir(args[0])
    print(report.to_json())
    return 0 if report.ok else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
