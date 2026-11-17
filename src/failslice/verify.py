"""Independent replay of an exported reproducer (``docs/design.md`` §11).

Builds a fresh DuckDB database only from the files a run directory's ``spec.json``
(:class:`~failslice.runspec.RunSpec`) names, disables external access, restores source
column types, checks referential integrity and evaluates the predicate (uncached).
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

from failslice.config import ExpectPredicate, ForeignKey, SqlPredicate
from failslice.predicate import PredicateRunner, Row, normalize_expected
from failslice.runspec import RunSpec, RunSpecError, restoring_select
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


def _describe(con: duckdb.DuckDBPyConnection, relation: str) -> list[tuple[str, str]]:
    return [(str(r[0]), str(r[1])) for r in con.execute(f"DESCRIBE {relation}").fetchall()]


def verify_run_dir(run_dir: str | Path) -> VerificationReport:
    """Replay the reproducer in ``run_dir`` in a fresh in-memory DuckDB database.

    Only files named by ``spec.json`` and inside ``run_dir`` are read; external access
    is disabled before any column is cast back to its source type and before the
    predicate runs.
    """
    try:
        return _replay(Path(run_dir).resolve())
    except RunSpecError as exc:
        return VerificationReport("replay-failed", detail=str(exc))


def _replay(root: Path) -> VerificationReport:
    spec = RunSpec.read(root)
    predicate = spec.predicate
    query = predicate.sql if isinstance(predicate, SqlPredicate) else predicate.query
    validate_predicate_sql(query, spec.tables.keys())
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET enable_progress_bar = false")
        con.execute("CREATE SCHEMA __fs_raw")
        for name, table in spec.tables.items():
            path = spec.resolve(root, table.file)
            con.execute(
                f"CREATE TABLE __fs_raw.{_q(name)} AS SELECT * FROM read_parquet({_lit(str(path))})"
            )
        exempt = con.execute(
            "SELECT edge, row_index FROM read_parquet("
            f"{_lit(str(spec.resolve(root, spec.orphans_file)))})"
        ).fetchall()
        expected: list[Row] | None = None
        if isinstance(predicate, ExpectPredicate):
            if spec.expected_file:
                exp_path = spec.resolve(root, spec.expected_file)
                expected = [
                    tuple(r)
                    for r in con.execute(
                        f"SELECT * FROM read_parquet({_lit(str(exp_path))})"
                    ).fetchall()
                ]
            else:
                expected = normalize_expected(yaml.safe_load(spec.expected_yaml or "null"))
        con.execute("SET enable_external_access = false")
        con.execute(f"SET threads = {int(spec.settings.threads)}")

        counts: dict[str, int] = {}
        for name, table in spec.tables.items():
            raw = f"__fs_raw.{_q(name)}"
            got = _describe(con, raw)
            if got != [tuple(c) for c in table.columns]:
                return VerificationReport(
                    "replay-failed",
                    detail=f"schema of {name!r} differs: {got} != {list(table.columns)}",
                )
            try:
                con.execute(
                    f"CREATE TABLE {_q(name)} AS "
                    + restoring_select(raw, table.columns, table.source_columns)
                )
            except duckdb.Error as exc:
                return VerificationReport(
                    "replay-failed",
                    detail=f"cannot restore the source types of {name!r}: {exc}",
                )
            con.execute(f"DROP TABLE {raw}")
            restored = _describe(con, _q(name))
            if restored != [tuple(c) for c in table.source_columns]:
                return VerificationReport(
                    "replay-failed",
                    detail=f"cannot restore the source types of {name!r}: {restored}",
                )
            row = con.execute(f"SELECT count(*) FROM {_q(name)}").fetchone()
            counts[name] = int(row[0]) if row else 0

        fks = [ForeignKey.parse(text) for text in spec.foreign_keys]
        problem = _check_exemptions(exempt, fks, counts)
        if problem:
            return VerificationReport("integrity-failed", detail=problem, row_counts=counts)
        con.execute("CREATE TABLE __fs_exempt(edge INTEGER, row_index BIGINT)")
        if exempt:
            con.executemany("INSERT INTO __fs_exempt VALUES (?, ?)", [list(e) for e in exempt])

        violations: dict[str, int] = {}
        for i, fk in enumerate(fks):
            # Project only the FK column into a scratch table: its row-id pseudo column
            # is then the output row index, even if the user's table has a "rowid" column.
            con.execute(
                f"CREATE TABLE __fs_ix AS SELECT {_q(fk.child_column)} AS v FROM {_q(fk.child)}"
            )
            row = con.execute(
                f"SELECT count(*) FROM (SELECT rowid AS ri, v FROM __fs_ix) c "
                f"WHERE c.v IS NOT NULL AND NOT EXISTS "
                f"(SELECT 1 FROM {_q(fk.parent)} p WHERE p.{_q(fk.parent_column)} = c.v) "
                f"AND NOT EXISTS (SELECT 1 FROM __fs_exempt x "
                f"WHERE x.edge = {i} AND x.row_index = c.ri)"
            ).fetchone()
            con.execute("DROP TABLE __fs_ix")
            n = int(row[0]) if row else 0
            if n:
                violations[str(fk)] = n
        con.execute("DROP TABLE __fs_exempt")
        con.execute("DROP SCHEMA __fs_raw")
        runner = PredicateRunner(predicate, expected)
        outcome = runner.run(con, float(spec.settings.eval_timeout_s))
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
        detail = outcome.detail
        if spec.warnings:
            detail = "; ".join([detail, *spec.warnings]) if detail else "; ".join(spec.warnings)
        return VerificationReport(status, outcome.kind.value, outcome.signature, detail, counts, {})
    finally:
        con.close()


def _check_exemptions(
    exempt: list[tuple[Any, ...]], fks: list[ForeignKey], counts: dict[str, int]
) -> str | None:
    """Orphan exemptions must name an existing edge and a row of that edge's child
    table (tampered or malformed metadata would otherwise hide violations)."""
    seen: set[tuple[int, int]] = set()
    for edge, row_index in exempt:
        if edge is None or row_index is None:
            return "malformed orphans metadata: NULL edge or row index"
        if not all(isinstance(v, int) and not isinstance(v, bool) for v in (edge, row_index)):
            return f"malformed orphans metadata: non-integer entry ({edge!r}, {row_index!r})"
        if not 0 <= int(edge) < len(fks):
            return f"malformed orphans metadata: unknown edge {edge}"
        child = fks[int(edge)].child
        if not 0 <= int(row_index) < counts.get(child, 0):
            return f"malformed orphans metadata: row {row_index} not in {child!r}"
        if (int(edge), int(row_index)) in seen:
            return f"malformed orphans metadata: duplicate entry ({edge}, {row_index})"
        seen.add((int(edge), int(row_index)))
    return None


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
