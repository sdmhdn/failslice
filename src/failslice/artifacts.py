"""Run finalization: CSV copies, the pytest bundle, explain, ``report.md`` and
``manifest.json`` (ADR 0009).

:func:`finalize` runs after the replay, in the calling process, for both execution modes.
It reads only the run directory and the in-memory results; it never reopens the original
inputs, and unknown numbers stay ``null``. Each step checks the deadline first and runs its
DuckDB work under the cooperative watchdog; a skipped or failed step is recorded in the
manifest. ``manifest.json`` is written atomically and last.
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import hashlib
import json
import os
import platform
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict

from failslice import __version__
from failslice.config import Project
from failslice.explain import Explanation, explain
from failslice.portable import DeadlineConnection
from failslice.pytest_export import build_bundle
from failslice.reducer import Minimality
from failslice.report import render_report
from failslice.runspec import SPEC_FILE, RunSpec

if TYPE_CHECKING:
    from failslice.api import ReductionResult

__all__ = [
    "MANIFEST_FILE",
    "ArtifactEntry",
    "Manifest",
    "RunContext",
    "StepStatus",
    "finalize",
    "load_manifest",
]

MANIFEST_FILE = "manifest.json"
REPORT_FILE = "report.md"
EXPLAIN_FILE = "explain.json"
PYTEST_DIR = "pytest"
CSV_DIR = "csv"
MANIFEST_FORMAT = 1

Role = Literal[
    "table", "csv", "spec", "rowmap", "orphans", "expected", "report", "explain", "pytest"
]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ArtifactEntry(_Model):
    """One file of the run directory (the manifest and the checkpoint are not listed)."""

    path: str
    role: Role
    bytes: int
    sha256: str


class StepStatus(_Model):
    """Outcome of one finalization step."""

    name: Literal["csv", "pytest", "explain", "report"]
    status: Literal["done", "skipped", "failed"]
    detail: str = ""


class Manifest(_Model):
    """The authoritative summary of one run (``manifest.json``)."""

    format: Literal[1] = 1
    run_id: str
    created_at: str
    finished_at: str
    elapsed_s: float
    """Whole run as measured by the caller (reduction, export, replay, finalization)."""
    versions: dict[str, str]
    status: Literal["reduced", "no-candidate", "cancelled"]
    minimality: str
    """The label: "1-minimal (verified)", "reduced, minimality not verified" or "no reproducer"."""
    minimality_detail: str
    verification: dict[str, Any] | None
    config: dict[str, Any]
    settings: dict[str, Any]
    inputs: dict[str, dict[str, Any]]
    tables: dict[str, dict[str, Any]]
    rows_before: int | None
    rows_after: int | None
    shrink_ratio: float | None
    """``rows_after / rows_before``."""
    stats: dict[str, Any] | None
    failure: Literal["killed", "crashed", "cancelled"] | None = None
    """Why the run stopped early, if it did: cancelled by the caller, or (isolated) the
    reduction process was killed or crashed."""
    notes: tuple[str, ...]
    steps: tuple[StepStatus, ...]
    artifacts: tuple[ArtifactEntry, ...]


@dataclass(frozen=True)
class RunContext:
    """Facts fixed when the run starts (passed to :func:`finalize`)."""

    run_id: str
    run_dir: Path
    created_at: str
    started: float
    """``time.monotonic()`` at the start."""
    config: dict[str, Any]
    settings: dict[str, Any]

    @classmethod
    def start(cls, project: Project, run_dir: Path, run_id: str) -> RunContext:
        """Capture the context at the start of a run."""
        return cls(
            run_id=run_id,
            run_dir=run_dir,
            created_at=_now(),
            started=time.monotonic(),
            config=project_config(project),
            settings=project.settings.model_dump(mode="json"),
        )


def project_config(project: Project) -> dict[str, Any]:
    """The effective configuration as plain data (paths resolved)."""
    return {
        "base_dir": str(project.base_dir),
        "root": str(project.root),
        "tables": {
            name: {
                **spec.model_dump(mode="json", exclude_none=True),
                "resolved": str(project.table_path(name)),
            }
            for name, spec in project.tables.items()
        },
        "foreign_keys": [str(fk) for fk in project.foreign_keys],
        "predicate": project.predicate.model_dump(mode="json"),
    }


def _now() -> str:
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="microseconds")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def versions() -> dict[str, str]:
    """FailSlice, DuckDB and Python versions."""
    return {
        "failslice": __version__,
        "duckdb": duckdb.__version__,
        "python": platform.python_version(),
    }


def minimality_label(result: ReductionResult, spec: RunSpec | None) -> str:
    """ "1-minimal (verified)" only if certification completed, the replay is verified and
    the export recorded no lossy round trip; "reduced, ..." otherwise."""
    if result.status != "reduced":
        return "no reproducer"
    certified = result.stats is not None and result.stats.minimality is Minimality.VERIFIED
    replayed = result.verification is not None and result.verification.ok
    lossless = spec is not None and not spec.warnings
    if certified and replayed and lossless:
        return "1-minimal (verified)"
    return "reduced, minimality not verified"


class _Steps:
    def __init__(self, deadline: float | None, cancel: threading.Event | None = None) -> None:
        self.deadline = deadline
        self.cancel = cancel
        self.done: list[StepStatus] = []

    def remaining(self) -> float | None:
        return None if self.deadline is None else self.deadline - time.monotonic()

    def run(self, name: Any, fn: Callable[[], None], skip: str | None = None) -> bool:
        if skip is not None:
            self.done.append(StepStatus(name=name, status="skipped", detail=skip))
            return False
        if self.cancel is not None and self.cancel.is_set():
            self.done.append(StepStatus(name=name, status="skipped", detail="cancelled"))
            return False
        left = self.remaining()
        if left is not None and left <= 0:
            self.done.append(StepStatus(name=name, status="skipped", detail="deadline reached"))
            return False
        try:
            fn()
        except Exception as exc:
            self.done.append(
                StepStatus(name=name, status="failed", detail=f"{type(exc).__name__}: {exc}")
            )
            return False
        self.done.append(StepStatus(name=name, status="done"))
        return True


def write_csv(run_dir: Path, spec: RunSpec, deadline: float | None) -> None:
    """Write ``csv/<t>.csv`` (UTF-8, header, ``\\N`` for NULL, non-NULL strings quoted);
    every statement is bounded by ``deadline``."""
    out = run_dir / CSV_DIR
    out.mkdir(exist_ok=True)
    raw = duckdb.connect(":memory:")
    con = DeadlineConnection(raw, deadline)
    try:
        for name, entry in spec.tables.items():
            src = spec.resolve(run_dir, entry.file)
            dest = out / f"{name}.csv"
            text_cols = [c for c, t in entry.columns if t == "VARCHAR"]
            quote = f", FORCE_QUOTE ({', '.join(_q(c) for c in text_cols)})" if text_cols else ""
            con.execute(
                f"COPY (SELECT * FROM read_parquet({_lit(str(src))})) TO {_lit(str(dest))} "
                f"(FORMAT csv, HEADER true, NULLSTR '\\N'{quote})"
            )
    finally:
        raw.close()


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _inventory(run_dir: Path) -> tuple[ArtifactEntry, ...]:
    roles: list[tuple[str, Role]] = [
        (SPEC_FILE, "spec"),
        ("rowmap.parquet", "rowmap"),
        ("orphans.parquet", "orphans"),
        (REPORT_FILE, "report"),
        (EXPLAIN_FILE, "explain"),
    ]
    entries: list[ArtifactEntry] = []
    files: list[tuple[Path, Role]] = [(run_dir / rel, role) for rel, role in roles]
    folders: list[tuple[str, Role]] = [
        ("tables", "table"),
        (CSV_DIR, "csv"),
        ("expected", "expected"),
        (PYTEST_DIR, "pytest"),
    ]
    for folder, role in folders:
        base = run_dir / folder
        if base.is_dir():
            files.extend((p, role) for p in sorted(base.rglob("*")) if p.is_file())
    for path, role in files:
        if path.is_file():
            entries.append(
                ArtifactEntry(
                    path=path.relative_to(run_dir).as_posix(),
                    role=role,
                    bytes=path.stat().st_size,
                    sha256=_sha256(path),
                )
            )
    return tuple(entries)


def finalize(
    context: RunContext,
    result: ReductionResult,
    *,
    deadline: float | None = None,
    explain_limit: int = 10,
    cancel: threading.Event | None = None,
) -> Manifest:
    """Write the remaining artifacts and the manifest for a finished run. ``cancel`` is
    checked between steps (cooperative): once set, the remaining artifact steps are
    skipped and only the report and the manifest are written."""
    run_dir = context.run_dir
    spec = result.spec
    steps = _Steps(deadline, cancel)
    no_spec = None if spec is not None else "no exported reproducer"
    steps.run("csv", lambda: write_csv(run_dir, spec, deadline), skip=no_spec)  # type: ignore[arg-type]
    verified = result.verification is not None and result.verification.ok
    steps.run(
        "pytest",
        lambda: _discard(build_bundle(run_dir, run_dir / PYTEST_DIR, deadline=deadline)),
        skip=no_spec or (None if verified else "replay is not verified"),
    )
    explanation: list[Explanation] = []

    def do_explain() -> None:
        explanation.append(explain(run_dir, limit=explain_limit, deadline=deadline))
        (run_dir / EXPLAIN_FILE).write_text(
            explanation[0].model_dump_json(indent=2) + "\n", encoding="utf-8"
        )

    steps.run("explain", do_explain, skip=no_spec)
    if cancel is not None and cancel.is_set() and result.status != "cancelled":
        # Cancelled while artifacts were being written: the run's outcome is "cancelled"
        # (the step history above shows what was finished before).
        result = dataclasses.replace(
            result,
            status="cancelled",
            failure="cancelled",
            notes=[*result.notes, "cancelled by the caller while writing artifacts"],
        )
    manifest_without_report = _manifest(context, result, steps.done, ())
    # The report is written regardless of the deadline or a cancel (bounded by metadata size).
    steps.deadline = None
    steps.cancel = None
    steps.run(
        "report",
        lambda: _discard(
            (run_dir / REPORT_FILE).write_text(
                render_report(manifest_without_report, explanation[0] if explanation else None),
                encoding="utf-8",
            )
        ),
    )
    manifest = _manifest(context, result, steps.done, _inventory(run_dir))
    tmp = run_dir / (MANIFEST_FILE + ".tmp")
    tmp.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, run_dir / MANIFEST_FILE)
    return manifest


def _manifest(
    context: RunContext,
    result: ReductionResult,
    steps: list[StepStatus],
    artifacts: tuple[ArtifactEntry, ...],
) -> Manifest:
    spec = result.spec
    stats = result.stats
    tables: dict[str, dict[str, Any]] = {}
    inputs: dict[str, dict[str, Any]] = {}
    if spec is not None:
        inputs = {k: v.model_dump() for k, v in spec.inputs.items()}
        for name, entry in spec.tables.items():
            tables[name] = {
                "rows_before": stats.sizes_before.get(name) if stats else None,
                "rows_after": entry.rows,
                "columns": [list(c) for c in entry.source_columns],
                "exported_columns": [list(c) for c in entry.columns],
                "primary_key": entry.primary_key,
            }
    elif stats is not None:
        for name, n in stats.sizes_before.items():
            tables[name] = {"rows_before": n, "rows_after": None}
    before = stats.input_rows if stats is not None and stats.sizes_before else None
    after = sum(e.rows for e in spec.tables.values()) if spec is not None else None
    ratio = round(after / before, 6) if before and after is not None else None
    stats_data: dict[str, Any] | None = None
    if stats is not None:
        known = stats.complete
        stats_data = {
            "complete": known,
            "evaluations": stats.evaluations,
            "evaluations_started": stats.evaluations_started,
            "outcomes": dict(stats.outcomes) if known else None,
            "error_classes": dict(stats.error_classes) if known else None,
            "cache_hits": stats.cache_hits if known else None,
            "accepted": stats.accepted if known else None,
            "sweeps": stats.sweeps if known else None,
            "stop_reason": stats.stop_reason.value,
            "reduction_elapsed_s": round(stats.elapsed_s, 3),
        }
    return Manifest(
        run_id=context.run_id,
        created_at=context.created_at,
        finished_at=_now(),
        elapsed_s=round(time.monotonic() - context.started, 3),
        versions=versions(),
        status=result.status,
        minimality=minimality_label(result, spec),
        minimality_detail=stats.minimality_detail if stats is not None else "",
        verification=json.loads(result.verification.to_json()) if result.verification else None,
        config=context.config,
        settings=context.settings,
        inputs=inputs,
        tables=tables,
        rows_before=before,
        rows_after=after,
        shrink_ratio=ratio,
        stats=stats_data,
        failure=result.failure,
        notes=tuple(result.notes),
        steps=tuple(steps),
        artifacts=artifacts,
    )


def _discard(_value: object) -> None:
    return None


def load_manifest(run_dir: str | Path) -> Manifest:
    """Read ``manifest.json`` from a run directory."""
    path = Path(run_dir) / MANIFEST_FILE
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("format") != MANIFEST_FORMAT:
        raise ValueError(f"{path}: unsupported manifest format {data.get('format')!r}")
    return Manifest.model_validate(data)
