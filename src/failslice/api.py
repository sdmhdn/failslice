"""Public API: :func:`check`, :func:`reduce`, :func:`verify`."""

from __future__ import annotations

import datetime as _dt
import json
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from failslice.config import ConfigError, Project
from failslice.engine import Engine
from failslice.predicate import Outcome
from failslice.reducer import (
    Budget,
    DeterminismError,
    Minimality,
    PredicateNotFailingError,
    ProgressEvent,
    Reducer,
    ReductionStats,
)
from failslice.runspec import RunSpec
from failslice.safety import validate_predicate_sql
from failslice.supervisor import SupervisedOutcome, run_supervised, write_checkpoint
from failslice.verify import VerificationReport, verify_in_subprocess

__all__ = ["CheckReport", "ReductionResult", "check", "reduce", "validate_predicate", "verify"]

MIN_REPLAY_S = 10.0


def replay_reserve(wall_clock_s: float) -> float:
    """Part of ``wall_clock_s`` kept back for export and the independent replay:
    10% but at least ``MIN_REPLAY_S``, and never more than half the budget (so short
    budgets still leave time to reduce)."""
    return min(max(MIN_REPLAY_S, 0.1 * wall_clock_s), 0.5 * wall_clock_s)


def validate_predicate(project: Project) -> None:
    """Validate the predicate query against the safety rules (ADR 0007)."""
    for sql in project.predicate_queries():
        validate_predicate_sql(sql, project.tables.keys())


@dataclass
class CheckReport:
    """Result of :func:`check`."""

    ok: bool
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    orphans: dict[str, int] = field(default_factory=dict)
    outcomes: list[Outcome] = field(default_factory=list)
    deterministic: bool = False
    problems: list[str] = field(default_factory=list)


def check(project: Project) -> CheckReport:
    """Validate the predicate, load the data, report FK orphans and run the
    determinism check (the predicate must be TRUE on the full data, four times,
    with identical signatures)."""
    report = CheckReport(ok=False)
    validate_predicate(project)
    engine = Engine(project)
    try:
        for name, info in engine.tables.items():
            report.tables[name] = {
                "rows": info.row_count,
                "columns": [list(c) for c in info.columns],
                "source": info.source,
            }
        report.orphans = dict(engine.orphan_counts)
        reducer = Reducer(engine, Budget(max_evals=4, wall_clock_s=project.settings.wall_clock_s))
        try:
            report.outcomes = reducer.check_determinism()
            report.deterministic = True
            report.ok = True
        except DeterminismError as exc:
            report.outcomes = exc.outcomes
            report.problems.append(str(exc))
    finally:
        engine.close()
    return report


@dataclass
class ReductionResult:
    """Result of :func:`reduce`.

    ``status`` is ``"reduced"`` when a configuration was observed TRUE and exported,
    ``"no-candidate"`` when the run stopped before any confirmed configuration.
    The reproducer is trustworthy only if ``verification.ok``.
    """

    run_dir: Path
    status: str
    stats: ReductionStats | None
    """Counters of the run; also present for ``no-candidate`` when the run got far
    enough to count anything."""
    verification: VerificationReport | None
    notes: list[str] = field(default_factory=list)
    spec: RunSpec | None = None
    """The exported reproducer's contract (``spec.json``), if one was exported."""

    @property
    def minimality_label(self) -> str:
        """Human label: ``1-minimal (verified)`` only if certification completed."""
        if self.status != "reduced":
            return "no reproducer"
        if self.stats is not None and self.stats.minimality is Minimality.VERIFIED:
            return "1-minimal (verified)"
        return "reduced, minimality not verified"


def _new_run_dir(project: Project, out_dir: str | Path | None) -> tuple[Path, str]:
    run_id = _dt.datetime.now(_dt.UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3)
    if out_dir is None:
        run_dir = project.base_dir / ".failslice" / "runs" / run_id
    else:
        run_dir = Path(out_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    if any(p.name != ".gitkeep" for p in run_dir.iterdir()):
        raise ConfigError(f"run directory {run_dir} is not empty")
    return run_dir, run_id


def reduce(
    project: Project,
    *,
    out_dir: str | Path | None = None,
    isolate: bool = False,
    on_progress: Callable[[ProgressEvent], None] | None = None,
) -> ReductionResult:
    """Reduce the project's data to a smaller configuration that keeps the predicate
    TRUE, export it into a run directory and replay it in a fresh process.

    Budgets and seed come from ``project.settings`` (use
    :meth:`Project.with_settings` to change them). With ``isolate=True`` the reduction
    runs in a supervised child process with hard deadlines; otherwise in-process,
    where per-evaluation timeouts are cooperative (DuckDB interrupt).
    """
    validate_predicate(project)
    settings = project.settings
    deadline = time.monotonic() + settings.wall_clock_s  # covers reduction, export, replay
    run_dir, run_id = _new_run_dir(project, out_dir)
    reserve = replay_reserve(settings.wall_clock_s)
    budget = Budget(max_evals=settings.max_evals, wall_clock_s=settings.wall_clock_s - reserve)
    notes: list[str] = []
    stats: ReductionStats | None
    if isolate:
        sup = run_supervised(
            project,
            run_dir,
            run_id,
            budget,
            deadline=deadline - reserve,
            on_progress=on_progress,
        )
        stats = _supervised_stats(sup, notes)
    else:
        stats = _reduce_in_process(project, run_dir, run_id, budget, on_progress)
    if stats is None or not stats.confirmed:
        notes.append("stopped before any configuration was confirmed TRUE")
        result = ReductionResult(run_dir, "no-candidate", stats, None, notes)
        _write_result(result)
        return result
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        verification = VerificationReport(
            "unverified", detail="no time left for the independent replay (wall_clock_s)"
        )
    else:
        verification = verify_in_subprocess(run_dir, timeout_s=remaining)
    spec = RunSpec.read(run_dir)
    notes.extend(spec.warnings)
    result = ReductionResult(run_dir, "reduced", stats, verification, notes, spec)
    _write_result(result)
    return result


def _supervised_stats(sup: SupervisedOutcome, notes: list[str]) -> ReductionStats | None:
    if sup.determinism_error is not None:
        if sup.predicate_not_failing:
            raise PredicateNotFailingError(sup.determinism_error, [])
        raise DeterminismError(sup.determinism_error, [])
    if sup.killed_reason:
        notes.append(sup.killed_reason)
    if sup.error:
        notes.append(sup.error)
    if sup.recovered:
        notes.append("recovered the last checkpoint after the reduction process stopped")
    if sup.stats is None:
        return sup.unconfirmed_stats
    return sup.stats


def _reduce_in_process(
    project: Project,
    run_dir: Path,
    run_id: str,
    budget: Budget,
    on_progress: Callable[[ProgressEvent], None] | None,
) -> ReductionStats:
    engine = Engine(project, allowed_directories=[run_dir])
    try:
        reducer = Reducer(
            engine,
            budget,
            on_progress=on_progress,
            on_accept=lambda eng: write_checkpoint(eng, run_dir, run_id, reducer.snapshot()),
        )
        stats = reducer.run(certify=project.settings.minimality)
        if stats.confirmed:  # never export a configuration not observed TRUE
            engine.export(run_dir)
        return stats
    finally:
        engine.close()


def _write_result(result: ReductionResult) -> None:
    stats = result.stats
    data: dict[str, Any] = {
        "status": result.status,
        "minimality": result.minimality_label,
        "notes": result.notes,
        "verification": json.loads(result.verification.to_json()) if result.verification else None,
    }
    if stats is not None:
        data.update(
            {
                "sizes_before": stats.sizes_before,
                "sizes_after": stats.sizes_after,
                "evaluations": stats.evaluations,
                "evaluations_started": stats.evaluations_started,
                "outcomes": dict(stats.outcomes),
                "error_classes": dict(stats.error_classes),
                "cache_hits": stats.cache_hits,
                "accepted": stats.accepted,
                "sweeps": stats.sweeps,
                "stop_reason": stats.stop_reason.value,
                "minimality_detail": stats.minimality_detail,
                "elapsed_s": round(stats.elapsed_s, 3),
            }
        )
    (result.run_dir / "result.json").write_text(json.dumps(data, indent=2) + "\n")


def verify(run_dir: str | Path, *, timeout_s: float = 300.0) -> VerificationReport:
    """Replay a run directory in a fresh process (see :mod:`failslice.verify`)."""
    return verify_in_subprocess(run_dir, timeout_s=timeout_s)
