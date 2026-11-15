"""Run supervisor: reduction in a child process with checkpoints and hard deadlines.

ADR 0005. The child process loads the data, reduces, and writes an atomic checkpoint
(kept row ids per table) after the determinism check and after every accepted
configuration. It announces every predicate evaluation *before* it starts, so the
parent's evaluation count stays exact even if the child is killed. The parent kills
the child when the wall clock is exceeded or a single evaluation overruns
``eval_timeout_s + grace``, then recovers the last checkpoint by re-loading the
original files.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import queue
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from failslice.config import Project
from failslice.engine import Engine
from failslice.reducer import (
    Budget,
    DeterminismError,
    Minimality,
    ProgressEvent,
    Reducer,
    ReductionStats,
    StopReason,
)

__all__ = [
    "CHECKPOINT",
    "SupervisedOutcome",
    "read_checkpoint",
    "run_supervised",
    "write_checkpoint",
]

CHECKPOINT = "checkpoint.json"
DEFAULT_GRACE_S = 5.0


def write_checkpoint(engine: Engine, run_dir: Path, run_id: str) -> None:
    """Atomically write the engine's current configuration to ``run_dir``."""
    data = {
        "run_id": run_id,
        "inputs": {n: t.source_sha256 for n, t in engine.tables.items()},
        "kept": engine.kept_rids(),
        "sizes": engine.sizes,
    }
    tmp = run_dir / (CHECKPOINT + ".tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, run_dir / CHECKPOINT)


def read_checkpoint(run_dir: Path, run_id: str) -> dict[str, Any] | None:
    """Return the checkpoint for ``run_id``, or ``None`` if there is none."""
    path = run_dir / CHECKPOINT
    if not path.is_file():
        return None
    data: dict[str, Any] = json.loads(path.read_text())
    return data if data.get("run_id") == run_id else None


def _stats_to_dict(stats: ReductionStats) -> dict[str, Any]:
    d = asdict(stats)
    d["outcomes"] = dict(stats.outcomes)
    d["error_classes"] = dict(stats.error_classes)
    return d


def stats_from_dict(d: dict[str, Any]) -> ReductionStats:
    """Inverse of the child's stats serialization."""
    d = dict(d)
    d["outcomes"] = Counter(d["outcomes"])
    d["error_classes"] = Counter(d["error_classes"])
    d["stop_reason"] = StopReason(d["stop_reason"])
    d["minimality"] = Minimality(d["minimality"])
    return ReductionStats(**d)


def _child(project: Project, run_dir: str, run_id: str, budget: Budget, out: Any) -> None:
    rd = Path(run_dir)
    try:
        engine = Engine(project, allowed_directories=[rd])
        out.put(("loaded", engine.sizes))
        reducer = Reducer(
            engine,
            budget,
            on_progress=lambda ev: out.put(("progress", asdict(ev))),
            before_eval=lambda: out.put(("eval_start", None)),
            on_accept=lambda eng: write_checkpoint(eng, rd, run_id),
        )
        try:
            reducer.check_determinism()
        except DeterminismError as exc:
            if not project.settings.allow_nondeterministic:
                out.put(("determinism_error", str(exc)))
                return
        write_checkpoint(engine, rd, run_id)
        out.put(("checkpoint", None))
        stats = reducer.run(check_determinism=False, certify=project.settings.minimality)
        stats.confirmed = True
        stats.determinism_signatures = reducer.stats.determinism_signatures
        engine.export(rd)
        out.put(("done", _stats_to_dict(stats)))
    except BaseException as exc:  # report everything to the parent
        out.put(("crash", f"{type(exc).__name__}: {exc}"))


class SupervisedOutcome:
    """What the parent learned from a supervised run."""

    def __init__(self) -> None:
        self.stats: ReductionStats | None = None
        self.evaluations_started = 0
        self.killed_reason: str | None = None
        self.error: str | None = None
        self.determinism_error: str | None = None
        self.sizes_before: dict[str, int] = {}
        self.recovered = False
        self.has_checkpoint = False


def run_supervised(
    project: Project,
    run_dir: Path,
    run_id: str,
    budget: Budget,
    *,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    grace_s: float = DEFAULT_GRACE_S,
) -> SupervisedOutcome:
    """Run the reduction in a child process under hard deadlines.

    On success the child has exported the result into ``run_dir``. If the child is
    killed or crashes after the initial checkpoint, the parent re-loads the original
    files, restores the checkpoint and exports it (``recovered``).
    """
    ctx = mp.get_context("spawn")
    events: Any = ctx.Queue()
    proc = ctx.Process(
        target=_child, args=(project, str(run_dir), run_id, budget, events), daemon=True
    )
    start = time.monotonic()
    deadline = start + budget.wall_clock_s + grace_s
    eval_limit = project.settings.eval_timeout_s + grace_s
    result = SupervisedOutcome()
    eval_started_at: float | None = None
    proc.start()
    finished = False
    try:
        while not finished:
            now = time.monotonic()
            if now > deadline:
                result.killed_reason = "wall-clock deadline exceeded"
                break
            if eval_started_at is not None and now - eval_started_at > eval_limit:
                result.killed_reason = (
                    f"engine unresponsive: one evaluation exceeded {eval_limit:g}s"
                )
                break
            try:
                kind, payload = events.get(timeout=0.2)
            except queue.Empty:
                if not proc.is_alive():
                    result.error = result.error or f"child exited with code {proc.exitcode}"
                    break
                continue
            if kind == "loaded":
                result.sizes_before = dict(payload)
            elif kind == "eval_start":
                result.evaluations_started += 1
                eval_started_at = time.monotonic()
            elif kind == "progress":
                eval_started_at = None
                if on_progress is not None:
                    on_progress(ProgressEvent(**payload))
            elif kind == "checkpoint":
                result.has_checkpoint = True
            elif kind == "determinism_error":
                result.determinism_error = str(payload)
                finished = True
            elif kind == "crash":
                result.error = str(payload)
                finished = True
            elif kind == "done":
                result.stats = stats_from_dict(payload)
                finished = True
    finally:
        if proc.is_alive():
            if result.stats is None and result.determinism_error is None:
                proc.kill()
            proc.join(timeout=grace_s)
            if proc.is_alive():  # pragma: no cover - last resort
                proc.kill()
                proc.join()
        events.close()

    if result.stats is None and result.determinism_error is None:
        _recover(project, run_dir, run_id, result)
    return result


def _recover(project: Project, run_dir: Path, run_id: str, result: SupervisedOutcome) -> None:
    checkpoint = read_checkpoint(run_dir, run_id)
    if checkpoint is None:
        return
    result.has_checkpoint = True
    engine = Engine(project, allowed_directories=[run_dir])
    try:
        inputs = {n: t.source_sha256 for n, t in engine.tables.items()}
        if inputs != checkpoint["inputs"]:
            result.error = "input files changed since the checkpoint; cannot recover"
            return
        sizes_before = engine.sizes
        engine.reset_state(checkpoint["kept"])
        engine.export(run_dir)
        stats = ReductionStats(sizes_before=result.sizes_before or sizes_before)
        stats.sizes_after = engine.sizes
        stats.evaluations = result.evaluations_started
        stats.stop_reason = StopReason.KILLED
        stats.minimality = (
            Minimality.NOT_VERIFIED if project.settings.minimality else (Minimality.SKIPPED)
        )
        stats.minimality_detail = f"run stopped: {result.killed_reason or result.error}"
        stats.confirmed = True
        result.stats = stats
        result.recovered = True
    finally:
        engine.close()
