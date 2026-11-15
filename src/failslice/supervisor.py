"""Run supervisor: reduction in a child process with checkpoints and hard deadlines.

ADR 0005. The child process loads the data, reduces, and writes an atomic checkpoint
(kept row ids per table) once the full data is confirmed TRUE and after every accepted
configuration. Before every predicate execution it announces the evaluation over a pipe
(a synchronous send), so the parent's evaluation count is exact even if the child is
killed. The parent kills the child when the deadline passes or a single evaluation
overruns ``eval_timeout_s + grace``. Recovery (re-loading the original files and
exporting the last checkpoint) also runs in a child process under the same deadline.

The parent closes its copy of the pipe's write end, so a child that dies mid-message
produces EOF instead of a blocked read.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from failslice.config import Project
from failslice.engine import Engine
from failslice.reducer import (
    Budget,
    DeterminismError,
    Minimality,
    PredicateNotFailingError,
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
    "stats_from_dict",
    "write_checkpoint",
]

CHECKPOINT = "checkpoint.json"
DEFAULT_GRACE_S = 5.0


def write_checkpoint(engine: Engine, run_dir: Path, run_id: str) -> None:
    """Atomically write the engine's current configuration to ``run_dir``."""
    data = {
        "run_id": run_id,
        "inputs": engine.input_identity(),
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


def _reduce_child(
    project: Project, run_dir: str, run_id: str, budget: Budget, conn: Connection
) -> None:
    rd = Path(run_dir)
    send = conn.send
    try:
        engine = Engine(project, allowed_directories=[rd])
        send(("loaded", engine.sizes))

        def confirmed(eng: Engine) -> None:
            write_checkpoint(eng, rd, run_id)
            send(("checkpoint", None))

        reducer = Reducer(
            engine,
            budget,
            on_progress=lambda ev: send(("progress", asdict(ev))),
            before_eval=lambda: send(("eval_start", None)),
            on_accept=lambda eng: write_checkpoint(eng, rd, run_id),
            on_confirmed=confirmed,
        )
        try:
            stats = reducer.run(certify=project.settings.minimality)
        except DeterminismError as exc:
            kind = "not_failing" if isinstance(exc, PredicateNotFailingError) else "unstable"
            send(("determinism_error", (kind, str(exc))))
            return
        if not stats.confirmed:
            send(("unconfirmed", _stats_to_dict(stats)))
            return
        engine.export(rd)
        send(("done", _stats_to_dict(stats)))
    except BaseException as exc:  # report everything to the parent
        with contextlib.suppress(Exception):
            send(("crash", f"{type(exc).__name__}: {exc}"))
    finally:
        conn.close()


def _recover_child(project: Project, run_dir: str, run_id: str, conn: Connection) -> None:
    rd = Path(run_dir)
    try:
        checkpoint = read_checkpoint(rd, run_id)
        if checkpoint is None:
            conn.send(("no_checkpoint", None))
            return
        engine = Engine(project, allowed_directories=[rd])
        try:
            if engine.input_identity() != checkpoint["inputs"]:
                conn.send(("crash", "input files changed since the checkpoint; cannot recover"))
                return
            engine.reset_state(checkpoint["kept"])
            engine.export(rd)
            conn.send(("recovered", engine.sizes))
        finally:
            engine.close()
    except BaseException as exc:
        with contextlib.suppress(Exception):
            conn.send(("crash", f"recovery failed: {type(exc).__name__}: {exc}"))
    finally:
        conn.close()


@dataclass
class SupervisedOutcome:
    """What the parent learned from a supervised run."""

    stats: ReductionStats | None = None
    evaluations_started: int = 0
    killed_reason: str | None = None
    error: str | None = None
    determinism_error: str | None = None
    predicate_not_failing: bool = False
    sizes_before: dict[str, int] = field(default_factory=dict)
    recovered: bool = False
    has_checkpoint: bool = False
    exported: bool = False


def _pump(
    target: Callable[..., None],
    args: tuple[Any, ...],
    deadline: float,
    on_event: Callable[[str, Any], bool],
    eval_limit: float | None,
    grace_s: float,
) -> str | None:
    """Run ``target`` in a spawned child, feeding events to ``on_event`` (which returns
    True when the child has reported a final result). Returns a kill reason, or None."""
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=target, args=(*args, child_conn), daemon=True)
    proc.start()
    child_conn.close()  # only the child may write; its death then yields EOF here
    eval_started: list[float | None] = [None]
    kill_reason: str | None = None

    def handle(kind: str, payload: Any) -> bool:
        if kind == "eval_start":
            eval_started[0] = time.monotonic()
        elif kind == "progress":
            eval_started[0] = None
        return on_event(kind, payload)

    finished = False
    try:
        while not finished:
            now = time.monotonic()
            if now > deadline:
                kill_reason = "deadline exceeded"
                break
            started = eval_started[0]
            if eval_limit is not None and started is not None and now - started > eval_limit:
                kill_reason = f"engine unresponsive: one evaluation exceeded {eval_limit:g}s"
                break
            try:
                if not parent_conn.poll(min(0.2, max(0.0, deadline - now))):
                    continue
                kind, payload = parent_conn.recv()
            except (EOFError, OSError):
                break  # child exited (possibly mid-message)
            finished = handle(kind, payload)
    finally:
        if proc.is_alive() and not finished:
            proc.kill()
        proc.join(timeout=grace_s)
        if proc.is_alive():  # pragma: no cover - last resort
            proc.kill()
            proc.join()
        # Drain complete messages the child sent before it stopped (exact counts).
        with contextlib.suppress(EOFError, OSError):
            while not finished and parent_conn.poll(0):
                finished = handle(*parent_conn.recv())
        parent_conn.close()
    return kill_reason


def run_supervised(
    project: Project,
    run_dir: Path,
    run_id: str,
    budget: Budget,
    *,
    deadline: float | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    grace_s: float = DEFAULT_GRACE_S,
) -> SupervisedOutcome:
    """Run the reduction in a child process under hard deadlines.

    ``deadline`` (``time.monotonic()`` value) bounds reduction *and* recovery; by default
    it is ``budget.wall_clock_s + grace_s`` from now. On success the child has exported
    the result into ``run_dir`` (``exported``). If the child is killed or crashes after
    the initial checkpoint, a recovery child re-loads the original files, restores the
    checkpoint and exports it (``recovered``).
    """
    start = time.monotonic()
    hard = start + budget.wall_clock_s + grace_s
    deadline = hard if deadline is None else min(deadline, hard)
    result = SupervisedOutcome()

    def on_reduce_event(kind: str, payload: Any) -> bool:
        if kind == "loaded":
            result.sizes_before = dict(payload)
        elif kind == "eval_start":
            result.evaluations_started += 1
        elif kind == "progress":
            if on_progress is not None:
                on_progress(ProgressEvent(**payload))
        elif kind == "checkpoint":
            result.has_checkpoint = True
        elif kind == "determinism_error":
            reason, message = payload
            result.determinism_error = str(message)
            result.predicate_not_failing = reason == "not_failing"
            return True
        elif kind == "crash":
            result.error = str(payload)
            return True
        elif kind == "unconfirmed":
            return True
        elif kind == "done":
            result.stats = stats_from_dict(payload)
            result.exported = True
            return True
        return False

    result.killed_reason = _pump(
        _reduce_child,
        (project, str(run_dir), run_id, budget),
        deadline,
        on_reduce_event,
        project.settings.eval_timeout_s + grace_s,
        grace_s,
    )
    if result.stats is None and result.determinism_error is None:
        _recover(project, run_dir, run_id, result, deadline, grace_s)
    return result


def _recover(
    project: Project,
    run_dir: Path,
    run_id: str,
    result: SupervisedOutcome,
    deadline: float,
    grace_s: float,
) -> None:
    if read_checkpoint(run_dir, run_id) is None:
        return
    result.has_checkpoint = True
    recovered: dict[str, Any] = {}

    def on_event(kind: str, payload: Any) -> bool:
        if kind == "recovered":
            recovered["sizes"] = dict(payload)
        elif kind == "crash":
            result.error = "; ".join(x for x in (result.error, str(payload)) if x)
        return True

    # Recovery gets at least a short grace period even right at the deadline.
    kill = _pump(
        _recover_child,
        (project, str(run_dir), run_id),
        max(deadline, time.monotonic() + grace_s),
        on_event,
        None,
        grace_s,
    )
    if "sizes" not in recovered:
        reason = f"recovery stopped: {kill}" if kill else "recovery failed"
        result.error = "; ".join(x for x in (result.error, reason) if x)
        return
    stats = ReductionStats(sizes_before=result.sizes_before)
    stats.sizes_after = recovered["sizes"]
    stats.evaluations = result.evaluations_started
    stats.stop_reason = StopReason.KILLED
    stats.minimality = (
        Minimality.NOT_VERIFIED if project.settings.minimality else Minimality.SKIPPED
    )
    stats.minimality_detail = f"run stopped: {result.killed_reason or result.error}"
    stats.confirmed = True
    result.stats = stats
    result.recovered = True
    result.exported = True
