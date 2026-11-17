"""Run supervisor: reduction in a child process with checkpoints and hard deadlines.

ADR 0005. The child process loads the data, reduces, and writes an atomic checkpoint
(kept row ids per table) once the full data is confirmed TRUE and after every accepted
configuration. Before every predicate execution it announces the evaluation over a pipe
(a synchronous send), so the parent's evaluation count is exact even if the child is
killed. The parent kills the child when the deadline passes or a single evaluation
overruns ``eval_timeout_s + grace``. Recovery (re-loading the original files and
exporting the last checkpoint) also runs in a child process under the same deadline.

The parent closes its copy of the pipe's write end, so a child that dies mid-message
produces EOF. Messages are received on a reader thread, so a child that stalls
mid-message cannot block the deadline loop, and user progress callbacks run on their
own thread (latest event only), so a slow callback cannot delay a kill.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import queue
import threading
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
PROGRESS_DRAIN_S = 0.5
"""How long a finished run waits for a slow progress callback before abandoning it."""


def write_checkpoint(
    engine: Engine, run_dir: Path, run_id: str, stats: ReductionStats | None = None
) -> None:
    """Atomically write the engine's current configuration (and the statistics as of
    that configuration, so recovery reports matching numbers) to ``run_dir``."""
    data = {
        "run_id": run_id,
        "inputs": engine.input_identity(),
        "kept": engine.kept_rids(),
        "sizes": engine.sizes,
        "stats": _stats_to_dict(stats) if stats is not None else None,
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
            write_checkpoint(eng, rd, run_id, reducer.snapshot())
            send(("checkpoint", None))

        reducer = Reducer(
            engine,
            budget,
            on_progress=lambda ev: send(("progress", asdict(ev))),
            before_eval=lambda: send(("eval_start", None)),
            on_accept=lambda eng: write_checkpoint(eng, rd, run_id, reducer.snapshot()),
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
            conn.send(("recovered", (engine.sizes, checkpoint.get("stats"))))
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
    unconfirmed_stats: ReductionStats | None = None
    """Statistics of a run that stopped before any configuration was confirmed TRUE."""
    last_progress: ProgressEvent | None = None


class _ProgressDispatcher:
    """Delivers progress events to a user callback on its own thread, so a slow callback
    never delays supervision. Events carry cumulative state, so only the latest pending
    event is kept (bounded memory); intermediate events may be skipped."""

    def __init__(self, callback: Callable[[ProgressEvent], None]) -> None:
        self._callback = callback
        self._cond = threading.Condition()
        self._pending: ProgressEvent | None = None
        self._closed = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def post(self, event: ProgressEvent) -> None:
        with self._cond:
            self._pending = event
            self._cond.notify()

    def _run(self) -> None:
        while True:
            with self._cond:
                while self._pending is None and not self._closed:
                    self._cond.wait()
                event, self._pending = self._pending, None
                if event is None:
                    return
            with contextlib.suppress(Exception):  # a failing callback must not stop the run
                self._callback(event)

    def close(self, timeout_s: float) -> None:
        """Deliver the last pending event if the callback keeps up within ``timeout_s``.

        A callback that is still blocked after that is abandoned: its (daemon) thread ends
        when the callback returns, and it receives no further events. Python cannot stop
        a running callback, so callers should keep callbacks non-blocking."""
        with self._cond:
            self._closed = True
            self._cond.notify()
        self._thread.join(timeout_s)


_EOF = object()


def _reader(conn: Connection, events: queue.SimpleQueue[Any]) -> None:
    """Move complete messages from the pipe to ``events``; a partial frame blocks only
    this thread (the child's death then ends it with EOF)."""
    try:
        while True:
            events.put(conn.recv())
    except (EOFError, OSError):
        pass
    finally:
        events.put(_EOF)


def _pump(
    target: Callable[..., None],
    args: tuple[Any, ...],
    deadline: float,
    on_event: Callable[[str, Any], bool],
    eval_limit: float | None,
    grace_s: float,
) -> str | None:
    """Run ``target`` in a spawned child, feeding events to ``on_event`` (which returns
    True when the child has reported a final result). Returns a kill reason, or None.

    The deadline loop never blocks on the pipe: a reader thread receives messages, so a
    child that stalls mid-message is still killed on time. ``on_event`` must not block.
    """
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=target, args=(*args, child_conn), daemon=True)
    proc.start()
    child_conn.close()  # only the child may write; its death then yields EOF here
    events: queue.SimpleQueue[Any] = queue.SimpleQueue()
    reader = threading.Thread(target=_reader, args=(parent_conn, events), daemon=True)
    reader.start()
    eval_started: list[float | None] = [None]
    kill_reason: str | None = None
    eof = False

    def handle(kind: str, payload: Any) -> bool:
        if kind == "eval_start":
            eval_started[0] = time.monotonic()
        elif kind == "progress":
            eval_started[0] = None
        return on_event(kind, payload)

    finished = False
    try:
        while not finished and not eof:
            now = time.monotonic()
            if now > deadline:
                kill_reason = "deadline exceeded"
                break
            started = eval_started[0]
            if eval_limit is not None and started is not None and now - started > eval_limit:
                kill_reason = f"engine unresponsive: one evaluation exceeded {eval_limit:g}s"
                break
            try:
                item = events.get(timeout=min(0.2, max(0.001, deadline - now)))
            except queue.Empty:
                continue
            if item is _EOF:
                eof = True  # child exited (possibly mid-message)
            else:
                finished = handle(*item)
    finally:
        if proc.is_alive() and not finished:
            proc.kill()
        proc.join(timeout=grace_s)
        if proc.is_alive():  # pragma: no cover - last resort
            proc.kill()
            proc.join()
        # The child is gone, so the reader sees EOF; drain the complete messages it
        # received before that (exact counts).
        reader.join(timeout=grace_s)
        while not finished:
            try:
                item = events.get_nowait()
            except queue.Empty:
                break
            if item is _EOF:
                break
            finished = handle(*item)
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
    dispatcher = _ProgressDispatcher(on_progress) if on_progress is not None else None

    def on_reduce_event(kind: str, payload: Any) -> bool:
        if kind == "loaded":
            result.sizes_before = dict(payload)
        elif kind == "eval_start":
            result.evaluations_started += 1
        elif kind == "progress":
            event = ProgressEvent(**payload)
            result.last_progress = event
            if dispatcher is not None:
                dispatcher.post(event)
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
            result.unconfirmed_stats = stats_from_dict(payload)
            return True
        elif kind == "done":
            result.stats = stats_from_dict(payload)
            result.exported = True
            return True
        return False

    try:
        result.killed_reason = _pump(
            _reduce_child,
            (project, str(run_dir), run_id, budget),
            deadline,
            on_reduce_event,
            project.settings.eval_timeout_s + grace_s,
            grace_s,
        )
    finally:
        if dispatcher is not None:
            dispatcher.close(min(grace_s, PROGRESS_DRAIN_S))
    if (
        result.stats is None
        and result.determinism_error is None
        and result.unconfirmed_stats is None
    ):
        _recover(project, run_dir, run_id, result, deadline, grace_s)
    if result.stats is None and result.unconfirmed_stats is None and result.sizes_before:
        # Stopped (killed or crashed) before any confirmed configuration: keep what the
        # parent observed. Completed evaluations come from the last progress event.
        lost = ReductionStats(sizes_before=dict(result.sizes_before))
        lost.sizes_after = dict(result.sizes_before)
        lost.evaluations = result.last_progress.evaluations if result.last_progress else 0
        lost.stop_reason = StopReason.KILLED if result.killed_reason else lost.stop_reason
        lost.minimality_detail = f"run stopped: {result.killed_reason or result.error}"
        result.unconfirmed_stats = lost
    for stats in (result.stats, result.unconfirmed_stats):
        if stats is not None:
            stats.evaluations_started = result.evaluations_started
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
            sizes, stats = payload
            recovered["sizes"] = dict(sizes)
            recovered["stats"] = stats
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
    if recovered.get("stats") is not None:
        # Numbers as of the recovered configuration (written atomically with it).
        stats = stats_from_dict(recovered["stats"])
    else:  # pragma: no cover - checkpoints written by this version always carry stats
        stats = ReductionStats(sizes_before=result.sizes_before)
    stats.sizes_after = recovered["sizes"]
    stats.stop_reason = StopReason.KILLED
    stats.minimality = (
        Minimality.NOT_VERIFIED if project.settings.minimality else Minimality.SKIPPED
    )
    stats.minimality_detail = f"run stopped: {result.killed_reason or result.error}"
    stats.confirmed = True
    result.stats = stats
    result.recovered = True
    result.exported = True
