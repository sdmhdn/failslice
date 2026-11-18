"""Cancellation (ADR 0011): the caller's event stops reduce, check and replay; a cancelled
run is never recovered or verified and records the cancellation."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path

import failslice
from failslice.config import Project
from failslice.reducer import StopReason
from failslice.verify import verify_in_subprocess
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables

# A valid but very slow predicate: a four-way cross join of 400 lines (2.56e10 rows).
SLOW = {"sql": "SELECT count(*) > 0 FROM lines a, lines b, lines c, lines d"}


def _slow(factory: Callable[..., Project]) -> Project:
    return factory(
        fanout_tables(),
        fks=FANOUT_FKS,
        predicate=SLOW,
        settings={"eval_timeout_s": 120, "wall_clock_s": 300},
    )


def test_isolated_reduce_is_cancelled(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    cancel = threading.Event()
    threading.Timer(3.0, cancel.set).start()
    start = time.monotonic()
    res = failslice.reduce(
        _slow(project_factory), out_dir=tmp_path / "run", isolate=True, cancel=cancel
    )
    assert time.monotonic() - start < 30  # killed, not waited out
    assert res.status == "cancelled" and res.failure == "cancelled"
    assert res.verification is None and res.minimality_label == "no reproducer"
    m = res.manifest
    assert m is not None and m.status == "cancelled" and m.failure == "cancelled"
    assert {s.name: s.status for s in m.steps} == {
        "csv": "skipped",
        "pytest": "skipped",
        "explain": "skipped",
        "report": "done",
    }
    assert not (res.run_dir / "pytest").exists()  # nothing recovered or exported
    assert "**Cancelled.**" in (res.run_dir / "report.md").read_text(encoding="utf-8")


def test_in_process_reduce_is_cancelled(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    cancel = threading.Event()
    cancel.set()
    res = failslice.reduce(p, out_dir=tmp_path / "run", cancel=cancel)
    assert res.status == "cancelled"
    assert res.stats is not None and res.stats.stop_reason is StopReason.CANCELLED
    assert not (res.run_dir / "spec.json").exists()


def test_check_is_cancelled(project_factory: Callable[..., Project]) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    cancel = threading.Event()
    cancel.set()
    assert failslice.check(p, cancel=cancel).stop == "cancelled"
    assert failslice.check(p, isolate=True, cancel=cancel).stop == "cancelled"


def test_replay_is_cancelled(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    cancel = threading.Event()
    cancel.set()
    start = time.monotonic()
    report = verify_in_subprocess(res.run_dir, timeout_s=60, cancel=cancel)
    assert report.status == "cancelled" and time.monotonic() - start < 10


def test_cancelled_reduce_exits_130(project_factory: Callable[..., Project], monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from failslice.api import ReductionResult
    from failslice.cli import _reduce_exit

    r = ReductionResult(Path("x"), "cancelled", None, None, [], failure="cancelled")
    assert _reduce_exit(r) == ("cancelled", "interrupted")
