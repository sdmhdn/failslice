"""Regression tests for the Phase 2 review findings (one test per finding)."""

from __future__ import annotations

import json
import os
import struct
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import duckdb
import pytest

import failslice
from failslice.config import ExpectPredicate, Project, SqlPredicate
from failslice.engine import Candidate, Engine
from failslice.predicate import OutcomeKind, PredicateRunner, results_equal
from failslice.reducer import Budget, Minimality, PredicateNotFailingError
from failslice.supervisor import _pump, run_supervised
from failslice.verify import verify_run_dir
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables, posix_only


# Finding 1 + 6: a user column named "rowid" must not corrupt row identity / replay.
def test_user_rowid_column(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT")], [(1,)]),
            "c": ([("rowid", "BIGINT"), ("fk", "BIGINT")], [(0, 1), (0, 99), (5, 1)]),
        },
        fks=["c.fk -> p.k"],
        predicate={"sql": "SELECT count(*) >= 1 FROM c WHERE fk = 99"},
    )
    e = Engine(p)
    try:
        rids = e.con.execute('SELECT __fs_rid, rowid, fk FROM __fs_base."c" ORDER BY 1').fetchall()
        assert rids == [(0, 0, 1), (1, 0, 99), (2, 5, 1)]
        assert e.tables["c"].columns == (("rowid", "BIGINT"), ("fk", "BIGINT"))
        assert e.orphan_counts == {"c.fk -> p.k": 1}
        assert e.attempt(Candidate("p", ((0, 0),))).accepted
        assert e.kept_rids()["c"] == [1]  # only the input orphan survives the cascade
    finally:
        e.close()
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None
    assert res.verification.ok, res.verification  # orphan exemption maps to row_index 0


# Finding 2: a predicate never TRUE is fatal even with allow_nondeterministic.
@pytest.mark.parametrize("isolate", [False, True])
def test_never_true_is_fatal_even_if_nondeterminism_allowed(
    project_factory: Callable[..., Project], tmp_path: Path, isolate: bool
) -> None:
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(1,)])},
        predicate={"sql": "SELECT false"},
        settings={"allow_nondeterministic": True},
    )
    with pytest.raises(PredicateNotFailingError):
        failslice.reduce(p, out_dir=tmp_path / "run", isolate=isolate)
    assert not (tmp_path / "run" / "tables").exists()


def _partial_frame_child(conn: Connection) -> None:
    # A frame header announcing 1000 bytes, then only 3 bytes, then death.
    os.write(conn.fileno(), struct.pack("!i", 1000) + b"abc")
    os._exit(0)


def _sleeping_child(conn: Connection) -> None:
    conn.send(("hello", None))
    time.sleep(600)


# Finding 3: a child dying mid-message must not hang the parent.
@posix_only  # writes raw bytes to the pipe's file descriptor (a HANDLE on Windows)
def test_partial_message_does_not_hang_parent() -> None:
    events: list[Any] = []
    start = time.monotonic()
    kill = _pump(
        _partial_frame_child,
        (),
        time.monotonic() + 60,
        lambda k, v: events.append(k) or False,
        None,
        1.0,
    )
    assert time.monotonic() - start < 30
    assert kill is None
    assert events == []


# Finding 4: the deadline is enforced on any supervised child (reduction or recovery).
def test_pump_enforces_deadline() -> None:
    events: list[Any] = []
    start = time.monotonic()
    kill = _pump(
        _sleeping_child,
        (),
        time.monotonic() + 3,
        lambda k, v: events.append(k) or False,
        None,
        1.0,
    )
    assert kill == "deadline exceeded"
    assert events == ["hello"]
    assert time.monotonic() - start < 20


# Finding 5: a budget stop before the full data was confirmed TRUE exports nothing.
def test_unconfirmed_run_is_no_candidate(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE, settings={"max_evals": 2}
    )
    for isolate in (False, True):
        out = tmp_path / f"run{isolate}"
        res = failslice.reduce(p, out_dir=out, isolate=isolate)
        assert res.status == "no-candidate"
        assert res.verification is None
        assert not (out / "tables").exists()
        assert json.loads((out / "manifest.json").read_text())["status"] == "no-candidate"


# Finding 8: replay refuses expectation paths outside the run directory.
def test_verify_rejects_expectation_outside_run_dir(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    query = "SELECT count(*) FROM lines"
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate={"expect": {"query": query, "expected": 0}}
    )
    out = tmp_path / "run"
    e = Engine(p, allowed_directories=[out])
    try:
        e.export(out)
    finally:
        e.close()
    spec = json.loads((out / "spec.json").read_text())
    spec["expected_file"] = "../outside.parquet"
    (out / "spec.json").write_text(json.dumps(spec))
    report = verify_run_dir(out)
    assert report.status == "replay-failed" and "outside the run directory" in report.detail


# Finding 9: supervised evaluation counts exclude cache hits and match the child.
def test_supervised_counts_match(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sup = run_supervised(p, run_dir, "rid", Budget(max_evals=1000, wall_clock_s=120))
    assert sup.stats is not None and sup.exported
    assert sup.stats.cache_hits > 0
    assert sup.evaluations_started == sup.stats.evaluations
    assert sup.stats.minimality is Minimality.VERIFIED


# Finding 10: a scalar expectation demands a 1x1 result.
def test_scalar_expectation_requires_one_by_one() -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE t(x BIGINT)")
    runner = PredicateRunner(ExpectPredicate(query="SELECT x FROM t", expected=0))
    out = runner.run(con, 10)
    assert out.kind is OutcomeKind.ERROR
    assert "1x1" in out.detail
    con.execute("INSERT INTO t VALUES (1), (2)")
    assert runner.run(con, 10).kind is OutcomeKind.ERROR
    rows = PredicateRunner(ExpectPredicate(query="SELECT x FROM t", expected=[[1]]))
    assert rows.run(con, 10).kind is OutcomeKind.TRUE  # list expectations have no shape rule


# Finding 11: tolerant bag comparison is order independent.
def test_tolerant_bag_matching_is_order_independent() -> None:
    expected = [(0.0,), (2.0,)]
    assert results_equal([(1.0,), (0.0,)], expected, ordered=False, tol=1.0)
    assert results_equal([(0.0,), (1.0,)], expected, ordered=False, tol=1.0)
    assert not results_equal([(1.0,), (1.0,)], [(0.0,), (5.0,)], ordered=False, tol=1.0)


class _FetchFails:
    """A connection whose fetch raises (errors can surface after execute)."""

    description = (("x", "BOOLEAN"),)

    def execute(self, sql: str) -> None:
        pass

    def fetchall(self) -> list[Any]:
        raise duckdb.ConversionException("Conversion Error: during fetch")

    def interrupt(self) -> None:  # pragma: no cover - not reached
        pass


# Finding 13: errors raised while fetching are classified, not leaked.
def test_fetch_time_errors_are_classified() -> None:
    out = PredicateRunner(SqlPredicate(sql="SELECT true")).run(_FetchFails(), 10)  # type: ignore[arg-type]
    assert out.kind is OutcomeKind.ERROR
    assert out.error_class == "ConversionException"
