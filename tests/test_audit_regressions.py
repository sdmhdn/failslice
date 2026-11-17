"""Regression tests for the independent audit after Phase 2 (one block per finding)."""

from __future__ import annotations

import dataclasses
import decimal
import json
import os
import struct
import threading
import time
from collections.abc import Callable
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import duckdb
import pytest

import failslice
from failslice import api
from failslice.config import ConfigError, ExpectPredicate, Project, SqlPredicate, parse_project
from failslice.engine import Engine
from failslice.predicate import OutcomeKind, PredicateRunner, results_equal
from failslice.reducer import Budget, Minimality, ProgressEvent, Reducer
from failslice.runspec import RunSpec
from failslice.safety import PredicateValidationError, validate_predicate_sql
from failslice.supervisor import _pump, read_checkpoint, run_supervised
from failslice.verify import verify_run_dir
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables, posix_only


# Finding 1: numbers compare exactly (no float rounding), nested values are typed.
@pytest.mark.parametrize(
    ("actual", "expected", "tol", "equal"),
    [
        (2**100 + 1, 2**100, 0.0, False),
        (2**100 + 1, 2**100, None, False),
        (10**30 + 1, 0, 1e30, False),  # exact arithmetic even for huge differences
        (10**30, 0, 1e30, True),
        (decimal.Decimal("0.1"), 0.1, None, True),
        (float("inf"), float("inf"), 1.0, True),
        (float("inf"), 1e308, 1e308, False),
        (float("nan"), float("nan"), None, True),
        (decimal.Decimal("NaN"), float("nan"), 0.5, True),
        ([True], [1], None, False),
        ({"a": [1, True]}, {"a": [1, 1]}, None, False),
        ({"a": [1, 2.0]}, {"a": [1, 2]}, None, True),
        ([1, 2], [1], None, False),
    ],
)
def test_exact_typed_comparison(actual: Any, expected: Any, tol: float | None, equal: bool) -> None:
    assert results_equal([(actual,)], [(expected,)], ordered=False, tol=tol) is equal
    assert results_equal([(actual,)], [(expected,)], ordered=True, tol=tol) is equal


def test_big_integer_expectation_keeps_the_right_row(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    big = 2**62  # exceeds float precision once +1 is added
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(big + 1,), (big,)])},
        predicate={
            "expect": {
                "query": "SELECT max(x) FROM t",
                "expected": big + 1,
                "mode": "equals",
                "float_tolerance": 0,
            }
        },
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok
    rows = (
        duckdb.connect()
        .execute(f"SELECT x FROM read_parquet('{tmp_path / 'run' / 'tables' / 't.parquet'}')")
        .fetchall()
    )
    assert rows == [(big + 1,)]


# Finding 2: DuckDB readers expand globs, so glob characters in paths are refused.
@posix_only
def test_glob_paths_are_refused(tmp_path: Path) -> None:
    safe = tmp_path / "safe"
    outside = tmp_path / "outside"
    safe.mkdir()
    outside.mkdir()
    (safe / "t*.csv").write_text("id\n1\n")
    (outside / "secret.csv").write_text("id\n999\n")
    (safe / "t-secret.csv").symlink_to(outside / "secret.csv")
    config = {"tables": {"t": {"path": "t*.csv"}}, "predicate": {"sql": "SELECT true"}}
    with pytest.raises(ConfigError, match="glob character"):
        parse_project(config, base_dir=safe)
    weird = tmp_path / "dir[1]"
    weird.mkdir()
    (weird / "t.csv").write_text("id\n1\n")
    config["tables"] = {"t": {"path": "t.csv"}}
    with pytest.raises(ConfigError, match="glob character"):
        parse_project(config, base_dir=weird)


def _fanout_run(project_factory: Callable[..., Project], tmp_path: Path) -> Path:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    out = tmp_path / "run"
    e = Engine(p, allowed_directories=[out])
    try:
        e.reset_state({"customers": [7], "orders": [7, 120], "lines": [7]})
        e.export(out)
    finally:
        e.close()
    assert verify_run_dir(out).ok
    return out


def test_replay_refuses_symlinked_orphans_file(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    out = _fanout_run(project_factory, tmp_path)
    elsewhere = tmp_path / "elsewhere.parquet"
    (out / "orphans.parquet").rename(elsewhere)
    (out / "orphans.parquet").symlink_to(elsewhere)
    report = verify_run_dir(out)
    assert report.status == "replay-failed" and "outside the run directory" in report.detail


# Finding 3: a watchdog that fired always yields TIMEOUT (never TRUE).
def test_fired_watchdog_is_timeout_even_if_the_query_completed() -> None:
    con = duckdb.connect()
    runner = PredicateRunner(SqlPredicate(sql="SELECT true"))
    # A zero timeout fires before or during the query; either way it is a TIMEOUT or
    # (if the interrupt landed first) an interrupted query: never TRUE.
    outcomes = {runner.run(con, 1e-9).kind for _ in range(20)}
    assert OutcomeKind.TRUE not in outcomes


# Finding 4: large tolerant comparisons neither recurse nor crash; errors become ERROR.
def test_large_tolerant_matching_is_iterative() -> None:
    assert results_equal([(1.0,)] * 3000, [(1.0,)] * 3000, ordered=False, tol=0.1)
    # A chain: actual i matches expected i-1 and i. In this order the greedy start leaves
    # actual 0 unmatched and its augmenting path is n long (deeper than Python's
    # recursion limit for a recursive matcher).
    n = 1200
    actual = [(float(i),) for i in reversed(range(n))]
    expected = [(i + 0.5,) for i in range(n)]
    assert results_equal(actual, expected, ordered=False, tol=0.5)
    assert not results_equal(actual, [*expected[:-1], (n + 5.0,)], ordered=False, tol=0.5)


def test_tolerant_matching_needs_no_exact_pairs() -> None:
    # Cancelling the common row (1) first would leave 0 vs 2 unmatched.
    assert results_equal([(0,), (1,)], [(1,), (2,)], ordered=False, tol=1.0)


def test_comparison_failure_is_an_error_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    from failslice import portable as predicate_mod

    def boom(*_a: Any, **_k: Any) -> bool:
        raise RuntimeError("cannot compare")

    monkeypatch.setattr(predicate_mod, "results_equal", boom)
    runner = PredicateRunner(ExpectPredicate(query="SELECT 1", expected=[[1]]))
    outcome = runner.run(duckdb.connect(), 10)
    assert outcome.kind is OutcomeKind.ERROR and outcome.error_class == "comparison"


# Finding 5: malformed orphan exemptions cannot hide integrity violations.
@pytest.mark.parametrize("bad", [(0, None), (5, 0), (0, 99), (None, 0)])
def test_malformed_orphan_metadata_is_rejected(
    project_factory: Callable[..., Project], tmp_path: Path, bad: tuple[Any, Any]
) -> None:
    out = _fanout_run(project_factory, tmp_path)
    con = duckdb.connect()
    edge, row = bad
    con.execute(
        f"COPY (SELECT {edge if edge is not None else 'NULL'}::INTEGER AS edge, "
        f"{row if row is not None else 'NULL'}::BIGINT AS row_index) "
        f"TO '{out / 'orphans.parquet'}' (FORMAT parquet)"
    )
    report = verify_run_dir(out)
    assert report.status == "integrity-failed" and "malformed orphans" in report.detail


# Finding 6: short wall-clock budgets still leave time to reduce.
@pytest.mark.parametrize("wall", [2.0, 5.0, 10.0, 100.0, 1000.0])
def test_replay_reserve_is_bounded(wall: float) -> None:
    reserve = api.replay_reserve(wall)
    assert 0 < reserve <= wall / 2
    assert reserve == pytest.approx(max(10.0, 0.1 * wall)) or reserve == wall / 2


def test_short_isolated_budget_reduces(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(i,) for i in range(5)])},
        predicate={"sql": "SELECT count(*) > 0 FROM t WHERE x = 3"},
        settings={"wall_clock_s": 8},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run", isolate=True)
    assert res.status == "reduced", res.notes
    assert res.stats is not None and res.stats.sizes_after == {"t": 1}  # really reduced


# Finding 7: a slow progress callback cannot delay the hard deadline.
def test_slow_progress_callback_does_not_delay_kill(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    from tests.test_verify_and_api import SLOW_WHEN_SMALL

    p = project_factory(
        fanout_tables(),
        fks=FANOUT_FKS,
        predicate=SLOW_WHEN_SMALL,
        settings={"eval_timeout_s": 60},
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    seen: list[ProgressEvent] = []

    def slow(ev: ProgressEvent) -> None:
        seen.append(ev)
        time.sleep(30)

    start = time.monotonic()
    sup = run_supervised(
        p,
        run_dir,
        "rid",
        Budget(max_evals=100_000, wall_clock_s=600),
        deadline=time.monotonic() + 5,
        on_progress=slow,
        grace_s=1,
    )
    elapsed = time.monotonic() - start
    assert sup.killed_reason == "deadline exceeded"  # the kill really happened ...
    assert elapsed < 15, elapsed  # ... on time, although the callback sleeps 30 s
    assert len(seen) == 1  # the callback ran on its own thread and was then abandoned


def _stalled_frame_child(conn: Connection) -> None:
    # Announce a 1000-byte frame, send 3 bytes, then stay alive without finishing it.
    os.write(conn.fileno(), struct.pack("!i", 1000) + b"abc")
    time.sleep(600)


def test_stalled_live_writer_is_killed_on_deadline() -> None:
    start = time.monotonic()
    kill = _pump(_stalled_frame_child, (), time.monotonic() + 2, lambda k, v: False, None, 1.0)
    assert kill == "deadline exceeded"
    assert time.monotonic() - start < 15


# Finding 8: a file that changes while it is loaded is refused.
def test_input_changed_while_loading(
    project_factory: Callable[..., Project], monkeypatch: pytest.MonkeyPatch
) -> None:
    from failslice import engine as engine_mod

    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT true"})
    calls = iter(["a", "b"])
    monkeypatch.setattr(engine_mod, "file_sha256", lambda _p: next(calls))
    with pytest.raises(ConfigError, match="changed while it was being loaded"):
        Engine(p)


# Finding 9: unconfirmed and recovered runs keep their statistics.
@pytest.mark.parametrize("isolate", [False, True])
def test_unconfirmed_run_keeps_stats(
    project_factory: Callable[..., Project], tmp_path: Path, isolate: bool
) -> None:
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE, settings={"max_evals": 2}
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run", isolate=isolate)
    assert res.status == "no-candidate"
    assert res.minimality_label == "no reproducer"
    assert res.stats is not None and res.stats.evaluations == 2
    if isolate:
        assert res.stats.evaluations_started == 2
    data = json.loads((tmp_path / "run" / "manifest.json").read_text())
    assert data["status"] == "no-candidate" and data["stats"]["evaluations"] == 2


def test_checkpoint_carries_matching_stats(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.stats is not None
    cp = read_checkpoint(res.run_dir, json.loads("null") or _run_id(res.run_dir))
    assert cp is not None and cp["stats"] is not None
    assert cp["stats"]["sizes_after"] == cp["sizes"]
    assert cp["stats"]["accepted"] == res.stats.accepted


def _run_id(run_dir: Path) -> str:
    return str(json.loads((run_dir / "checkpoint.json").read_text())["run_id"])


# Finding 10: volatile functions are rejected, including aliases and built-in macros.
@pytest.mark.parametrize(
    "call",
    [
        "current_localtime()",
        "current_localtimestamp()",
        "current_query_id()",
        "pg_conf_load_time()",
        "current_schema()",
        "ago(INTERVAL 1 DAY)",
    ],
)
def test_volatile_aliases_are_rejected(call: str) -> None:
    with pytest.raises(PredicateValidationError) as info:
        validate_predicate_sql(f"SELECT {call} IS NOT NULL FROM t", ["t"])
    assert info.value.reason == "volatile_function"


@pytest.mark.parametrize("call", ["error('x')", "abs(-1)", "list_histogram([1, 2])"])
def test_deterministic_functions_stay_allowed(call: str) -> None:
    validate_predicate_sql(f"SELECT {call} IS NOT NULL FROM t", ["t"])


# Finding 11: ``expected: null`` is a valid scalar expectation.
def test_expected_null_scalar(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(1,), (None,), (3,)])},
        predicate={"expect": {"query": "SELECT max(x) FROM t", "expected": None}},
    )
    assert isinstance(p.predicate, ExpectPredicate) and p.predicate.inline
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok
    assert res.stats is not None and res.stats.output_rows == 1


def test_expect_needs_exactly_one_source(tmp_path: Path) -> None:
    base = {"tables": {"t": {"path": "t.csv"}}}
    for expect in (
        {"query": "SELECT 1"},
        {"query": "SELECT 1", "expected": 1, "expected_file": "e.csv"},
    ):
        with pytest.raises(ConfigError, match="exactly one"):
            parse_project({**base, "predicate": {"expect": expect}}, base_dir=tmp_path)


# Own finding: ENUM columns (no Parquet equivalent) replay with their source type.
def test_enum_columns_replay_with_source_type(tmp_path: Path) -> None:
    db = tmp_path / "data.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TYPE mood AS ENUM ('b', 'a')")
    con.execute(
        "CREATE TABLE t AS SELECT i::INTEGER AS id, "
        "(CASE WHEN i % 2 = 0 THEN 'a' ELSE 'b' END)::mood AS m FROM range(10) r(i)"
    )
    con.close()
    (tmp_path / "failslice.yaml").write_text(
        "tables:\n  t: {path: data.duckdb}\n"
        "predicate:\n"
        # enum_code() only works if the ENUM type survives the Parquet round trip.
        '  sql: "SELECT count(*) > 0 FROM t WHERE enum_code(m) = 0 AND id = 3"\n'
    )
    p = failslice.load_project(tmp_path / "failslice.yaml")
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok, res.verification
    assert res.spec is not None
    entry = res.spec.tables["t"]
    assert entry.columns[1] == ("m", "VARCHAR")
    assert entry.source_columns[1] == ("m", "ENUM('b', 'a')")
    assert not res.spec.warnings


# Architecture: the run directory has a typed, versioned contract.
def test_runspec_is_versioned_and_strict(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    out = _fanout_run(project_factory, tmp_path)
    spec = RunSpec.read(out)
    assert spec.format == 3 and spec.inputs["table:lines"].rows == 400
    assert spec.input_orphans == {str(fk): 0 for fk in FANOUT_FKS}
    assert spec.run_settings["seed"] == 0
    data = json.loads((out / "spec.json").read_text())
    data["format"] = 2
    (out / "spec.json").write_text(json.dumps(data))
    report = verify_run_dir(out)
    assert report.status == "replay-failed" and "unsupported spec format" in report.detail


def test_reducer_snapshot_is_a_copy(project_factory: Callable[..., Project]) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    e = Engine(p)
    try:
        r = Reducer(e, Budget(max_evals=10))
        r.check_determinism()
        snap = r.snapshot()
        snap.outcomes["TRUE"] += 100
        assert r.stats.outcomes["TRUE"] == 4
        assert snap.sizes_after == e.sizes and snap.minimality is Minimality.SKIPPED
    finally:
        e.close()


# Second review, finding 1: tampered type strings cannot inject SQL into replay.
def test_tampered_type_string_cannot_inject_sql(tmp_path: Path) -> None:
    db = tmp_path / "data.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TYPE mood AS ENUM ('b', 'a')")
    con.execute("CREATE TABLE t AS SELECT 3::INTEGER AS id, 'b'::mood AS m")
    con.close()
    (tmp_path / "failslice.yaml").write_text(
        "tables:\n  t: {path: data.duckdb}\npredicate:\n  sql: SELECT count(*) > 0 FROM t\n"
    )
    res = failslice.reduce(
        failslice.load_project(tmp_path / "failslice.yaml"), out_dir=tmp_path / "r"
    )
    assert res.verification is not None and res.verification.ok
    spec = json.loads((tmp_path / "r" / "spec.json").read_text())
    spec["tables"]["t"]["source_columns"][1][1] = (
        "BIGINT) AS m FROM __fs_raw.t; CREATE TABLE injected AS SELECT 42; SELECT CAST(1 AS BIGINT"
    )
    (tmp_path / "r" / "spec.json").write_text(json.dumps(spec))
    report = verify_run_dir(tmp_path / "r")
    assert report.status == "replay-failed" and "cannot restore" in report.detail


def test_restoring_select_only_emits_parsed_types() -> None:
    from failslice.runspec import restoring_select

    sql = restoring_select("src", [("m", "VARCHAR")], [("m", "BIGINT); DROP TABLE x; --")])
    assert "DROP" not in sql and ";" not in sql


# Second review, finding 2: fractional orphan coordinates are malformed.
def test_fractional_orphan_metadata_is_rejected(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    out = _fanout_run(project_factory, tmp_path)
    duckdb.connect().execute(
        "COPY (SELECT -0.1::DOUBLE AS edge, -0.1::DOUBLE AS row_index) "
        f"TO '{out / 'orphans.parquet'}' (FORMAT parquet)"
    )
    report = verify_run_dir(out)
    assert report.status == "integrity-failed" and "non-integer" in report.detail


# Second review, finding 3: a round trip that cannot be restored is a warning.
def test_unrestorable_round_trip_is_a_warning(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    from failslice import engine as engine_mod

    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT true"})
    out = tmp_path / "run"
    e = Engine(p, allowed_directories=[out])
    real = engine_mod.restoring_select
    try:
        engine_mod.restoring_select = lambda *a: real(*a) + " WHERE error('boom')"  # type: ignore[assignment]
        spec = e.export(out)
    finally:
        engine_mod.restoring_select = real  # type: ignore[assignment]
        e.close()
    assert len(spec.warnings) == 1 and "cannot restore" in spec.warnings[0]
    assert RunSpec.read(out).warnings == spec.warnings


# Second review, finding 4: MAP keys are typed (TRUE never equals 1).
def test_map_keys_are_typed() -> None:
    con = duckdb.connect()
    (value,) = con.execute("SELECT map([true], ['v'])").fetchone()  # type: ignore[misc]
    assert not results_equal([(value,)], [({1: "v"},)], ordered=True, tol=None)
    assert not results_equal([(value,)], [({1: "v"},)], ordered=False, tol=None)
    assert results_equal([(value,)], [({True: "v"},)], ordered=False, tol=None)


# Second review, finding 6: a run killed before any confirmation keeps its counts.
def test_killed_before_confirmation_keeps_stats(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    from tests.test_verify_and_api import SLOW_WHEN_SMALL

    slow_always = {"sql": SLOW_WHEN_SMALL["sql"].replace("^ 4", "^ 0 * 0 + 1e15")}
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=slow_always, settings={"eval_timeout_s": 60}
    )
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sup = run_supervised(p, run_dir, "rid", Budget(max_evals=100, wall_clock_s=3), grace_s=1)
    assert sup.killed_reason is not None and sup.stats is None
    stats = sup.unconfirmed_stats
    assert stats is not None and not stats.confirmed
    assert stats.evaluations_started == sup.evaluations_started >= 1
    assert stats.sizes_before == {"customers": 30, "orders": 121, "lines": 400}
    assert stats.elapsed_s > 1 and "per-outcome counts are unavailable" in (stats.minimality_detail)


def test_no_progress_delivered_after_close() -> None:
    from failslice.supervisor import _ProgressDispatcher

    got: list[int] = []
    release = threading.Event()

    def blocking(ev: ProgressEvent) -> None:
        got.append(ev.evaluations)
        release.wait(10)

    d = _ProgressDispatcher(blocking)
    ev = ProgressEvent("reduce", None, 1, 0, 1, 1, 0.0, False)
    d.post(ev)
    time.sleep(0.2)  # the first event is now inside the blocked callback
    d.post(dataclasses.replace(ev, evaluations=2))
    d.close(0.2)
    d.post(dataclasses.replace(ev, evaluations=3))
    release.set()
    time.sleep(0.3)
    assert got == [1]  # the pending and the late event were dropped


# Second review, finding 7: in-process runs write the initial checkpoint too.
def test_in_process_writes_initial_checkpoint(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(3,)])},
        predicate={"sql": "SELECT count(*) > 0 FROM t WHERE x = 3"},
        settings={"max_evals": 5},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.status == "reduced"
    assert (tmp_path / "run" / "checkpoint.json").is_file()
