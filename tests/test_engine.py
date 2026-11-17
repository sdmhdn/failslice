from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from failslice.config import ConfigError, Project
from failslice.engine import Candidate, Engine, q
from failslice.predicate import OutcomeKind
from failslice.runspec import RunSpec
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables, make_project
from tests.reference import Schema, greatest_intact, kept_sets, schemas

ALWAYS = {"sql": "SELECT true"}


def test_load_assigns_row_ids_in_file_order(project_factory: Callable[..., Project]) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(30,), (10,), (20,)])}, predicate=ALWAYS)
    e = Engine(p)
    try:
        rows = e.con.execute('SELECT __fs_rid, x FROM __fs_base."t" ORDER BY __fs_rid').fetchall()
        assert rows == [(0, 30), (1, 10), (2, 20)]
        assert e.tables["t"].columns == (("x", "BIGINT"),)
        assert e.tables["t"].row_count == 3
        assert len(e.tables["t"].source_sha256) == 64
        assert e.sizes == {"t": 3} and e.total == 3
        assert e.kept_rids() == {"t": [0, 1, 2]}
    finally:
        e.close()


def test_csv_input(tmp_path: Path) -> None:
    p = make_project(
        tmp_path,
        {"t": ([("x", "BIGINT"), ("s", "VARCHAR")], [(1, "a")])},
        predicate=ALWAYS,
        suffix=".csv",
    )
    e = Engine(p)
    try:
        assert e.tables["t"].row_count == 1
    finally:
        e.close()


def test_external_access_is_disabled_after_load(project_factory: Callable[..., Project]) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate=ALWAYS)
    e = Engine(p)
    try:
        with pytest.raises(duckdb.PermissionException):
            e.con.execute(f"SELECT * FROM read_parquet('{p.table_path('t')}')").fetchall()
        with pytest.raises(duckdb.Error):
            e.con.execute("SET enable_external_access = true")
    finally:
        e.close()


def test_load_errors(tmp_path: Path) -> None:
    p = make_project(tmp_path / "a", {"t": ([("__fs_x", "BIGINT")], [(1,)])}, predicate=ALWAYS)
    with pytest.raises(ConfigError, match="reserved prefix"):
        Engine(p)
    p = make_project(
        tmp_path / "b",
        {"a": ([("k", "BIGINT")], [(1,)]), "b": ([("x", "BIGINT")], [(1,)])},
        fks=["b.nope -> a.k"],
        predicate=ALWAYS,
    )
    with pytest.raises(ConfigError, match="no column 'nope'"):
        Engine(p)
    p = make_project(
        tmp_path / "c",
        {"a": ([("k", "DATE")], [("2024-01-01",)]), "b": ([("x", "BLOB")], [(b"\x00",)])},
        fks=["b.x -> a.k"],
        predicate=ALWAYS,
    )
    with pytest.raises(ConfigError, match="cannot be compared"):
        Engine(p)
    (tmp_path / "c" / "a.parquet").unlink()
    with pytest.raises(ConfigError, match="file not found"):
        Engine(p)


def test_duckdb_input_tables_and_views(tmp_path: Path) -> None:
    db = tmp_path / "src.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE TABLE orders AS SELECT range AS id FROM range(4)")
    con.execute("CREATE VIEW v AS SELECT * FROM orders")
    con.close()
    cfg = "tables:\n  orders: {path: src.duckdb}\npredicate: {sql: SELECT true}\n"
    (tmp_path / "f.yaml").write_text(cfg)
    from failslice.config import load_project

    e = Engine(load_project(tmp_path / "f.yaml"))
    assert e.sizes == {"orders": 4}
    e.close()
    for source, match in (("v", "is a view"), ("missing", "no table")):
        (tmp_path / "g.yaml").write_text(
            f"tables:\n  orders: {{path: src.duckdb, source_table: {source}}}\n"
            "predicate: {sql: SELECT true}\n"
        )
        with pytest.raises(ConfigError, match=match):
            Engine(load_project(tmp_path / "g.yaml"))


def test_orphans_are_detected_and_exempt(project_factory: Callable[..., Project]) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT")], [(1,), (2,)]),
            "c": ([("id", "BIGINT"), ("pk", "BIGINT")], [(0, 1), (1, 99), (2, None), (3, 2)]),
        },
        fks=["c.pk -> p.k"],
        predicate=ALWAYS,
    )
    e = Engine(p)
    try:
        assert e.orphan_counts == {"c.pk -> p.k": 1}
        # Delete both parents: only the supported children cascade away; the input
        # orphan (pk=99) and the NULL reference survive.
        res = e.attempt(Candidate("p", ((0, 1),)))
        assert res.accepted
        kept = e.kept_rids()
        pk = dict(e.con.execute('SELECT __fs_rid, pk FROM __fs_base."c"').fetchall())
        assert {pk[r] for r in kept["c"]} == {99, None}
        assert e.sizes == {"p": 0, "c": 2}
    finally:
        e.close()


def test_duplicate_parent_keys_do_not_cascade_while_one_survives(
    project_factory: Callable[..., Project],
) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT"), ("tag", "VARCHAR")], [(1, "a"), (1, "b")]),
            "c": ([("pk", "BIGINT")], [(1,), (1,)]),
        },
        fks=["c.pk -> p.k"],
        predicate=ALWAYS,
    )
    e = Engine(p)
    try:
        assert e.attempt(Candidate("p", ((0, 0),))).accepted
        assert e.sizes == {"p": 1, "c": 2}
        assert e.attempt(Candidate("p", ((0, 0),))).accepted
        assert e.sizes == {"p": 0, "c": 0}
    finally:
        e.close()


def test_rejected_candidates_roll_back_and_are_cached(
    project_factory: Callable[..., Project],
) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    e = Engine(p)
    try:
        before = e.kept_rids()
        assert e.evaluate_current("plain").is_true
        assert e.evaluate_current("view").is_true
        # Deleting all orders cascades all lines: predicate FALSE, nothing changes.
        cand = Candidate("orders", ((0, e.sizes["orders"] - 1),))
        res = e.attempt(cand)
        assert not res.accepted and res.outcome.kind is OutcomeKind.FALSE and not res.cached
        assert e.kept_rids() == before
        again = e.attempt(cand)
        assert again.cached and not again.accepted and e.cache_hits == 1
        fresh = e.attempt(cand, use_cache=False)
        assert not fresh.cached
        # The search path is restored after every attempt.
        assert e.con.execute("SELECT current_setting('search_path')").fetchone() == ("main",)
    finally:
        e.close()


def test_errors_and_timeouts_roll_back(project_factory: Callable[..., Project]) -> None:
    pred = {"sql": "SELECT CAST(s AS INTEGER) > 0 FROM t WHERE k = 0"}
    p = project_factory(
        {"t": ([("k", "BIGINT"), ("s", "VARCHAR")], [(0, "1"), (1, "x")])}, predicate=pred
    )
    e = Engine(p)
    try:
        res = e.attempt(Candidate("t", ((0, 1),)))  # deleting both -> 0 rows -> shape error
        assert res.outcome.kind is OutcomeKind.ERROR and not res.accepted
        assert e.sizes == {"t": 2}
        assert (
            e.attempt(Candidate("t", ((1, 1),))).accepted
            or e.attempt(Candidate("t", ((0, 0),))).accepted
        )
    finally:
        e.close()
    slow = {"sql": "SELECT count(*) > 0 FROM t, range(100000000000)"}
    p2 = project_factory(
        {"t": ([("k", "BIGINT")], [(0,), (1,)])}, predicate=slow, settings={"eval_timeout_s": 0.2}
    )
    e2 = Engine(p2)
    try:
        res = e2.attempt(Candidate("t", ((0, 0),)))
        assert res.outcome.kind is OutcomeKind.TIMEOUT
        assert not e2.attempt(Candidate("t", ((0, 0),))).cached  # TIMEOUT is never cached
        assert e2.sizes == {"t": 2}
    finally:
        e2.close()


def test_reset_state_validates(project_factory: Callable[..., Project]) -> None:
    p = project_factory(
        {"p": ([("k", "BIGINT")], [(1,)]), "c": ([("pk", "BIGINT")], [(1,)])},
        fks=["c.pk -> p.k"],
        predicate=ALWAYS,
    )
    e = Engine(p)
    try:
        e.reset_state({"p": [0], "c": [0]})
        assert e.sizes == {"p": 1, "c": 1}
        with pytest.raises(ValueError, match="not intact"):
            e.reset_state({"p": [], "c": [0]})
        with pytest.raises(ValueError, match="out of range"):
            e.reset_state({"p": [5], "c": []})
        with pytest.raises(KeyError):
            e.attempt(Candidate("nope", ((0, 0),)))
    finally:
        e.close()


def test_seed_changes_order_but_not_content(project_factory: Callable[..., Project]) -> None:
    tables = {"t": ([("x", "BIGINT")], [(i,) for i in range(50)])}
    orders = []
    for seed in (0, 0, 1):
        p = project_factory(tables, predicate=ALWAYS, settings={"seed": seed})
        e = Engine(p)
        orders.append(
            [
                r[0]
                for r in e.con.execute('SELECT rid FROM __fs_state."cur_t" ORDER BY pos').fetchall()
            ]
        )
        e.close()
    assert orders[0] == orders[1] != orders[2]
    assert sorted(orders[2]) == list(range(50))


def test_export_writes_replay_files(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)
    out = tmp_path / "run"
    e = Engine(p, allowed_directories=[out])
    try:
        e.reset_state({"customers": [7], "orders": [7, 120], "lines": [7]})
        spec = e.export(out)
    finally:
        e.close()
    assert set(spec.tables) == {"customers", "orders", "lines"}
    assert spec.tables["orders"].rows == 2 and not spec.warnings
    assert spec.inputs["table:orders"].rows == 121
    assert RunSpec.read(out) == spec
    assert json.loads((out / "spec.json").read_text())["foreign_keys"] == list(FANOUT_FKS)
    con = duckdb.connect()
    rowmap = con.execute(
        f"SELECT table_name, row_index, source_rid FROM read_parquet('{out / 'rowmap.parquet'}') "
        "ORDER BY ALL"
    ).fetchall()
    assert rowmap == [("customers", 0, 7), ("lines", 0, 7), ("orders", 0, 7), ("orders", 1, 120)]
    assert con.execute(f"SELECT count(*) FROM '{out / 'orphans.parquet'}'").fetchone() == (0,)
    assert con.execute(f"SELECT * FROM '{out / 'tables' / 'orders.parquet'}'").fetchall() == [
        (7, 7),
        (7, 7),
    ]


def test_export_outside_allowed_directory_is_refused(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate=ALWAYS)
    e = Engine(p, allowed_directories=[tmp_path / "allowed"])
    try:
        with pytest.raises(duckdb.PermissionException):
            e.export(tmp_path / "elsewhere")
    finally:
        e.close()


def test_quote() -> None:
    assert q('a"b') == '"a""b"'


@settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(schema=schemas(), data=st.data())
def test_cascade_matches_reference(
    tmp_path_factory: Any, schema: Schema, data: st.DataObject
) -> None:
    """Engine repair == pure-Python greatest intact subset, for random deletions."""
    base = tmp_path_factory.mktemp("cascade")
    e = Engine(schema.project(base, "SELECT true"))
    try:
        for _ in range(3):
            table = data.draw(st.sampled_from(schema.tables))
            n = e.sizes[table]
            if n == 0:
                continue
            lo = data.draw(st.integers(0, n - 1))
            hi = data.draw(st.integers(lo, n - 1))
            pos_to_rid = dict(
                e.con.execute(f'SELECT pos, rid FROM __fs_state."cur_{table}"').fetchall()
            )
            current = kept_sets(e.kept_rids())
            removed = {pos_to_rid[i] for i in range(lo, hi + 1)}
            target = {t: (s - removed if t == table else set(s)) for t, s in current.items()}
            expected = greatest_intact(schema, target)
            assert e.attempt(Candidate(table, ((lo, hi),))).accepted
            assert kept_sets(e.kept_rids()) == expected
    finally:
        e.close()
