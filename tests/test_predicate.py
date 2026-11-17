from __future__ import annotations

import datetime as dt
import decimal
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from failslice.config import ExpectPredicate, RaisesPredicate, SqlPredicate
from failslice.predicate import (
    OutcomeKind,
    PredicateRunner,
    normalize_expected,
    results_equal,
    rows_signature,
)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    c.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, '7x')) v(k, s)")
    yield c
    c.close()


def run_sql(con: duckdb.DuckDBPyConnection, sql: str, timeout: float = 10.0) -> Any:
    return PredicateRunner(SqlPredicate(sql=sql)).run(con, timeout)


@pytest.mark.parametrize(
    ("sql", "kind", "sig"),
    [
        ("SELECT count(*) = 3 FROM t", OutcomeKind.TRUE, "bool:true"),
        ("SELECT count(*) = 4 FROM t", OutcomeKind.FALSE, "bool:false"),
        ("SELECT sum(k) > 1 FROM t WHERE false", OutcomeKind.FALSE, "bool:null"),
        ("SELECT true, false", OutcomeKind.ERROR, "shape:expected 1 column, got 2"),
        ("SELECT 1", OutcomeKind.ERROR, "shape:expected a BOOLEAN column, got INTEGER"),
        ("SELECT NULL", OutcomeKind.ERROR, 'shape:expected a BOOLEAN column, got "NULL"'),
        ("SELECT true WHERE false", OutcomeKind.ERROR, "shape:expected exactly 1 row, got 0"),
        ("SELECT k > 0 FROM t", OutcomeKind.ERROR, "shape:expected exactly 1 row, got 3"),
    ],
)
def test_boolean_form(
    con: duckdb.DuckDBPyConnection, sql: str, kind: OutcomeKind, sig: str
) -> None:
    out = run_sql(con, sql)
    assert out.kind is kind
    assert out.signature == sig
    assert out.is_true == (kind is OutcomeKind.TRUE)


def test_boolean_form_exception_is_error(con: duckdb.DuckDBPyConnection) -> None:
    out = run_sql(con, "SELECT CAST(s AS INTEGER) > 0 FROM t")
    assert out.kind is OutcomeKind.ERROR
    assert out.error_class == "ConversionException"
    assert out.signature.startswith("error:ConversionException:")


def test_aggregate_conversion_error_is_error(con: duckdb.DuckDBPyConnection) -> None:
    out = run_sql(con, "SELECT bool_and(CAST(s AS INTEGER) > 0) FROM t")
    assert out.kind is OutcomeKind.ERROR


def test_timeout_interrupts(con: duckdb.DuckDBPyConnection) -> None:
    out = run_sql(con, "SELECT count(*) > 0 FROM range(100000000000) a", timeout=0.2)
    assert out.kind is OutcomeKind.TIMEOUT
    assert run_sql(con, "SELECT true").kind is OutcomeKind.TRUE  # connection still usable


def _raises(match: str, **kw: Any) -> PredicateRunner:
    return PredicateRunner(
        RaisesPredicate(query="SELECT CAST(s AS INTEGER) FROM t", match=match, **kw)
    )


def test_raises_form(con: duckdb.DuckDBPyConnection) -> None:
    out = _raises(r"Could not convert string '[a-z0-9]+' to INT32").run(con, 10)
    assert out.kind is OutcomeKind.TRUE and out.error_class == "ConversionException"
    assert _raises("convert", error_class="ConversionException").run(con, 10).is_true
    assert _raises("convert", error_class="DatabaseError").run(con, 10).is_true  # base class
    assert _raises("CONVERT", flags=("IGNORECASE",)).run(con, 10).is_true


def test_raises_different_error_is_error(con: duckdb.DuckDBPyConnection) -> None:
    out = _raises("something else").run(con, 10)
    assert out.kind is OutcomeKind.ERROR and "different error" in out.detail
    out = _raises("convert", error_class="BinderException").run(con, 10)
    assert out.kind is OutcomeKind.ERROR


def test_raises_success_is_false(con: duckdb.DuckDBPyConnection) -> None:
    runner = PredicateRunner(RaisesPredicate(query="SELECT 1", match="x"))
    assert runner.run(con, 10).kind is OutcomeKind.FALSE


def test_raises_timeout_overrides(con: duckdb.DuckDBPyConnection) -> None:
    runner = PredicateRunner(
        RaisesPredicate(query="SELECT count(*) FROM range(100000000000)", match=".*")
    )
    assert runner.run(con, 0.2).kind is OutcomeKind.TIMEOUT


def _expect(expected: Any = None, **kw: Any) -> PredicateRunner:
    spec = ExpectPredicate(query=kw.pop("query", "SELECT k, s FROM t"), expected=expected, **kw)
    return PredicateRunner(spec)


def test_expect_differs(con: duckdb.DuckDBPyConnection) -> None:
    rows = [[1, "a"], [2, "b"], [3, "7x"]]
    assert _expect(rows).run(con, 10).kind is OutcomeKind.FALSE
    assert _expect(list(reversed(rows))).run(con, 10).kind is OutcomeKind.FALSE  # bag
    assert _expect(list(reversed(rows)), ordered=True).run(con, 10).kind is OutcomeKind.TRUE
    assert _expect(rows[:2]).run(con, 10).kind is OutcomeKind.TRUE
    assert _expect([*rows, rows[0]]).run(con, 10).kind is OutcomeKind.TRUE  # duplicates count
    assert _expect(rows, mode="equals").run(con, 10).kind is OutcomeKind.TRUE


def test_expect_scalar_and_tolerance(con: duckdb.DuckDBPyConnection) -> None:
    q = "SELECT sum(k) / 3.0 FROM t"
    assert _expect(2, query=q).run(con, 10).kind is OutcomeKind.FALSE
    assert _expect(2.0000001, query=q).run(con, 10).kind is OutcomeKind.TRUE
    assert _expect(2.0000001, query=q, float_tolerance=1e-3).run(con, 10).kind is OutcomeKind.FALSE


def test_expect_query_error_is_error(con: duckdb.DuckDBPyConnection) -> None:
    out = _expect(1, query="SELECT CAST(s AS INTEGER) FROM t").run(con, 10)
    assert out.kind is OutcomeKind.ERROR


def test_expect_needs_rows_for_file() -> None:
    spec = ExpectPredicate(query="SELECT 1", expected_file="x.csv")
    with pytest.raises(ValueError, match="expected_rows"):
        PredicateRunner(spec)
    assert PredicateRunner(spec, [(1,)]).check.expected == ((1,),)
    assert PredicateRunner(SqlPredicate(sql="SELECT true")).query == "SELECT true"


def test_normalize_expected() -> None:
    assert normalize_expected(5) == [(5,)]
    assert normalize_expected([1, 2]) == [(1,), (2,)]
    assert normalize_expected([[1, "a"]]) == [(1, "a")]


@pytest.mark.parametrize(
    ("a", "b", "equal"),
    [
        ([(1,)], [(1.0,)], True),
        ([(decimal.Decimal("0.1"),)], [(0.1,)], True),
        ([(None,)], [(None,)], True),
        ([(None,)], [(0,)], False),
        ([(True,)], [(1,)], False),
        ([(True,)], [(True,)], True),
        ([("1",)], [(1,)], False),
        ([(dt.date(2024, 1, 1),)], [(dt.date(2024, 1, 1),)], True),
        ([(dt.date(2024, 1, 1),)], [(dt.datetime(2024, 1, 1),)], False),
        ([(float("nan"),)], [(float("nan"),)], True),
        ([(1, 2)], [(1,)], False),
    ],
)
def test_results_equal(a: Any, b: Any, equal: bool) -> None:
    assert results_equal(a, b, ordered=False, tol=None) is equal
    assert results_equal(a, b, ordered=True, tol=None) is equal


def test_results_equal_with_tolerance_as_bag() -> None:
    assert results_equal([(1.0,), (2.0,)], [(2.0001,), (0.9999,)], ordered=False, tol=1e-3)
    assert not results_equal([(1.0,), (2.0,)], [(2.1,), (1.0,)], ordered=False, tol=1e-3)
    assert results_equal([(float("nan"),)], [(float("nan"),)], ordered=False, tol=1e-3)


def test_rows_signature_bag_vs_ordered() -> None:
    a = [(1, "x"), (2, None)]
    b = list(reversed(a))
    assert rows_signature(a, ordered=False) == rows_signature(b, ordered=False)
    assert rows_signature(a, ordered=True) != rows_signature(b, ordered=True)
    assert rows_signature([(float("nan"), [1, {"k": 2}])], ordered=True).startswith("rows:1:")
