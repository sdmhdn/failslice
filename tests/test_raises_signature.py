"""The ``raises`` signature is the concrete class plus the matched text (ADR 0012).

Diagnostics are controlled with DuckDB's ``error()`` and casts, so these tests do not depend
on which offending row an optimizer happens to reach first.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from failslice.config import RaisesPredicate
from failslice.predicate import OutcomeKind, PredicateRunner


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect()
    yield c
    c.close()


def _run(con: duckdb.DuckDBPyConnection, query: str, match: str, **kw: Any) -> Any:
    return PredicateRunner(RaisesPredicate(query=query, match=match, **kw)).run(con, 10)


def test_different_offending_values_same_matched_text(con: duckdb.DuckDBPyConnection) -> None:
    a = _run(con, "SELECT error('Could not parse string \"03/13/2024\"')", "Could not parse string")
    b = _run(con, "SELECT error('Could not parse string \"03/15/2024\"')", "Could not parse string")
    assert a.is_true and b.is_true
    assert a.signature == b.signature == "error:InvalidInputException:Could not parse string"
    assert "03/13/2024" in a.detail and "03/15/2024" in b.detail  # full message kept


def test_varying_matched_text_still_differs(con: duckdb.DuckDBPyConnection) -> None:
    pattern = r"string \"03/1\d/2024\""
    a = _run(con, "SELECT error('Could not parse string \"03/13/2024\"')", pattern)
    b = _run(con, "SELECT error('Could not parse string \"03/15/2024\"')", pattern)
    assert a.is_true and b.is_true and a.signature != b.signature


def test_concrete_class_is_part_of_the_signature(con: duckdb.DuckDBPyConnection) -> None:
    # Both match through the common base class, but they are different failures.
    conv = _run(con, "SELECT CAST('boom' AS INTEGER)", "boom", error_class="DatabaseError")
    inv = _run(con, "SELECT error('boom')", "boom", error_class="DatabaseError")
    assert conv.is_true and inv.is_true
    assert conv.error_class == "ConversionException" and inv.error_class == "InvalidInputException"
    assert conv.signature != inv.signature


def test_non_matching_error_keeps_full_message(con: duckdb.DuckDBPyConnection) -> None:
    out = _run(con, "SELECT error('something else 42')", "Could not parse")
    assert out.kind is OutcomeKind.ERROR
    assert out.signature == "error:InvalidInputException:Invalid Input Error: something else 42"


def test_empty_match_signature_is_class_only(con: duckdb.DuckDBPyConnection) -> None:
    out = _run(con, "SELECT error('anything')", "")
    assert out.is_true and out.signature == "error:InvalidInputException:"
