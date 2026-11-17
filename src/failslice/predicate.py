"""Predicate execution and outcome classification (ADR 0003).

Outcomes are TRUE (the failure is present), FALSE, ERROR (malformed result, or an
unexpected / different error) and TIMEOUT. Only TRUE counts as "still failing".

The semantics live in :mod:`failslice.portable` (shared with generated pytest bundles,
ADR 0009); this module adapts the configuration models to it.
"""

from __future__ import annotations

from collections.abc import Sequence

import duckdb

from failslice.config import ExpectPredicate, PredicateSpec, RaisesPredicate, SqlPredicate
from failslice.portable import (
    Check,
    Outcome,
    OutcomeKind,
    Row,
    normalize_expected,
    results_equal,
    rows_signature,
    run_check,
)

__all__ = [
    "Outcome",
    "OutcomeKind",
    "PredicateRunner",
    "Row",
    "check_for",
    "normalize_expected",
    "results_equal",
    "rows_signature",
]


def check_for(spec: PredicateSpec, expected_rows: Sequence[Row] | None = None) -> Check:
    """The portable :class:`~failslice.portable.Check` for a predicate spec.

    ``expected_rows`` is required for an ``expected_file`` expectation (the snapshot).
    """
    if isinstance(spec, SqlPredicate):
        return Check(form="sql", query=spec.sql)
    if isinstance(spec, RaisesPredicate):
        return Check(
            form="raises",
            query=spec.query,
            match=spec.match,
            flags=spec.compiled().flags,
            error_class=spec.error_class,
        )
    assert isinstance(spec, ExpectPredicate)
    if expected_rows is None:
        if not spec.inline:
            raise ValueError("expected_rows must be given for expected_file predicates")
        expected_rows = normalize_expected(spec.expected)
    return Check(
        form="expect",
        query=spec.query,
        expected=tuple(expected_rows),
        # An inline scalar expectation demands a 1x1 result (ADR 0003).
        scalar=spec.inline and not isinstance(spec.expected, list),
        mode=spec.mode,
        ordered=spec.ordered,
        tolerance=spec.float_tolerance,
    )


class PredicateRunner:
    """Executes one predicate spec on a connection and classifies the outcome."""

    def __init__(self, spec: PredicateSpec, expected_rows: Sequence[Row] | None = None) -> None:
        self.spec = spec
        self.check = check_for(spec, expected_rows)

    @property
    def query(self) -> str:
        """The SQL text the predicate executes."""
        return self.check.query

    def run(self, con: duckdb.DuckDBPyConnection, timeout_s: float) -> Outcome:
        """Execute the predicate (fully fetching the result) and classify it."""
        return run_check(con, self.check, timeout_s)
