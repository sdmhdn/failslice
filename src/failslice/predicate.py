"""Predicate execution and outcome classification (ADR 0003).

Outcomes are TRUE (the failure is present), FALSE, ERROR (malformed result, or an
unexpected / different error) and TIMEOUT. Only TRUE counts as "still failing".
"""

from __future__ import annotations

import decimal
import fractions
import functools
import hashlib
import math
import threading
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import duckdb

from failslice.config import ExpectPredicate, PredicateSpec, RaisesPredicate, SqlPredicate

__all__ = ["Outcome", "OutcomeKind", "PredicateRunner", "normalize_expected"]

Row = tuple[Any, ...]


class OutcomeKind(StrEnum):
    """Classification of one predicate evaluation."""

    TRUE = "TRUE"
    FALSE = "FALSE"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"


@dataclass(frozen=True)
class Outcome:
    """Result of one predicate evaluation.

    ``signature`` identifies *what* was observed (the boolean value, a hash of the
    result, or the error class and message) so the determinism check can compare runs.
    ``error_class`` is set for ERROR outcomes caused by an exception and for TRUE
    outcomes of the ``raises`` form.
    """

    kind: OutcomeKind
    signature: str
    detail: str = ""
    error_class: str | None = None

    @property
    def is_true(self) -> bool:
        """Whether the failure is present."""
        return self.kind is OutcomeKind.TRUE


class _Watchdog:
    """Calls ``con.interrupt()`` once the timeout elapses (cooperative, ADR 0005)."""

    def __init__(self, con: duckdb.DuckDBPyConnection, timeout_s: float) -> None:
        self._con = con
        self._lock = threading.Lock()
        self._done = False
        self.fired = False
        self._timer = threading.Timer(timeout_s, self._fire)
        self._timer.daemon = True

    def _fire(self) -> None:
        with self._lock:
            if not self._done:
                self.fired = True
                self._con.interrupt()

    def __enter__(self) -> _Watchdog:
        self._timer.start()
        return self

    def __exit__(self, *exc: object) -> None:
        with self._lock:
            self._done = True
        self._timer.cancel()
        self._timer.join()


def _canonical(value: Any) -> str:
    """Stable text form of a result value, used for signatures."""
    if isinstance(value, float):
        return "nan" if math.isnan(value) else repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(f"{_canonical(k)}:{_canonical(v)}" for k, v in value.items()) + "}"
    return f"{type(value).__name__}:{value!r}"


def rows_signature(rows: Sequence[Row], *, ordered: bool) -> str:
    """Hash a result set (as a bag unless ``ordered``)."""
    lines = [_canonical(r) for r in rows]
    if not ordered:
        lines.sort()
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()
    return f"rows:{len(rows)}:{digest[:32]}"


def _error_signature(exc: BaseException) -> str:
    return f"error:{type(exc).__name__}:{exc}"


def normalize_expected(value: Any) -> list[Row]:
    """Turn an inline expectation into rows.

    A scalar becomes a 1x1 result; a list of lists is a list of rows; a list of
    scalars is a single-column result.
    """
    if isinstance(value, list):
        return [tuple(v) if isinstance(v, (list, tuple)) else (v,) for v in value]
    return [(value,)]


_NUMERIC = (int, float, decimal.Decimal)

# A numeric value as compared: an exact fraction, or a special float value.
_Num = fractions.Fraction | float


@functools.lru_cache(maxsize=1 << 16, typed=True)
def _num(value: int | float | decimal.Decimal) -> _Num:
    """Exact value of a number. Floats are taken at their shortest decimal form
    (``repr``), so ``0.1`` equals ``DECIMAL '0.1'``; NaN and infinities stay floats."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return value
        return fractions.Fraction(repr(value))
    if isinstance(value, decimal.Decimal):
        if value.is_nan():
            return math.nan
        if value.is_infinite():
            return math.inf if value > 0 else -math.inf
    return fractions.Fraction(value)


def _numbers_equal(a: _Num, b: _Num, tol: fractions.Fraction | None) -> bool:
    if isinstance(a, float) or isinstance(b, float):  # NaN or an infinity involved
        if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
            return True
        return a == b  # equal infinities; NaN never equals anything else
    if tol is None:
        return a == b
    return abs(a - b) <= tol


def _values_equal(a: Any, b: Any, tol: fractions.Fraction | None) -> bool:
    """Typed equality: NULL only equals NULL, booleans only equal booleans, numbers
    compare exactly (or within ``tol``), lists and structs compare element-wise."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, _NUMERIC) and isinstance(b, _NUMERIC):
        return _numbers_equal(_num(a), _num(b), tol)
    if isinstance(a, (list, tuple)) or isinstance(b, (list, tuple)):
        return (
            isinstance(a, (list, tuple))
            and isinstance(b, (list, tuple))
            and len(a) == len(b)
            and all(_values_equal(x, y, tol) for x, y in zip(a, b, strict=True))
        )
    if isinstance(a, dict) or isinstance(b, dict):
        return (
            isinstance(a, dict)
            and isinstance(b, dict)
            and list(a) == list(b)
            and all(_values_equal(a[k], b[k], tol) for k in a)
        )
    if type(a) is not type(b) and not (isinstance(a, str) and isinstance(b, str)):
        return False
    return bool(a == b)


def _rows_equal(a: Row, b: Row, tol: fractions.Fraction | None) -> bool:
    return len(a) == len(b) and all(_values_equal(x, y, tol) for x, y in zip(a, b, strict=True))


def _tolerance(tol: float | None) -> fractions.Fraction | None:
    return None if tol is None else fractions.Fraction(repr(float(tol)))


def results_equal(
    actual: Sequence[Row], expected: Sequence[Row], *, ordered: bool, tol: float | None
) -> bool:
    """Typed bag (or list) equality: duplicates count, NULL equals NULL."""
    if len(actual) != len(expected):
        return False
    t = _tolerance(tol)
    if ordered:
        return all(_rows_equal(a, e, t) for a, e in zip(actual, expected, strict=True))
    try:
        # Exactly equal bags are equal under any tolerance. (Only the *whole* bags may be
        # cancelled: removing common rows first can break a tolerant perfect matching.)
        if Counter(_row_key(r) for r in actual) == Counter(_row_key(r) for r in expected):
            return True
        if t is None:
            return False
    except TypeError:  # pragma: no cover - unhashable values fall back to matching
        pass
    return _perfect_matching(actual, expected, t)


def _perfect_matching(
    actual: Sequence[Row], expected: Sequence[Row], tol: fractions.Fraction | None
) -> bool:
    """Whether every actual row can be paired with a distinct, equal expected row
    (bipartite matching with augmenting paths, iterative so large results cannot
    exhaust the Python stack). Order-independent, unlike greedy matching."""
    edges = [[j for j, e in enumerate(expected) if _rows_equal(a, e, tol)] for a in actual]
    if any(not e for e in edges) or len({j for e in edges for j in e}) < len(expected):
        return False  # some row has no partner at all
    match_of_expected: list[int] = [-1] * len(expected)
    unmatched: list[int] = []
    for i, candidates in enumerate(edges):  # greedy start: most rows match directly
        free = next((j for j in candidates if match_of_expected[j] == -1), None)
        if free is None:
            unmatched.append(i)
        else:
            match_of_expected[free] = i
    for root in unmatched:
        seen = [False] * len(expected)
        stack = [root]  # actual rows on the current alternating path
        next_edge = [0]  # per stack frame: index of the next edge to try
        via: list[int] = [-1]  # per stack frame: expected row being tried
        augmented = False
        while stack and not augmented:
            depth = len(stack) - 1
            i = stack[depth]
            if next_edge[depth] == len(edges[i]):
                stack.pop()
                next_edge.pop()
                via.pop()
                continue
            j = edges[i][next_edge[depth]]
            next_edge[depth] += 1
            if seen[j]:
                continue
            seen[j] = True
            via[depth] = j
            if match_of_expected[j] == -1:
                for d, row in enumerate(stack):  # flip the path
                    match_of_expected[via[d]] = row
                augmented = True
            else:
                stack.append(match_of_expected[j])
                next_edge.append(0)
                via.append(-1)
        if not augmented:
            return False
    return True


def _row_key(row: Row) -> tuple[Any, ...]:
    return tuple(_canonical_key(v) for v in row)


def _canonical_key(value: Any) -> Any:
    """Hashable key with the same equality as :func:`_values_equal` without tolerance."""
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, _NUMERIC):
        n = _num(value)
        return ("num", "nan" if isinstance(n, float) and math.isnan(n) else n)
    if value is None:
        return ("null",)
    if isinstance(value, (list, tuple)):
        return ("list", tuple(_canonical_key(v) for v in value))
    if isinstance(value, dict):
        return ("dict", tuple((k, _canonical_key(v)) for k, v in value.items()))
    if isinstance(value, str):
        return ("str", value)
    return (type(value).__name__, value)


class PredicateRunner:
    """Executes one predicate spec on a connection and classifies the outcome."""

    def __init__(self, spec: PredicateSpec, expected_rows: Sequence[Row] | None = None) -> None:
        self.spec = spec
        if isinstance(spec, ExpectPredicate):
            if expected_rows is None:
                if not spec.inline:
                    raise ValueError("expected_rows must be given for expected_file predicates")
                expected_rows = normalize_expected(spec.expected)
            self.expected: list[Row] | None = list(expected_rows)
            # An inline scalar expectation demands a 1x1 result (ADR 0003).
            self.scalar = spec.inline and not isinstance(spec.expected, list)
        else:
            self.expected = None
            self.scalar = False

    @property
    def query(self) -> str:
        """The SQL text the predicate executes."""
        s = self.spec
        return s.sql if isinstance(s, SqlPredicate) else s.query

    def run(self, con: duckdb.DuckDBPyConnection, timeout_s: float) -> Outcome:
        """Execute the predicate (fully fetching the result) and classify it."""
        rows: list[Row] = []
        types: list[str] = []
        exc: duckdb.Error | None = None
        with _Watchdog(con, timeout_s) as dog:
            try:
                con.execute(self.query)
                types = [str(d[1]) for d in (con.description or [])]
                rows = [tuple(r) for r in con.fetchall()]
            except duckdb.Error as err:
                exc = err
        if dog.fired:
            # Past the deadline, even if the query completed just before the interrupt
            # landed: a TIMEOUT is never accepted, so this errs on the safe side.
            return Outcome(OutcomeKind.TIMEOUT, "timeout", f"exceeded {timeout_s:g}s")
        try:
            if exc is not None:
                return self._classify_error(exc)
            return self._classify_result(types, rows)
        except Exception as err:  # e.g. a comparison that cannot be done: never TRUE
            return Outcome(
                OutcomeKind.ERROR,
                f"comparison:{type(err).__name__}",
                f"cannot classify the result: {type(err).__name__}: {err}",
                "comparison",
            )

    def _classify_error(self, exc: duckdb.Error) -> Outcome:
        cls = type(exc).__name__
        spec = self.spec
        if isinstance(spec, RaisesPredicate):
            class_ok = spec.error_class is None or spec.error_class in {
                c.__name__ for c in type(exc).__mro__
            }
            if class_ok and spec.compiled().search(str(exc)):
                return Outcome(OutcomeKind.TRUE, _error_signature(exc), str(exc), cls)
            return Outcome(
                OutcomeKind.ERROR,
                _error_signature(exc),
                f"different error: {cls}: {exc}",
                cls,
            )
        return Outcome(OutcomeKind.ERROR, _error_signature(exc), f"{cls}: {exc}", cls)

    def _classify_result(self, types: list[str], rows: list[Row]) -> Outcome:
        spec = self.spec
        if isinstance(spec, RaisesPredicate):
            return Outcome(OutcomeKind.FALSE, "completed", "query completed without error")
        if isinstance(spec, SqlPredicate):
            if len(types) != 1:
                return _shape_error(f"expected 1 column, got {len(types)}")
            if types[0] != "BOOLEAN":
                return _shape_error(f"expected a BOOLEAN column, got {types[0]}")
            if len(rows) != 1:
                return _shape_error(f"expected exactly 1 row, got {len(rows)}")
            value = rows[0][0]
            if value is True:
                return Outcome(OutcomeKind.TRUE, "bool:true")
            if value is None:
                return Outcome(OutcomeKind.FALSE, "bool:null", "NULL counts as FALSE")
            return Outcome(OutcomeKind.FALSE, "bool:false")
        assert self.expected is not None
        if self.scalar and (len(types) != 1 or len(rows) != 1):
            return _shape_error(
                f"a scalar expectation needs a 1x1 result, got {len(rows)}x{len(types)}"
            )
        sig = rows_signature(rows, ordered=spec.ordered)
        equal = results_equal(rows, self.expected, ordered=spec.ordered, tol=spec.float_tolerance)
        present = (not equal) if spec.mode == "differs" else equal
        relation = "equals" if equal else "differs from"
        detail = f"result ({len(rows)} rows) {relation} the expectation"
        return Outcome(OutcomeKind.TRUE if present else OutcomeKind.FALSE, sig, detail)


def _shape_error(detail: str) -> Outcome:
    return Outcome(OutcomeKind.ERROR, f"shape:{detail}", f"malformed predicate result: {detail}")
