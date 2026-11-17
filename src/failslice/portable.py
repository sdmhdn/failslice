"""Portable predicate semantics: outcome classification, typed result comparison, the
expectation codec, the referential-integrity check and the pytest bundle runner.

This module imports only the Python standard library and ``duckdb``. FailSlice uses it
for reduction and replay, and every generated pytest bundle ships a byte-identical copy
(``_failslice_portable.py``), so a generated test classifies outcomes exactly like the
tool that produced it (ADR 0009). Keep it free of other imports.
"""

from __future__ import annotations

import base64
import datetime as _dt
import decimal
import fractions
import functools
import hashlib
import json
import math
import re
import threading
import time
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import duckdb

__all__ = [
    "PORTABLE_VERSION",
    "BundleResult",
    "Check",
    "ForeignKeyRef",
    "Outcome",
    "OutcomeKind",
    "Row",
    "count_violations",
    "decode_rows",
    "encode_rows",
    "exemption_problem",
    "normalize_expected",
    "results_equal",
    "rows_signature",
    "run_bundle",
    "run_check",
]

PORTABLE_VERSION = 1
"""Bumped whenever classification or comparison semantics change."""

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
            and len(a) == len(b)
            and all(  # keys are typed values too (a MAP key TRUE never equals 1)
                _values_equal(ka, kb, None) and _values_equal(va, vb, tol)
                for (ka, va), (kb, vb) in zip(a.items(), b.items(), strict=True)
            )
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
        return (
            "dict",
            tuple((_canonical_key(k), _canonical_key(v)) for k, v in value.items()),
        )
    if isinstance(value, str):
        return ("str", value)
    return (type(value).__name__, value)


class DeadlineConnection:
    """Wraps a connection so that every statement runs under its own watchdog, armed
    with the time left until ``deadline`` (absolute ``time.monotonic()``). A statement
    started after the deadline, or interrupted by it, raises ``TimeoutError``."""

    def __init__(self, con: duckdb.DuckDBPyConnection, deadline: float | None) -> None:
        self.con = con
        self.deadline = deadline

    def _run(self, fn: Any, *args: Any) -> duckdb.DuckDBPyConnection:
        if self.deadline is None:
            result: duckdb.DuckDBPyConnection = fn(*args)
            return result
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("deadline reached")
        with _Watchdog(self.con, left) as dog:
            try:
                result = fn(*args)
            except duckdb.InterruptException as exc:
                if dog.fired:
                    raise TimeoutError("deadline reached") from exc
                raise
        if dog.fired:
            raise TimeoutError("deadline reached")
        return result

    def execute(self, sql: str, params: Any = None) -> duckdb.DuckDBPyConnection:
        """``con.execute`` under the deadline."""
        return self._run(self.con.execute, sql, params)

    def executemany(self, sql: str, params: Any) -> duckdb.DuckDBPyConnection:
        """``con.executemany`` under the deadline."""
        return self._run(self.con.executemany, sql, params)

    def fetchall(self, sql: str, params: Any = None) -> list[Any]:
        """Execute and fetch every row, both under the deadline."""
        rows: list[Any] = []

        def run() -> duckdb.DuckDBPyConnection:
            rows.extend(self.con.execute(sql, params).fetchall())
            return self.con

        self._run(run)
        return rows

    def fetchone(self, sql: str, params: Any = None) -> Any:
        """Execute and fetch the first row, both under the deadline."""
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None


# ---------------------------------------------------------------------------- classification


@dataclass(frozen=True)
class Check:
    """One predicate, independent of FailSlice's configuration models."""

    form: Literal["sql", "expect", "raises"]
    query: str
    expected: tuple[Row, ...] | None = None
    """``expect`` form: the expected rows."""
    scalar: bool = False
    """``expect`` form: the expectation is a scalar, so a 1x1 result is required."""
    mode: Literal["differs", "equals"] = "differs"
    ordered: bool = False
    tolerance: float | None = None
    match: str | None = None
    """``raises`` form: regular expression searched in the error message."""
    flags: int = 0
    error_class: str | None = None


@functools.lru_cache(maxsize=64)
def _regex(pattern: str, flags: int) -> re.Pattern[str]:
    return re.compile(pattern, flags)


def run_check(con: duckdb.DuckDBPyConnection, check: Check, timeout_s: float) -> Outcome:
    """Execute the predicate (fully fetching the result) and classify it."""
    rows: list[Row] = []
    types: list[str] = []
    exc: duckdb.Error | None = None
    with _Watchdog(con, timeout_s) as dog:
        try:
            con.execute(check.query)
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
            return _classify_error(check, exc)
        return _classify_result(check, types, rows)
    except Exception as err:  # e.g. a comparison that cannot be done: never TRUE
        return Outcome(
            OutcomeKind.ERROR,
            f"comparison:{type(err).__name__}",
            f"cannot classify the result: {type(err).__name__}: {err}",
            "comparison",
        )


def _classify_error(check: Check, exc: duckdb.Error) -> Outcome:
    cls = type(exc).__name__
    if check.form == "raises":
        class_ok = check.error_class is None or check.error_class in {
            c.__name__ for c in type(exc).__mro__
        }
        if class_ok and _regex(check.match or "", check.flags).search(str(exc)):
            return Outcome(OutcomeKind.TRUE, _error_signature(exc), str(exc), cls)
        return Outcome(
            OutcomeKind.ERROR, _error_signature(exc), f"different error: {cls}: {exc}", cls
        )
    return Outcome(OutcomeKind.ERROR, _error_signature(exc), f"{cls}: {exc}", cls)


def _classify_result(check: Check, types: list[str], rows: list[Row]) -> Outcome:
    if check.form == "raises":
        return Outcome(OutcomeKind.FALSE, "completed", "query completed without error")
    if check.form == "sql":
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
    if check.expected is None:
        raise ValueError("an expect check needs expected rows")
    if check.scalar and (len(types) != 1 or len(rows) != 1):
        return _shape_error(
            f"a scalar expectation needs a 1x1 result, got {len(rows)}x{len(types)}"
        )
    sig = rows_signature(rows, ordered=check.ordered)
    equal = results_equal(rows, check.expected, ordered=check.ordered, tol=check.tolerance)
    present = (not equal) if check.mode == "differs" else equal
    relation = "equals" if equal else "differs from"
    detail = f"result ({len(rows)} rows) {relation} the expectation"
    return Outcome(OutcomeKind.TRUE if present else OutcomeKind.FALSE, sig, detail)


def _shape_error(detail: str) -> Outcome:
    return Outcome(OutcomeKind.ERROR, f"shape:{detail}", f"malformed predicate result: {detail}")


# ---------------------------------------------------------------------------- codec


def encode_value(value: Any) -> Any:
    """Encode one result value as fully tagged JSON (``[tag, payload]``). Raises
    ``TypeError`` for values without an exact encoding (never falls back to ``repr``)."""
    if value is None:
        return ["null"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        return ["float", repr(value)]
    if isinstance(value, decimal.Decimal):
        return ["decimal", str(value)]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ["bytes", base64.b64encode(bytes(value)).decode("ascii")]
    if isinstance(value, _dt.datetime):
        return ["datetime", value.isoformat()]
    if isinstance(value, _dt.date):
        return ["date", value.isoformat()]
    if isinstance(value, _dt.time):
        return ["time", value.isoformat()]
    if isinstance(value, _dt.timedelta):
        return ["timedelta", [value.days, value.seconds, value.microseconds]]
    if isinstance(value, uuid.UUID):
        return ["uuid", str(value)]
    if isinstance(value, (list, tuple)):
        return ["list", [encode_value(v) for v in value]]
    if isinstance(value, dict):
        return ["map", [[encode_value(k), encode_value(v)] for k, v in value.items()]]
    raise TypeError(f"cannot encode a value of type {type(value).__name__} exactly")


def decode_value(data: Any) -> Any:
    """Inverse of :func:`encode_value`."""
    if not isinstance(data, list) or not data or not isinstance(data[0], str):
        raise ValueError(f"malformed encoded value: {data!r}")
    tag = data[0]
    if tag == "null":
        return None
    payload = data[1]
    simple: dict[str, Any] = {
        "bool": bool,
        "int": int,
        "float": float,
        "decimal": decimal.Decimal,
        "str": str,
        "datetime": _dt.datetime.fromisoformat,
        "date": _dt.date.fromisoformat,
        "time": _dt.time.fromisoformat,
        "uuid": uuid.UUID,
    }
    if tag in simple:
        return simple[tag](payload)
    if tag == "bytes":
        return base64.b64decode(payload)
    if tag == "timedelta":
        days, seconds, micros = payload
        return _dt.timedelta(days=days, seconds=seconds, microseconds=micros)
    if tag == "list":
        return [decode_value(v) for v in payload]
    if tag == "map":
        return {decode_value(k): decode_value(v) for k, v in payload}
    raise ValueError(f"unknown value tag {tag!r}")


def encode_rows(rows: Sequence[Row]) -> list[list[Any]]:
    """Encode result rows (see :func:`encode_value`)."""
    return [[encode_value(v) for v in row] for row in rows]


def decode_rows(data: Sequence[Sequence[Any]]) -> list[Row]:
    """Inverse of :func:`encode_rows`."""
    return [tuple(decode_value(v) for v in row) for row in data]


# ---------------------------------------------------------------------------- integrity

ForeignKeyRef = tuple[str, str, str, str]
"""``(child, child_column, parent, parent_column)``."""


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def fk_label(fk: ForeignKeyRef) -> str:
    """``child.col -> parent.col``."""
    return f"{fk[0]}.{fk[1]} -> {fk[2]}.{fk[3]}"


def exemption_problem(
    exempt: Sequence[tuple[Any, Any]], fks: Sequence[ForeignKeyRef], counts: Mapping[str, int]
) -> str | None:
    """Orphan exemptions must name an existing edge and a row of that edge's child table
    (tampered or malformed metadata would otherwise hide violations)."""
    seen: set[tuple[int, int]] = set()
    for edge, row_index in exempt:
        if edge is None or row_index is None:
            return "malformed orphans metadata: NULL edge or row index"
        if not all(isinstance(v, int) and not isinstance(v, bool) for v in (edge, row_index)):
            return f"malformed orphans metadata: non-integer entry ({edge!r}, {row_index!r})"
        if not 0 <= edge < len(fks):
            return f"malformed orphans metadata: unknown edge {edge}"
        child = fks[edge][0]
        if not 0 <= row_index < counts.get(child, 0):
            return f"malformed orphans metadata: row {row_index} not in {child!r}"
        if (edge, row_index) in seen:
            return f"malformed orphans metadata: duplicate entry ({edge}, {row_index})"
        seen.add((edge, row_index))
    return None


def count_violations(
    con: DeadlineConnection,
    fks: Sequence[ForeignKeyRef],
    exempt: Sequence[tuple[int, int]],
) -> dict[str, int]:
    """New dangling references per FK edge: non-NULL child values without a parent, except
    the exempted ``(edge, row index)`` input orphans. Row indexes are the child table's
    insertion order (validate ``exempt`` with :func:`exemption_problem` first)."""
    con.execute("CREATE TEMP TABLE __fs_exempt(edge INTEGER, row_index BIGINT)")
    try:
        if exempt:
            con.executemany("INSERT INTO __fs_exempt VALUES (?, ?)", [list(e) for e in exempt])
        violations: dict[str, int] = {}
        for i, (child, col, parent, pcol) in enumerate(fks):
            # Project only the FK column into a scratch table: its row-id pseudo column is
            # then the row index, even if the user's table has a "rowid" column.
            con.execute(f"CREATE TEMP TABLE __fs_ix AS SELECT {_q(col)} AS v FROM {_q(child)}")
            try:
                row = con.fetchone(
                    "SELECT count(*) FROM (SELECT rowid AS ri, v FROM __fs_ix) c "
                    "WHERE c.v IS NOT NULL AND NOT EXISTS "
                    f"(SELECT 1 FROM {_q(parent)} p WHERE p.{_q(pcol)} = c.v) "
                    "AND NOT EXISTS (SELECT 1 FROM __fs_exempt x "
                    f"WHERE x.edge = {i} AND x.row_index = c.ri)"
                )
            finally:
                con.execute("DROP TABLE __fs_ix")
            n = int(row[0]) if row else 0
            if n:
                violations[fk_label((child, col, parent, pcol))] = n
        return violations
    finally:
        con.execute("DROP TABLE __fs_exempt")


# ---------------------------------------------------------------------------- bundle

BUNDLE_FORMAT = 1


@dataclass
class BundleResult:
    """What a pytest bundle observed."""

    outcome: Outcome | None = None
    problems: list[str] = field(default_factory=list)
    violations: dict[str, int] = field(default_factory=dict)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _inside(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or any(ch in str(path) for ch in "*?[]{}"):
        raise ValueError(f"bundle file outside the bundle or not a literal path: {relative}")
    return path


def run_bundle(
    bundle_dir: str | Path, *, timeout_s: float | None = None, deadline: float | None = None
) -> BundleResult:
    """Rebuild the reduced tables of a pytest bundle in a fresh in-memory DuckDB database
    (only from the bundle's own files), check fixture hashes, schemas and referential
    integrity, then run ``query.sql`` and classify the outcome.

    The fixture files are read as they are; external access is then switched off before
    any expression from ``reproducer.json`` (type restoration) or the query runs.
    ``timeout_s`` overrides the bundle's evaluation timeout; ``deadline`` (absolute
    ``time.monotonic()``) bounds every statement, the predicate included."""
    root = Path(bundle_dir).resolve()
    result = BundleResult()
    spec = json.loads((root / "reproducer.json").read_text(encoding="utf-8"))
    if spec.get("format") != BUNDLE_FORMAT or spec.get("portable_version") != PORTABLE_VERSION:
        result.problems.append(
            f"unsupported bundle (format {spec.get('format')!r}, portable "
            f"{spec.get('portable_version')!r}); regenerate it with this FailSlice version"
        )
        return result
    for rel, digest in sorted(spec["fixtures"].items()):
        if _sha256(_inside(root, rel)) != digest:
            result.problems.append(f"fixture {rel} changed (sha256 mismatch)")
    if result.problems:
        return result
    raw = duckdb.connect(
        ":memory:",
        config={"autoinstall_known_extensions": False, "autoload_known_extensions": False},
    )
    con = DeadlineConnection(raw, deadline)
    try:
        con.execute(f"SET threads = {int(spec['threads'])}")
        counts: dict[str, int] = {}
        con.execute("CREATE SCHEMA __fs_raw")
        for i, table in enumerate(spec["tables"]):
            source = f"read_parquet({_lit(str(_inside(root, table['file'])))})"
            con.execute(f"CREATE TABLE __fs_raw.t{i} AS SELECT * FROM {source}")
        exempt_rows = con.fetchall(
            f"SELECT edge, row_index FROM read_parquet({_lit(str(_inside(root, spec['orphans'])))})"
        )
        con.execute("SET enable_external_access = false")
        for i, table in enumerate(spec["tables"]):
            exprs = ", ".join(table["select"])
            con.execute(f"CREATE TABLE {_q(table['name'])} AS SELECT {exprs} FROM __fs_raw.t{i}")
        con.execute("DROP SCHEMA __fs_raw CASCADE")
        for table in spec["tables"]:
            got = [[str(r[0]), str(r[1])] for r in con.fetchall(f"DESCRIBE {_q(table['name'])}")]
            if got != [list(c) for c in table["columns"]]:
                result.problems.append(f"schema of {table['name']!r} differs: {got}")
            row = con.fetchone(f"SELECT count(*) FROM {_q(table['name'])}")
            counts[table["name"]] = int(row[0]) if row else 0
        if result.problems:
            return result
        fks = [tuple(fk) for fk in spec["foreign_keys"]]
        exempt = [(e, r) for e, r in exempt_rows]
        problem = exemption_problem(exempt, fks, counts)
        if problem:
            result.problems.append(problem)
            return result
        result.violations = count_violations(con, fks, exempt)
        p = spec["predicate"]
        expected = p.get("expected")
        check = Check(
            form=p["form"],
            query=(root / "query.sql").read_text(encoding="utf-8"),
            expected=tuple(decode_rows(expected)) if expected is not None else None,
            scalar=bool(p.get("scalar", False)),
            mode=p.get("mode", "differs"),
            ordered=bool(p.get("ordered", False)),
            tolerance=p.get("tolerance"),
            match=p.get("match"),
            flags=int(p.get("flags", 0)),
            error_class=p.get("error_class"),
        )
        timeout = float(spec["eval_timeout_s"]) if timeout_s is None else timeout_s
        if deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("deadline reached")
            timeout = min(timeout, left)
        result.outcome = run_check(raw, check, timeout)
        return result
    finally:
        raw.close()
