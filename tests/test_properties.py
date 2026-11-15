"""Property tests on random small DAG schemas with planted bugs, plus a brute-force oracle.

Checked for every reduction (docs/design.md §15):
- the output still satisfies the predicate (independently re-evaluated);
- FK integrity holds (no new orphans) and the output is a subset of the input;
- the same seed gives the same output;
- whenever "1-minimal (verified)" is reported, every single-row cascade deletion is
  indeed not TRUE (checked against the pure-Python reference model).
"""

from __future__ import annotations

import os
from typing import Any

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from failslice.engine import Engine
from failslice.reducer import Budget, DeterminismError, Minimality, Reducer
from tests.reference import (
    Schema,
    brute_force_minimum,
    evaluate,
    greatest_intact,
    is_intact,
    kept_sets,
    predicates,
    schemas,
)

EXAMPLES = int(os.environ.get("FAILSLICE_PROPERTY_EXAMPLES", "60"))
_settings = settings(
    max_examples=EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


def _reduce(
    schema: Schema, predicate: str, base: Any, seed: int = 0
) -> tuple[dict[str, set[int]], Any]:
    e = Engine(schema.project(base, predicate, seed=seed))
    try:
        stats = Reducer(e, Budget(max_evals=5000, wall_clock_s=120)).run()
        return kept_sets(e.kept_rids()), stats
    finally:
        e.close()


@_settings
@given(data=st.data())
def test_reduction_properties(tmp_path_factory: Any, data: st.DataObject) -> None:
    schema = data.draw(schemas())
    predicate = data.draw(predicates(schema))
    full = {t: set(range(len(schema.rows[t]))) for t in schema.tables}
    assume(evaluate(schema, full, predicate) is True)
    seed = data.draw(st.integers(0, 3))
    try:
        kept, stats = _reduce(schema, predicate, tmp_path_factory.mktemp("prop"), seed)
    except DeterminismError:  # pragma: no cover - our predicates are deterministic
        raise AssertionError("deterministic predicate refused") from None

    assert evaluate(schema, kept, predicate) is True
    assert is_intact(schema, kept)
    assert all(kept[t] <= full[t] for t in schema.tables)
    assert stats.output_rows == sum(len(v) for v in kept.values())

    again, _ = _reduce(schema, predicate, tmp_path_factory.mktemp("prop"), seed)
    assert again == kept

    if stats.minimality is Minimality.VERIFIED:
        for t in schema.tables:
            for rid in kept[t]:
                smaller = {u: (kept[u] - {rid} if u == t else set(kept[u])) for u in schema.tables}
                repaired = greatest_intact(schema, smaller)
                assert evaluate(schema, repaired, predicate) is not True, (t, rid)


@settings(
    max_examples=max(10, EXAMPLES // 3),
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(data=st.data())
def test_brute_force_oracle(tmp_path_factory: Any, data: st.DataObject) -> None:
    """On tiny inputs, the reduced size is never below the true minimum (sanity), and
    a single-table monotone 'at least m matching rows' bug is reduced to the optimum."""
    schema = data.draw(schemas(max_tables=2, max_rows=4))
    predicate = data.draw(predicates(schema))
    full = {t: set(range(len(schema.rows[t]))) for t in schema.tables}
    assume(evaluate(schema, full, predicate) is True)
    optimum = brute_force_minimum(schema, predicate)
    assert optimum is not None  # the full input is itself a witness
    kept, _ = _reduce(schema, predicate, tmp_path_factory.mktemp("oracle"))
    size = sum(len(v) for v in kept.values())
    assert size >= optimum
    monotone_single_table = (
        "count(*) >=" in predicate and " JOIN " not in predicate and "EXISTS" not in predicate
    )
    if monotone_single_table and not any(schema.parents.values()):
        assert size == optimum
