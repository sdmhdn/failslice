from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from failslice.config import Project
from failslice.engine import Engine
from failslice.reducer import (
    Budget,
    DeterminismError,
    Minimality,
    ProgressEvent,
    Reducer,
    StopReason,
    split_blocks,
)
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables


@pytest.mark.parametrize(("n", "g"), [(1, 1), (5, 2), (10, 3), (7, 7), (3, 10), (100, 8)])
def test_split_blocks_partitions(n: int, g: int) -> None:
    blocks = split_blocks(n, g)
    covered = list(itertools.chain.from_iterable(range(lo, hi + 1) for lo, hi in blocks))
    assert covered == list(range(n))
    sizes = [hi - lo + 1 for lo, hi in blocks]
    assert max(sizes) - min(sizes) <= 1
    assert len(blocks) == min(g, n)


@pytest.fixture
def fanout_engine(project_factory: Callable[..., Project]) -> Iterator[Engine]:
    e = Engine(project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE))
    yield e
    e.close()


def test_reduces_join_fanout_to_one_minimal(fanout_engine: Engine) -> None:
    events: list[ProgressEvent] = []
    stats = Reducer(fanout_engine, Budget(), on_progress=events.append).run()
    assert stats.stop_reason is StopReason.FIXED_POINT
    assert stats.minimality is Minimality.VERIFIED
    assert stats.confirmed
    assert fanout_engine.sizes == {"customers": 1, "orders": 2, "lines": 1}
    kept = fanout_engine.kept_rids()
    assert kept["orders"] == [7, 120]  # the original and its late-arriving duplicate
    assert fanout_engine.evaluate_current().is_true
    assert stats.input_rows == 30 + 121 + 400 and stats.output_rows == 4
    assert stats.evaluations == sum(stats.outcomes.values())
    assert {e.phase for e in events} == {"determinism", "reduce", "certify"}
    assert events[-1].kept_rows == 4


def test_same_seed_same_result(project_factory: Callable[..., Project]) -> None:
    results = []
    for seed in (5, 5, 6):
        p = project_factory(
            fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE, settings={"seed": seed}
        )
        e = Engine(p)
        stats = Reducer(e, Budget()).run()
        results.append((e.kept_rids(), stats.evaluations))
        e.close()
    assert results[0] == results[1]


def test_max_evals_budget(fanout_engine: Engine) -> None:
    stats = Reducer(fanout_engine, Budget(max_evals=10)).run()
    assert stats.stop_reason is StopReason.MAX_EVALS
    assert stats.evaluations == 10
    assert stats.minimality is Minimality.NOT_VERIFIED
    assert fanout_engine.evaluate_current().is_true  # last accepted config is still TRUE


def test_wall_clock_budget(fanout_engine: Engine) -> None:
    ticks = itertools.count()
    stats = Reducer(fanout_engine, Budget(wall_clock_s=8), clock=lambda: float(next(ticks))).run()
    assert stats.stop_reason is StopReason.WALL_CLOCK
    assert stats.minimality is Minimality.NOT_VERIFIED


def test_budget_during_determinism_leaves_unconfirmed(fanout_engine: Engine) -> None:
    stats = Reducer(fanout_engine, Budget(max_evals=2)).run()
    assert stats.stop_reason is StopReason.MAX_EVALS
    assert not stats.confirmed


def test_certification_can_be_skipped(fanout_engine: Engine) -> None:
    stats = Reducer(fanout_engine, Budget()).run(certify=False)
    assert stats.minimality is Minimality.SKIPPED


def test_predicate_false_on_full_data_is_refused(project_factory: Callable[..., Project]) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    e = Engine(p)
    with pytest.raises(DeterminismError, match="FALSE on the full data"):
        Reducer(e, Budget()).run()
    e.close()


class _FlakyRunner:
    """Alternates TRUE / FALSE to simulate a nondeterministic predicate."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.calls = 0

    def run(self, con: Any, timeout_s: float) -> Any:
        self.calls += 1
        out = self.inner.run(con, timeout_s)
        if self.calls == 2:
            from failslice.predicate import Outcome, OutcomeKind

            return Outcome(OutcomeKind.FALSE, "bool:false")
        return out


def test_nondeterminism_is_refused(fanout_engine: Engine) -> None:
    fanout_engine.runner = _FlakyRunner(fanout_engine.runner)  # type: ignore[assignment]
    with pytest.raises(DeterminismError, match="not deterministic") as info:
        Reducer(fanout_engine, Budget()).run()
    assert len(info.value.outcomes) == 4


def test_nondeterminism_can_be_allowed(project_factory: Callable[..., Project]) -> None:
    p = project_factory(
        fanout_tables(),
        fks=FANOUT_FKS,
        predicate=FANOUT_PREDICATE,
        settings={"allow_nondeterministic": True},
    )
    e = Engine(p)
    e.runner = _FlakyRunner(e.runner)  # type: ignore[assignment]
    stats = Reducer(e, Budget()).run()
    assert stats.minimality is Minimality.SKIPPED  # never claimed for flaky predicates
    assert "nondeterministic" in stats.minimality_detail
    e.close()


def test_errors_during_certification_block_the_claim(
    project_factory: Callable[..., Project],
) -> None:
    # Deleting one row makes the count 1 -> FALSE: certification is clean.
    pred = {"sql": "SELECT count(*) = 2 FROM t GROUP BY g"}
    p = project_factory({"t": ([("g", "BIGINT")], [(1,), (1,)])}, predicate=pred)
    e = Engine(p)
    stats = Reducer(e, Budget()).run()
    assert e.sizes == {"t": 2}
    assert stats.minimality is Minimality.VERIFIED  # count becomes 1 -> FALSE, still verified
    e.close()
    pred2 = {"sql": "SELECT count(*) >= 1 FROM t WHERE g = 1 GROUP BY g"}
    p2 = project_factory({"t": ([("g", "BIGINT")], [(1,)])}, predicate=pred2)
    e2 = Engine(p2)
    stats2 = Reducer(e2, Budget()).run()
    assert stats2.minimality is Minimality.NOT_VERIFIED
    assert "not FALSE" in stats2.minimality_detail
    assert stats2.error_classes["shape"] >= 1
    e2.close()


def test_whole_table_deletion_and_empty_tables(project_factory: Callable[..., Project]) -> None:
    p = project_factory(
        {
            "a": ([("x", "BIGINT")], [(i,) for i in range(20)]),
            "b": ([("y", "BIGINT")], [(i,) for i in range(20)]),
            "e": ([("z", "BIGINT")], []),
        },
        predicate={"sql": "SELECT count(*) >= 3 FROM a WHERE x % 5 = 0"},
    )
    e = Engine(p)
    stats = Reducer(e, Budget()).run()
    assert e.sizes == {"a": 3, "b": 0, "e": 0}
    assert stats.minimality is Minimality.VERIFIED
    e.close()


def test_hooks_are_called(fanout_engine: Engine) -> None:
    calls = {"before": 0, "accept": 0}

    def before() -> None:
        calls["before"] += 1

    def accept(_: Engine) -> None:
        calls["accept"] += 1

    stats = Reducer(fanout_engine, Budget(), before_eval=before, on_accept=accept).run()
    assert calls["accept"] == stats.accepted > 0
    assert calls["before"] >= stats.evaluations


def test_subset_step_of_ddmin(project_factory: Callable[..., Project]) -> None:
    # Only "keep one small block" works: complements leave too many rows.
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(i,) for i in range(12)])},
        predicate={"sql": "SELECT count(*) = 12 OR count(*) BETWEEN 1 AND 3 FROM t"},
    )
    e = Engine(p)
    stats = Reducer(e, Budget()).run()
    assert e.sizes == {"t": 1}
    assert stats.minimality is Minimality.VERIFIED
    e.close()


class _CertifyOnly(Reducer):
    def _sweeps(self) -> None:  # skip the search: certification must do all the work
        self.stats.sweeps = 0


def test_certification_accepts_and_restarts(fanout_engine: Engine) -> None:
    stats = _CertifyOnly(fanout_engine, Budget()).run()
    assert stats.minimality is Minimality.VERIFIED
    assert stats.accepted > 0
    assert fanout_engine.sizes == {"customers": 1, "orders": 2, "lines": 1}


def test_certification_ignores_poisoned_negative_cache(fanout_engine: Engine) -> None:
    """Simulate fingerprint collisions: every cached negative is wrong. The search
    may then miss deletions, but certification bypasses the cache and still reaches
    a verified 1-minimal result."""
    from failslice.predicate import Outcome, OutcomeKind

    original = fanout_engine._cache_key
    fanout_engine._cache_key = lambda fps: "collision"  # type: ignore[method-assign]
    fanout_engine._cache["collision"] = Outcome(OutcomeKind.FALSE, "bool:false")
    stats = Reducer(fanout_engine, Budget()).run()
    fanout_engine._cache_key = original  # type: ignore[method-assign]
    assert stats.minimality is Minimality.VERIFIED
    assert fanout_engine.sizes == {"customers": 1, "orders": 2, "lines": 1}
    assert fanout_engine.evaluate_current().is_true
