"""Cascade ddmin reduction, fixed-point sweeps and 1-minimality certification.

See ``docs/design.md`` §5-§7 and ADR 0002. Only TRUE outcomes are accepted, so every
configuration the reducer ends on was evaluated TRUE.
"""

from __future__ import annotations

import dataclasses
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal

from failslice.engine import AttemptResult, Candidate, Engine
from failslice.predicate import Outcome, OutcomeKind

__all__ = [
    "Budget",
    "DeterminismError",
    "Minimality",
    "PredicateNotFailingError",
    "ProgressEvent",
    "Reducer",
    "ReductionStats",
    "StopReason",
    "split_blocks",
]


class StopReason(StrEnum):
    """Why the reducer stopped."""

    FIXED_POINT = "fixed_point"
    MAX_EVALS = "max_evals"
    WALL_CLOCK = "wall_clock"
    KILLED = "killed"
    """The supervisor killed the reduction process (hard deadline or unresponsive engine)."""


class Minimality(StrEnum):
    """Whether the result was certified 1-minimal (ADR 0002)."""

    VERIFIED = "verified"
    NOT_VERIFIED = "not_verified"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class Budget:
    """Limits for one reduction. ``max_evals`` counts predicate executions (cache hits
    are free); ``wall_clock_s`` covers the reduction and certification."""

    max_evals: int = 5000
    wall_clock_s: float = 900.0


class _BudgetExhaustedError(Exception):
    def __init__(self, reason: StopReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class DeterminismError(RuntimeError):
    """The predicate is not stable on the full data (or, see subclass, not TRUE)."""

    def __init__(self, message: str, outcomes: list[Outcome]) -> None:
        super().__init__(message)
        self.outcomes = outcomes


class PredicateNotFailingError(DeterminismError):
    """The predicate was never TRUE on the full data: there is nothing to reduce.
    Never bypassed by ``allow_nondeterministic``."""


@dataclass(frozen=True)
class ProgressEvent:
    """Progress information emitted after every attempt."""

    phase: Literal["determinism", "reduce", "certify"]
    table: str | None
    evaluations: int
    cache_hits: int
    kept_rows: int
    input_rows: int
    elapsed_s: float
    accepted: bool


@dataclass
class ReductionStats:
    """Counters and status of a reduction run."""

    sizes_before: dict[str, int]
    sizes_after: dict[str, int] = field(default_factory=dict)
    evaluations: int = 0
    outcomes: Counter[str] = field(default_factory=Counter)
    error_classes: Counter[str] = field(default_factory=Counter)
    cache_hits: int = 0
    accepted: int = 0
    sweeps: int = 0
    stop_reason: StopReason = StopReason.FIXED_POINT
    minimality: Minimality = Minimality.SKIPPED
    minimality_detail: str = ""
    elapsed_s: float = 0.0
    determinism_signatures: list[str] = field(default_factory=list)
    confirmed: bool = False
    """Whether the final configuration was observed TRUE (determinism check or an
    accepted candidate). False only if a budget stopped the run before that."""
    evaluations_started: int | None = None
    """Predicate executions started, as counted by the supervisor (isolated runs only).
    Can exceed ``evaluations`` when the reduction process was stopped mid-evaluation or
    after its last checkpoint."""

    @property
    def input_rows(self) -> int:
        """Total rows before reduction."""
        return sum(self.sizes_before.values())

    @property
    def output_rows(self) -> int:
        """Total rows after reduction."""
        return sum(self.sizes_after.values())


def split_blocks(n: int, g: int) -> list[tuple[int, int]]:
    """Split positions ``0..n-1`` into ``g`` contiguous, balanced, inclusive ranges."""
    g = max(1, min(g, n))
    base, extra = divmod(n, g)
    blocks = []
    start = 0
    for i in range(g):
        size = base + (1 if i < extra else 0)
        blocks.append((start, start + size - 1))
        start += size
    return blocks


class Reducer:
    """Drives an :class:`Engine` through determinism check, sweeps and certification."""

    def __init__(
        self,
        engine: Engine,
        budget: Budget,
        *,
        on_progress: Callable[[ProgressEvent], None] | None = None,
        before_eval: Callable[[], None] | None = None,
        on_accept: Callable[[Engine], None] | None = None,
        on_confirmed: Callable[[Engine], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.engine = engine
        self.budget = budget
        self.on_progress = on_progress
        self.before_eval = before_eval
        self.on_accept = on_accept
        self.on_confirmed = on_confirmed
        self.clock = clock
        self._start = clock()
        self.stats = ReductionStats(sizes_before=engine.sizes)
        self._input_rows = engine.total
        self._phase: Literal["determinism", "reduce", "certify"] = "determinism"

    def snapshot(self) -> ReductionStats:
        """A copy of the statistics as of now (for checkpoints)."""
        stats = dataclasses.replace(
            self.stats,
            outcomes=Counter(self.stats.outcomes),
            error_classes=Counter(self.stats.error_classes),
            determinism_signatures=list(self.stats.determinism_signatures),
            sizes_before=dict(self.stats.sizes_before),
        )
        stats.sizes_after = self.engine.sizes
        stats.cache_hits = self.engine.cache_hits
        stats.elapsed_s = self._elapsed()
        return stats

    # ------------------------------------------------------------------ budget
    def _elapsed(self) -> float:
        return self.clock() - self._start

    def _check_budget(self) -> None:
        if self.stats.evaluations >= self.budget.max_evals:
            raise _BudgetExhaustedError(StopReason.MAX_EVALS)
        if self._elapsed() >= self.budget.wall_clock_s:
            raise _BudgetExhaustedError(StopReason.WALL_CLOCK)

    def _record(self, outcome: Outcome) -> None:
        self.stats.evaluations += 1
        self.stats.outcomes[outcome.kind.value] += 1
        if outcome.kind is OutcomeKind.ERROR:
            self.stats.error_classes[outcome.error_class or "shape"] += 1

    def _emit(self, table: str | None, accepted: bool) -> None:
        if self.on_progress is not None:
            self.on_progress(
                ProgressEvent(
                    phase=self._phase,
                    table=table,
                    evaluations=self.stats.evaluations,
                    cache_hits=self.engine.cache_hits,
                    kept_rows=self.engine.total,
                    input_rows=self._input_rows,
                    elapsed_s=self._elapsed(),
                    accepted=accepted,
                )
            )

    def _attempt(self, candidate: Candidate, *, use_cache: bool = True) -> AttemptResult:
        self._check_budget()
        hits_before = self.engine.cache_hits
        # before_eval fires inside the engine, after the cache lookup, so it counts
        # real predicate executions only.
        result = self.engine.attempt(candidate, use_cache=use_cache, before_eval=self.before_eval)
        if self.engine.cache_hits == hits_before:
            self._record(result.outcome)
        if result.accepted:
            self.stats.accepted += 1
            self.stats.confirmed = True
            if self.on_accept is not None:
                self.on_accept(self.engine)
        self._emit(candidate.table, result.accepted)
        return result

    # ------------------------------------------------------------------ phases
    def check_determinism(self) -> list[Outcome]:
        """Run the predicate on the full data twice through plain views and twice
        through the reduction views (uncached); require TRUE with equal signatures."""
        self._phase = "determinism"
        outcomes: list[Outcome] = []
        paths: tuple[Literal["plain", "view"], ...] = ("plain", "plain", "view", "view")
        for path in paths:
            self._check_budget()
            if self.before_eval is not None:
                self.before_eval()
            out = self.engine.evaluate_current(path)
            self._record(out)
            outcomes.append(out)
            self._emit(None, False)
        self.stats.determinism_signatures = [o.signature for o in outcomes]
        if not any(o.is_true for o in outcomes):
            raise PredicateNotFailingError(
                f"the predicate is {outcomes[0].kind.value} on the full data "
                f"({outcomes[0].detail or outcomes[0].signature}); nothing to reduce",
                outcomes,
            )
        if any(
            o.kind != outcomes[0].kind or o.signature != outcomes[0].signature for o in outcomes
        ):
            raise DeterminismError(
                "the predicate is not deterministic on the full data: "
                + ", ".join(f"{o.kind.value}[{o.signature[:40]}]" for o in outcomes),
                outcomes,
            )
        self._confirm()
        return outcomes

    def _confirm(self) -> None:
        self.stats.confirmed = True
        if self.on_confirmed is not None:
            self.on_confirmed(self.engine)

    def run(self, *, check_determinism: bool = True, certify: bool = True) -> ReductionStats:
        """Reduce to a fixed point (or budget stop), then optionally certify."""
        try:
            if check_determinism:
                try:
                    self.check_determinism()
                except PredicateNotFailingError:
                    raise
                except DeterminismError:
                    if not self.engine.project.settings.allow_nondeterministic:
                        raise
                    # The full data was observed TRUE at least once; reduce, but never
                    # claim minimality for an unstable predicate.
                    certify = False
                    self.stats.minimality_detail = "nondeterministic predicate allowed"
                    self._confirm()
            self._phase = "reduce"
            self._sweeps()
            if certify:
                self._certify()
        except _BudgetExhaustedError as stop:
            self.stats.stop_reason = stop.reason
            if self.stats.minimality is not Minimality.VERIFIED and certify:
                self.stats.minimality = Minimality.NOT_VERIFIED
                self.stats.minimality_detail = f"stopped by budget ({stop.reason.value})"
        self.stats.sizes_after = self.engine.sizes
        self.stats.cache_hits = self.engine.cache_hits
        self.stats.elapsed_s = self._elapsed()
        return self.stats

    def _sweeps(self) -> None:
        while True:
            self.stats.sweeps += 1
            before = self.engine.total
            for table in self.engine.project.topo_order:
                self._ddmin(table)
            if self.engine.total == before:
                return

    def _ddmin(self, table: str) -> None:
        n = self.engine.sizes[table]
        if n == 0:
            return
        if self._attempt(Candidate(table, ((0, n - 1),))).accepted:
            return
        g = 2
        while n >= 2:
            blocks = split_blocks(n, g)
            progressed = False
            for lo, hi in blocks:
                if self._attempt(Candidate(table, ((lo, hi),))).accepted:
                    n -= hi - lo + 1
                    g = min(max(g - 1, 2), n) if n >= 2 else 2
                    progressed = True
                    break
            if not progressed and len(blocks) > 2:
                for lo, hi in blocks:
                    excl = tuple(r for r in ((0, lo - 1), (hi + 1, n - 1)) if r[0] <= r[1])
                    if self._attempt(Candidate(table, excl)).accepted:
                        n = hi - lo + 1
                        g = 2
                        progressed = True
                        break
            if not progressed:
                if g >= n:
                    return
                g = min(2 * g, n)

    def _certify(self) -> None:
        self._phase = "certify"
        while True:
            unresolved: list[str] = []
            restarted = False
            for table in self.engine.project.topo_order:
                for i in range(self.engine.sizes[table]):
                    res = self._attempt(Candidate(table, ((i, i),)), use_cache=False)
                    if res.accepted:
                        restarted = True
                        break
                    if res.outcome.kind is not OutcomeKind.FALSE:
                        unresolved.append(f"{table}[{i}]: {res.outcome.kind.value}")
                if restarted:
                    break
            if restarted:
                continue
            if unresolved:
                self.stats.minimality = Minimality.NOT_VERIFIED
                self.stats.minimality_detail = (
                    f"{len(unresolved)} single-row deletions were not FALSE: "
                    + "; ".join(unresolved[:5])
                )
            else:
                self.stats.minimality = Minimality.VERIFIED
                self.stats.minimality_detail = "every single-row deletion evaluated FALSE"
            return
