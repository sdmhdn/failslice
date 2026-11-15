# ADR 0002: FK-consistent reduction by cascade deletion (greatest intact subset)

- Status: accepted (Phase 1 debate with Codex)
- Date: 2026-11-15

## Context
Every configuration passed to the predicate must be referentially intact, the search must
terminate within budgets and be reproducible. Candidates: upward closure (re-add referenced
parents) vs cascade deletion (deleting a parent deletes its dependants).

## Decision
- FK references are by value; parent keys need not be unique. A child row is supported if its
  FK is NULL, some kept parent matches, or it has an input-orphan exemption for that edge.
- Repair `C(K, D) = G(K \ D)`, the unique greatest intact subset, computed in one
  topological pass (parents first). Intact sets are closed under union, so `G` is well defined.
- Search: per-table ddmin (complement-first, then subsets, granularity doubling) over a seeded
  permutation of row ids; whole-table deletion tried first; sweeps in parent-first order until a
  sweep accepts nothing. Only TRUE outcomes are accepted.
- 1-minimality: `P(K) = TRUE` and `P(C(K,{r})) != TRUE` for every kept `r`. Certified only by
  a complete, uncached singleton sweep with no ERROR/TIMEOUT; a TRUE singleton is accepted and
  certification restarts over all tables.
- Integrity is **relative**: reduction never creates a dangling reference the input did not
  have. Input orphans are exempt per `(FK edge, original row id)`, reported by `check`, and
  remain removable. Surviving exemptions are exported (`orphans.parquet`) so replay can check
  integrity without the original data.
- Determinism: same inputs + settings + seed give the same result when the run stops without a
  time-based event (wall-clock stop or TIMEOUT outcome).

## Why not upward closure
With duplicate parent keys a child needs one of several parents; the smallest intact superset
is not unique and re-adding all matches over-keeps rows. Removals of "locked" parents become
silent no-ops. Cascade gives a unique, simple repair and large early deletions.

## Consequences
- Classic ddmin guarantees do not carry over (cascades make blocks overlap); hence the explicit
  certification pass and empirical cost estimates.
- The brief's absolute integrity wording ("every non-NULL FK value points to a kept parent")
  holds exactly when the input itself has no orphans; with orphans in the input we guarantee
  "no new orphans" and say so in the docs.
- Self-referencing FKs are cycles and are out of scope for v1.
