# Foreign keys and minimality

## Semantics

- A foreign key `child.col -> parent.col` is a reference **by value**; the parent column
  does not have to be unique (duplicate keys are often the bug).
- NULL in the child column means "no reference".
- Foreign keys are single-column and must form an acyclic graph (no self-references).
- Primary keys are informational only and never enforced.

## Input orphans

Some child rows may already reference a missing parent in the input. FailSlice records them
per foreign key and keeps them as **exemptions**: they may survive in the output, and they
are listed in `orphans.parquet`. They are never "repaired", because the orphan may be the
bug. `failslice check` reports their number per foreign key.

## The guarantee: relative integrity

Every configuration the predicate ever sees, and the exported result, has **no new
dangling references**: every non-NULL foreign-key value of a kept child row has a kept
parent row, unless that row was an input orphan for that foreign key.

The independent replay checks this again from the exported files only.

## Cascade deletion

The search removes ranges of rows table by table, parents first. When parent rows are
removed, child rows that would lose their only matching parent are removed with them
(transitively, through the whole FK graph). A candidate is accepted only if the predicate
is freshly evaluated TRUE on it; negative results are cached as search hints only.

## What "1-minimal (verified)" means

After the search reaches a fixed point, the certification pass (on by default) tries
deleting each remaining row on its own, with cascade. The result is labelled
**1-minimal (verified)** only if:

- every such single-row deletion made the predicate FALSE (not ERROR or TIMEOUT),
- the pass ran to completion within the budgets,
- the independent replay verified the result, and
- the export recorded no lossy type round trip.

Otherwise the result is "reduced, minimality not verified", and the report says why.

1-minimal is a local property under cascade deletion. It does not mean that no smaller
reproducer exists (finding the smallest one is a search over all subsets), and it says
nothing about *why* the rows cause the bug.

## Limits

- Composite (multi-column) and cyclic foreign keys are not supported.
- All data is loaded into memory (in-process DuckDB).
