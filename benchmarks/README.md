# Benchmarks

```bash
uv run python benchmarks/run.py            # measure; writes results.json and the README table
uv run python benchmarks/run.py --check    # README table == results.json? (run in the tests)
```

## What is measured

For each scenario in [`examples/`](../examples/) (plus TPC-H at scale factor 0.1, generated
into `benchmarks/.work/`):

| column | meaning |
|---|---|
| input rows / output rows | rows over all tables before and after reduction |
| kept | output rows / input rows |
| evaluations | predicate evaluations of the reduction: the determinism check (4), the search and the certification; cache hits, `failslice check` and the replay are not counted |
| wall time | median over `--repeat` runs (default 3) of `failslice.reduce(project, isolate=True)`: loading, the determinism check, reduction, certification, export, replay in a fresh process and all artifacts. Data generation and running the generated pytest test are excluded |
| minimality | the run's label; `1-minimal (verified)` only when certification completed and the replay verified |

`results.json` keeps every individual wall time and the environment (CPU, memory, OS,
Python, DuckDB, FailSlice version and git revision). Repeated runs must agree on the result,
or nothing is recorded.

## What is checked

The tests re-run every example and require the row counts and minimality label in
`results.json` (and in each example's README) to match a fresh run, and the README table to
match `results.json`. Wall times and evaluation counts are recorded, not checked: they
depend on the machine.

## What the numbers do not say

They are one machine's measurements on small, generated inputs with a fast predicate. Most
of the time of a small run is fixed cost (starting the reduction process, the replay
process, writing artifacts). Real predicates that take seconds per evaluation make a run
roughly *evaluations x predicate time*. FailSlice's result is a 1-minimal reproducer (no
single row can be removed), not the smallest possible one.
