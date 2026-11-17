# Configuration

A project is described by `failslice.yaml`. Relative paths are resolved against the
directory of the config file.

```yaml
version: 1
tables:
  customers: { path: data/customers.csv, primary_key: id }
  orders:    { path: data/orders.parquet, primary_key: order_id }
  lines:     { path: data/lines.parquet }
foreign_keys:
  - orders.customer_id -> customers.id
  - lines.order_id -> orders.order_id
predicate:
  sql: |
    SELECT (SELECT sum(l.amount) FROM lines l JOIN orders o USING (order_id))
        <> (SELECT sum(amount) FROM lines)
settings: { seed: 0, max_evals: 5000, wall_clock_s: 300 }
```

## Tables

| key | meaning |
|---|---|
| `path` | CSV, Parquet or `.duckdb` file. It must resolve (symlinks followed) inside the allowed root, which defaults to the config's directory (`--root` changes it). Paths containing glob characters `* ? [ ] { }` are refused, because DuckDB would expand them. |
| `format` | `csv`, `parquet` or `duckdb`; inferred from the extension when omitted |
| `source_table` | table name inside a `.duckdb` file (default: the configured name). Only physical tables are accepted, never views. |
| `primary_key` | informational only, never enforced: duplicate keys are often the bug |

Table names must be plain SQL identifiers and may not start with `__fs`.

## Foreign keys

`child.column -> parent.column`, single column, by value (the parent column does not need
to be unique). NULL means "no reference". The graph must be acyclic; a self-reference is a
cycle. See [Foreign keys and minimality](foreign-keys.md).

## Predicate

One of `sql`, `expect` or `raises`; see [Predicates](predicates.md).

## Settings

| key | default | meaning |
|---|---|---|
| `seed` | 0 | order in which rows are tried; same inputs, settings and seed give the same result (unless a run is stopped by time or an evaluation times out) |
| `max_evals` | 5000 | maximum predicate executions (cache hits are free) |
| `wall_clock_s` | 900 | total time budget, including export, replay and artifacts |
| `eval_timeout_s` | 30 | limit for one predicate execution |
| `minimality` | true | run the certification pass after the search |
| `allow_nondeterministic` | false | reduce even if the predicate is unstable on the full data (then no minimality claim is made) |
| `threads` | 1 | DuckDB threads (1 for reproducibility) |

Settings can be overridden per run: `failslice reduce --seed 1 --max-evals 2000`, or
`project.with_settings(seed=1)` in Python.

## Starting from a folder: `failslice init`

```bash
failslice init path/to/data          # writes path/to/data/failslice.yaml
```

- Every `.csv`, `.parquet` and `.pq` file in the folder (not recursive) becomes a table.
- Primary and foreign keys are written **commented out, as suggestions**, each with a
  confidence (`high`, `medium`, `low`) and its evidence: rows examined, NULLs, duplicates,
  matched and unmatched values. Review them and uncomment the correct ones.
- Only identifier-like columns are considered (`id`, `<table>_id`, `*_id`, `*_key`,
  `*_code`, `uuid`). A parent key with duplicates is still suggested, with the number of
  duplicated values, because duplicates are often the bug.
- By default the first 10,000 rows of each file are read; `--full-scan` reads everything.
  Suggestions based on a sample are always `low`.
- The predicate is the placeholder `SELECT false`; `failslice check` reports "never TRUE"
  until you replace it.
- An existing config is never overwritten.
