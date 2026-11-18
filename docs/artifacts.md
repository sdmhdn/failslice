# Run artifacts

Each `failslice reduce` writes a run directory (default `.failslice/runs/<run-id>/` next
to the config):

| file | content |
|---|---|
| `report.md` | human-readable summary: result, rows before/after, predicate, foreign keys, observations, search statistics, notes, how to reproduce |
| `manifest.json` | the authoritative summary (below) |
| `tables/<t>.parquet` | the kept rows, in original order (source of truth) |
| `csv/<t>.csv` | convenience copy: UTF-8, header, unquoted `\N` for NULL, non-NULL text quoted; types are not preserved |
| `pytest/` | the generated regression test (only after a verified replay) |
| `explain.json` | observations on the surviving rows |
| `spec.json` | the typed, versioned contract of the reproducer, which replay and all artifacts read |
| `rowmap.parquet` | output row -> original row number in the input file |
| `orphans.parquet` | input orphans that survive (allowed dangling references) |
| `expected/expected.parquet` | snapshot of the expectation file, if one is used |
| `checkpoint.json` | last confirmed configuration and its statistics (recovery of a killed run) |

## manifest.json

Versions (FailSlice, DuckDB, Python), run id and times, the effective config and settings,
inputs (path, sha256, rows; each input is hashed before and after loading), rows and
schemas per table before and after, statistics (completed and started evaluations,
outcomes, error classes, cache hits, accepted reductions, sweeps, stop reason), status,
minimality label and detail, the verification report, notes, the status of every
finalization step, and every file of the run directory with its size and sha256 (except
`manifest.json` itself and the supervisor's checkpoint). Numbers that are unknown
(for example after a crashed child) are `null`, never zero.

If the time budget runs out during finalization, the report and the manifest are still
written and the skipped steps are listed. Every SQL statement of the finalizer runs under
the deadline; Python-side work (copies, hashing) is not interruptible, so there is no hard
wall-clock bound on this last step.

## Independent replay

`failslice verify` (and the last step of `reduce`) starts a new Python process that builds
a fresh DuckDB database only from the files `spec.json` names (each must resolve inside the
run directory and be a regular file), switches off external access, restores source column
types that Parquet cannot store (e.g. `ENUM`), checks referential integrity with the
recorded orphan exemptions, and runs the predicate uncached.

| status | meaning |
|---|---|
| `verified` | predicate TRUE in the replay and no new dangling references |
| `replay-failed` | the predicate was not TRUE in the replay (or the run directory is invalid) |
| `integrity-failed` | new dangling references, or malformed orphan metadata |
| `unverified` | the replay did not finish in time |

## explain

`failslice explain RUN_DIR` reports observations on the reduced data: rows and NULLs per
table, duplicated values of declared primary keys, and per foreign key the NULL
references, surviving input orphans, referenced keys with more than one parent row, and
per key the number of pairs the declared equality join produces. For the quickstart
example it shows that order 4242 has two parent rows. These are observations about the rows
that survived reduction, not a diagnosis: FailSlice does not know which join or column the
predicate depends on.

## The generated test

The bundle (`pytest/`, or a new directory from `failslice export-test --out`) contains:

- `test_<name>.py`: the test; the polarity is the constant `EXPECT` at the top;
- `query.sql`: the predicate query, meant to be edited after you fix your pipeline;
- `reproducer.json`: predicate form and parameters, schemas, foreign keys, fixture hashes;
- `fixtures/tables/*.parquet` and `fixtures/orphans.parquet`;
- `_failslice_portable.py`: a byte-identical copy of the module FailSlice itself uses to
  classify outcomes and compare results.

It needs only `pytest` and `duckdb`, contains no absolute paths, never reads your original
data, and checks fixture hashes, schemas and referential integrity before running the
query.

- `--expect fails` (default): the test passes while the bug reproduces.
- `--expect passes`: the test passes once the bug is gone; put the fixed SQL into
  `query.sql`.
- Any other outcome (an unrelated error, a malformed result, a timeout) fails the test
  under either polarity.
- The default name `reproducer_<hash>` depends on the data, predicate and polarity, so
  bundles of different runs can be collected in one pytest session (pytest needs distinct
  test-module names; exporting the same run twice with the same polarity needs `--name`).
- The bundle is checked before it is published and an existing destination is never
  overwritten. A tampered `reproducer.json` cannot read outside the bundle: its expressions
  run only after external access is switched off.
