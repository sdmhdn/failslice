# ADR 0009: Artifacts, manifest and the portable pytest bundle

- Status: accepted (Phase 3 design: Claude draft + independent Codex design + one debate round)
- Date: 2026-11-17

## Decision
- **Finalizer.** `artifacts.finalize(context, result, deadline)` runs after replay, in the parent,
  for both modes. It receives a frozen run context (run id, settings, effective config, versions,
  start time), the `ReductionResult` and the `RunSpec`; it never reopens the original inputs and
  never infers unknown counters (they stay `null`). Steps: CSV copy, pytest bundle (only if the
  replay is `verified`), explain summary, `report.md`, then `manifest.json` written atomically and
  last. Every DuckDB step runs under the cooperative watchdog and every step checks the deadline
  first; a skipped or failed step is recorded as incomplete in the manifest, never hidden. The
  finalizer shares the replay reserve; after the deadline only the report and manifest are
  written (bounded by metadata size).
- **manifest.json** (format 1) is the single authoritative run summary; `result.json` is
  removed (never released). It holds versions (FailSlice, DuckDB, Python), run id and times, the
  effective config and settings, the predicate, inputs (path, sha256, rows), per-table rows
  before/after and schemas, statistics (completed and started evaluations, outcomes, error
  classes, cache hits, acceptances, sweeps, stop reason, elapsed), the candidate status, the
  verification report, minimality, warnings, and an artifact inventory (path, role, bytes,
  sha256; the manifest and the checkpoint excluded).
- **CSV** (`csv/<t>.csv`) is a convenience copy: UTF-8, header, `\N` (unquoted) for NULL, all
  non-NULL strings quoted. Parquet plus the recorded source schema is authoritative.
- **Report wording.** "reduced" by default; "1-minimal (verified)" only when certification
  completed, the replay is verified and the export recorded no lossy round trip. Explain
  observations say "survived reduction", never "needed" or "caused".
- **Pytest bundle** (`<run>/pytest/`): `test_<name>.py`, editable `query.sql`, `reproducer.json`,
  `fixtures/`, and `_failslice_portable.py`, a byte-identical copy of `failslice/portable.py`.
  `portable.py` imports only the standard library and DuckDB and contains outcome
  classification, typed comparison, the expectation codec, the integrity check and the bundle
  runner; `predicate.py` and `verify.py` use it, so test and tool share one implementation.
  Behavioural differential tests still run the bundle in a subprocess without FailSlice.
  - The test loads the companion by path under a unique module name (bundles never share it).
  - Restoring casts are canonicalized with sqlglot at generation time and baked in; the test
    disables extension auto-install/auto-load and external access before running the predicate.
  - Fixture hashes are checked by the test; `query.sql` is meant to be edited.
  - Polarity is one constant in the generated file: `fails` requires a classified TRUE, `passes`
    requires a classified FALSE; ERROR and TIMEOUT fail either way.
  - Inline expectations are stored with a fully tagged JSON codec (no `repr`, `eval`, pickle).
  - `export-test` re-verifies the run directory, stages the bundle and publishes it atomically;
    it refuses an existing destination and has no verification bypass.
- **RunSpec format 3** adds declared primary keys, the effective settings and the input orphan
  counts per FK edge.

## Consequences
The generated test runs anywhere with `pytest` and `duckdb`. Changing `portable.py` changes every
newly generated bundle; the file carries its own version constant.
