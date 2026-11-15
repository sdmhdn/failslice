# ADR 0001: Support Python 3.11 and newer

- Status: accepted
- Date: 2026-11-15

## Context

The brief asks for Python 3.10+ and to check whether 3.10 is still worth supporting.

Checked on 2026-11-15 (PyPI JSON API):

- Python 3.10 reached end of life in October 2026.
- `duckdb` 2.0.0 (current stable) requires Python >= 3.11.
- `hypothesis` 6.170.x (current stable) requires Python >= 3.11.

## Decision

`requires-python = ">=3.11"`. CI covers 3.11 through 3.14.

## Consequences

- We can use the latest DuckDB and Hypothesis without pinning old versions.
- `tomllib`, `typing.Self` and exception groups are available from the standard library.
- Users stuck on 3.10 cannot install FailSlice; they are on an unsupported interpreter anyway.
