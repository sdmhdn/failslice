# ADR 0008: A typed, versioned run-directory contract (`RunSpec`)

- Status: accepted (independent audit after Phase 2, debated with Codex)
- Date: 2026-11-17

## Context
`Engine.export` wrote `spec.json` as an untyped dictionary and `verify` interpreted it on its
own. Phase 3 adds more consumers (manifest, report, generated pytest, CLI, MCP). Two
concrete problems showed up: the exported Parquet schema can differ from the loaded schema
(DuckDB stores `ENUM` as `VARCHAR`), so replay failed for valid reproducers; and the result
carried no provenance, so later artifacts would have had to reopen the (mutable) originals.

## Decision
- `failslice/runspec.py` defines `RunSpec` (pydantic, frozen, `extra="forbid"`), format 2.
  `Engine.export` writes it; `verify` and all Phase 3/4 consumers read it and nothing else.
- Each table records both the exported and the source column types. Replay loads the
  Parquet files, disables external access, then casts differing columns back to the source
  type (so the predicate sees the same types) and checks the restored schema.
- The export reads each table back and compares it (after restoring types) with the kept
  rows; a lossy round trip is a `warning` in the spec and in the result notes, not hidden.
- Input provenance (path, sha256, loaded rows) is part of the spec; `ReductionResult.spec`
  exposes it.
- Every path in the spec is resolved inside the run directory and must be free of glob
  characters.

## Consequences
Format 1 run directories (written before this change) are refused with a clear message;
there were no releases, so no migration is provided.
