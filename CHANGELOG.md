# Changelog

All notable changes to this project are documented here. The project is pre-alpha and
has no releases yet.

## Unreleased

- Core engine (Phase 2): config, loading, predicate outcomes, cascade ddmin, negative
  cache, budgets, supervisor with checkpoints and recovery, independent replay.
- Independent audit fixes: exact typed result comparison, glob-safe paths, typed
  run-directory contract `spec.json` format 2 (ADR 0008) with ENUM type restoration in
  replay, validated orphan metadata, supervisor that cannot be blocked by a stalled
  child or a slow progress callback, statistics kept for unconfirmed and recovered runs,
  volatile functions taken from DuckDB's catalog, `expected: null` support.
- README in English and Persian; runnable quickstart example.
