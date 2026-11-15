# ADR 0004: Negative-only result cache with cheap fingerprints

- Status: accepted (Phase 1 debate with Codex)
- Date: 2026-11-15

## Context
Measured on DuckDB 2.0 at 800k kept ids: exact key (sha256 of ordered id list) ≈ 135 ms,
about as much as one evaluation; `(count, sum(hash(rid))::HUGEINT, bit_xor(hash(rid)))` ≈ 2 ms.

## Decision
- Cache only FALSE and ERROR outcomes, keyed by a namespace (engine + fingerprint algorithm
  versions, input hashes, schema, FKs, exact predicate spec, comparison rules, settings) and the
  per-table fingerprint of the repaired configuration.
- Never cache TRUE or TIMEOUT. Cached negatives are search hints only: determinism check,
  certification and replay bypass the cache.

## Consequences
A fingerprint collision can only make the search skip one deletion (lower reduction quality);
it can never make us accept a configuration that was not freshly evaluated TRUE.
