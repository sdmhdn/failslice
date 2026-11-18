# ADR 0003: Predicate forms and outcome classes

- Status: accepted (Phase 1 debate with Codex)
- Date: 2026-11-15

## Decision
- Forms: boolean `sql:`; `expect:` (query + expected value/file, default "differs"); `raises:`
  (query + regex search + optional error class). All are a single read-only SELECT.
- Outcomes: TRUE, FALSE, ERROR, TIMEOUT. Only TRUE means "still failing".
- Boolean form: exactly one row and one BOOLEAN column. TRUE → TRUE; FALSE or NULL → FALSE
  (NULL is interpreted only after the type check; SQL WHERE semantics, and aggregates over
  emptied tables return NULL). Zero/multiple rows or columns, non-BOOLEAN type, exceptions →
  ERROR. No scalar-subquery wrapper (it would turn "zero rows" into NULL).
- `raises:` a non-matching error is ERROR (different failure, slippage guard), never TRUE.
- Results are fetched completely before classification. TIMEOUT overrides all forms.
- Expectation files are read once when the engine loads the data and snapshotted into the run directory
  (hashed in the manifest) so the oracle cannot change mid-run and generated tests are self-contained.
- Expectations: exact typed bag equality (NULL = NULL, order ignored unless `ordered`),
  optional absolute float tolerance.
- Determinism check: full data twice through plain views over the base tables (original
  columns only) and twice through the reduction view layer,
  uncached, threads=1; all TRUE with identical signatures, else refuse
  (`--allow-nondeterministic` = loud warning and no 1-minimality claim). Volatile functions rejected.

## Consequences
Users wanting NULL to count as failure write `coalesce(..., true)`. ERROR counts per error
class appear in the report so users can see when the search hit other failures.
