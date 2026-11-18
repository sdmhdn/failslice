# Predicates

The predicate says "the bug is present". It is exactly one read-only DuckDB `SELECT`
(CTEs allowed) that refers to the configured tables by their names.

## Three forms

```yaml
# 1. Boolean: one row, one BOOLEAN column; TRUE means "the bug is present".
predicate:
  sql: SELECT count(*) > 0 FROM orders WHERE customer_id IS NULL

# 2. Expectation: the bug is present when the result differs from the expected one
#    (or, with mode: equals, when it equals it).
predicate:
  expect:
    query: SELECT coalesce(sum(amount), 0) - coalesce(sum(paid), 0) FROM lines
    expected: 0               # or expected_file: expected.csv (CSV or Parquet)
    mode: differs             # differs | equals
    ordered: false            # compare as a bag of rows by default
    float_tolerance: 0.001    # optional absolute tolerance

# 3. Error: the bug is present when the query raises a matching DuckDB error.
predicate:
  raises:
    query: SELECT sum(CAST(raw AS INTEGER)) FROM readings
    match: Could not convert string     # regular expression (re.search)
    error_class: ConversionException    # optional; base classes match too
    flags: [IGNORECASE]                 # optional: IGNORECASE, MULTILINE, DOTALL
```

## Outcomes

Every evaluation is one of:

| outcome | meaning |
|---|---|
| `TRUE` | the bug is present |
| `FALSE` | the query ran and the bug is absent (for `sql:`, NULL also counts as FALSE) |
| `ERROR` | a malformed result (wrong shape or type), an error the predicate does not match, or a comparison that could not be made |
| `TIMEOUT` | the evaluation exceeded `eval_timeout_s` (even if the query finished just as the timer fired) |

Only `TRUE` counts as "still failing". Every configuration FailSlice keeps satisfies *your
predicate*, so make it as specific as the bug: a broad `raises.match` regex can let the
search keep a different error that also matches.

## Comparison rules (`expect:`)

- Bag equality by default (duplicates count, row order ignored); `ordered: true` compares
  row by row. Columns compare by position.
- Numbers compare exactly as rationals: integers and decimals are never rounded through
  binary floats; a float is taken at its shortest decimal form, so `0.1` equals
  `DECIMAL '0.1'`. NaN equals NaN; infinities equal only themselves.
- Booleans never equal numbers, at any depth. Lists, structs and maps compare element by
  element with the same rules (map keys too).
- NULL equals NULL. A scalar `expected` (including `expected: null`) requires a 1x1 result.
- With `float_tolerance`, unordered results are matched with a bipartite matching, so the
  result does not depend on row order.
- An expectation file is read once when the run starts; its snapshot is what is evaluated
  and exported.

Tip: with `mode: differs`, an emptied table usually "differs" too, and the search may
reach a meaningless configuration. Prefer queries whose correct value does not depend on
the amount of data, e.g. a difference of two sums that must be 0.

## Determinism check

Before reducing (and in `failslice check`), the predicate runs four times on the full data,
twice on plain views and twice through the reduction views. All four must be TRUE with the
same result signature. Otherwise the run stops (`allow_nondeterministic` downgrades this
to a warning and disables the minimality claim). This is a screen, not a proof; the
independent replay is the real check.

For `raises`, the signature is the concrete error class plus the text your `match` regex
matched, not the whole message. With several bad values, DuckDB names whichever one it
reaches first, and that can change between runs; the declared failure stays the same
([ADR 0012](decisions/0012-examples-and-raises-signature.md),
[example](../examples/date_parse_crash/)). A regex that matches the varying part (such as
the bad value itself) is still reported as nondeterministic.

## What is rejected

The predicate is validated before anything is loaded:

- anything but a single `SELECT`: DDL, DML, `COPY`, `ATTACH`, `INSTALL`, `LOAD`, `PRAGMA`,
  `SET`, `CALL`, transactions;
- table functions and file readers (`read_csv`, `read_parquet`, `glob`, `read_text`, ...);
- references to anything but the configured tables and CTEs, and schema-qualified names;
- volatile functions: a hand-written list (`random()`, `now()`, `uuid()`, ...) plus every
  function DuckDB's own catalog marks volatile or query-dependent (e.g.
  `current_schema()`), and built-in macros that call one of them. `error()` stays allowed.

Validation is a second line of defence: after loading, DuckDB's external file access is
switched off, which is the actual barrier.
