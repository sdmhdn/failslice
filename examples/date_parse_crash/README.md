# Crash: a depot switches date format

**The story.** Each carrier reports shipment times as text in its own documented style
(`iso`, `eu` = day/month, `us` = month/day with AM/PM), and the staging query parses each
row according to its carrier's style. One Globex depot (style `eu`) started writing
US-style dates in March. `03/13/2024` has no month 13, so the nightly load fails with
`Could not parse string ...`, naming whichever bad value DuckDB reaches first.

**The data.** 4 carriers and 30,003 shipment events (generated), three of them malformed.

**The predicate** ([`failslice.yaml`](failslice.yaml)): the staging query *raises* an
`InvalidInputException` matching `Could not parse string`.

**Why a combination.** The text `03/13/2024 08:00` is a valid date for `us` carriers; it
only fails together with its carrier's `eu` style, which comes from the joined `carriers`
row. Any one of the three malformed shipments is enough; FailSlice keeps one.

**Several offending values.** The error message names the first bad value DuckDB happens to
reach, and that can differ between runs. FailSlice treats a `raises` predicate as stable
when the concrete error class and the text your `match` regex matched are the same (see
[predicates](../../docs/predicates.md)), so a realistic file with several bad values can be
reduced.

## Run it

```bash
uv run python examples/run.py date_parse_crash
```

## What FailSlice found

<!-- generated:start -->
<!-- Written by `uv run python examples/run.py date_parse_crash --update-readme`; do not edit by hand. -->

Result: **1-minimal (verified)**, replay **verified**, 30,007 -> 2 rows; the generated pytest test passes (the bug is reproduced).

| table | rows before | rows after |
|---|---:|---:|
| carriers | 4 | 1 |
| shipments | 30,003 | 1 |

Surviving rows:

`carriers`

| carrier_id | name | date_style |
|---|---|---|
| 2 | Globex Logistics | eu |

`shipments`

| shipment_id | carrier_id | shipped_at |
|---|---|---|
| 30000 | 2 | 03/13/2024 08:00 |
<!-- generated:end -->

The row counts above are what this example produces; they are checked by
[`tests/test_examples.py`](../../tests/test_examples.py). The reproducer is the one this
run kept; FailSlice does not claim it is the smallest possible one, or that it is the
cause. The generated pytest bundle is in the run directory (`.failslice/runs/<run-id>/pytest`).
