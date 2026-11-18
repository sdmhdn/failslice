# Latest record per key: a timestamp tie across two streams

**The story.** An account's current status is the status change with the latest
timestamp, taken from two streams: the app and a nightly import. The streams normally
interleave (app events on even seconds, import events on odd seconds), but one import event
was re-stamped with the time of an app event for the same account. "Latest per account" by
`max(updated_at)` then returns two current rows for that account, and every report keyed by
account counts it twice.

**The data.** 1,000 accounts, 5,000 app events and 5,000 import events (generated).

**The predicate** ([`failslice.yaml`](failslice.yaml)): the `current_status` result has more
rows than distinct accounts.

**Why a combination.** Neither stream alone contains the tie: the reproducer needs the
account, its latest app event, and the import event stamped with the same time.

**The fix is a tie-breaker**, e.g. `ORDER BY updated_at DESC, source, event_id` in a
`row_number()` window. Without a total order, `row_number()` would instead pick one of the
tied rows arbitrarily. FailSlice's determinism check refuses such a query only when its
four runs actually disagree; it can pick the same row every time, so the check is a screen,
not a proof.

## Run it

```bash
uv run python examples/run.py latest_tie
```

## What FailSlice found

<!-- generated:start -->
<!-- Written by `uv run python examples/run.py latest_tie --update-readme`; do not edit by hand. -->

Result: **1-minimal (verified)**, replay **verified**, 11,000 -> 3 rows; the generated pytest test passes (the bug is reproduced).

| table | rows before | rows after |
|---|---:|---:|
| accounts | 1,000 | 1 |
| app_events | 5,000 | 1 |
| import_events | 5,000 | 1 |

Surviving rows:

`accounts`

| account_id | name |
|---|---|
| 242 | account 242 |

`app_events`

| event_id | account_id | status | updated_at |
|---|---|---|---|
| 4242 | 242 | active-4 | 2024-06-01 00:00:08 |

`import_events`

| event_id | account_id | status | updated_at |
|---|---|---|---|
| 4242 | 242 | synced-4 | 2024-06-01 00:00:08 |
<!-- generated:end -->

The row counts above are what this example produces; they are checked by
[`tests/test_examples.py`](../../tests/test_examples.py). The reproducer is the one this
run kept; FailSlice does not claim it is the smallest possible one, or that it is the
cause. The generated pytest bundle is in the run directory (`.failslice/runs/<run-id>/pytest`).
