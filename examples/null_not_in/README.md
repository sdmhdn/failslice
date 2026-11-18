# NULL semantics: one NULL empties a `NOT IN` list

**The story.** Marketing e-mails go to orders whose customer is not on the suppression
(unsubscribe) list. The list holds customers 0-99, plus one record from a web form whose
e-mail matched no account, so its `customer_id` is NULL. The mailing query uses
`customer_id NOT IN (SELECT customer_id FROM suppressions)`. With a NULL in the list, that
condition is never TRUE, and the campaign silently selects nothing.

**The data.** 1,000 customers, 5,000 orders, 101 suppression records (generated).
`suppressions.customer_id` references `customers`; NULL means "no account".

**The predicate** ([`failslice.yaml`](failslice.yaml)): the number of orders selected with
`NOT EXISTS` (the intended rule) minus the number selected with `NOT IN` *differs from* 0.

**Why a combination.** The NULL record alone does nothing; the bug needs an order whose
customer is *not* suppressed (so `NOT EXISTS` selects it), that customer, and the NULL
record (so `NOT IN` does not).

## Run it

```bash
uv run python examples/run.py null_not_in
```

## What FailSlice found

<!-- generated:start -->
<!-- Written by `uv run python examples/run.py null_not_in --update-readme`; do not edit by hand. -->

Result: **1-minimal (verified)**, replay **verified**, 6,101 -> 3 rows; the generated pytest test passes (the bug is reproduced).

| table | rows before | rows after |
|---|---:|---:|
| customers | 1,000 | 1 |
| orders | 5,000 | 1 |
| suppressions | 101 | 1 |

Surviving rows:

`customers`

| customer_id | email |
|---|---|
| 750 | customer750@example.com |

`orders`

| order_id | customer_id | ordered_on |
|---|---|---|
| 4750 | 750 | 2024-09-07 |

`suppressions`

| suppression_id | customer_id | source |
|---|---|---|
| 101 | NULL | web form (no matching account) |
<!-- generated:end -->

The row counts above are what this example produces; they are checked by
[`tests/test_examples.py`](../../tests/test_examples.py). The reproducer is the one this
run kept; FailSlice does not claim it is the smallest possible one, or that it is the
cause. The generated pytest bundle is in the run directory (`.failslice/runs/<run-id>/pytest`).
