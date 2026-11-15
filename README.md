# FailSlice

> Status: early development (pre-alpha). Nothing is implemented or released yet; this
> section describes the planned v1.

The goal: FailSlice will shrink a multi-table dataset (CSV / Parquet) down to a small set
of rows that still triggers a data bug, keep foreign keys intact, verify the result
independently, and export a ready-to-run pytest regression test. An MCP server will expose
the same loop to AI agents.

FailSlice builds on established ideas (delta debugging / ddmin, database subsetting); it
aims to combine them for predicate-guided, multi-table, FK-consistent row reduction on
local files. The planned reporting rule: a result is called "1-minimal (verified)" only
when that check actually ran; otherwise it is "reduced".

Full documentation will follow as the project takes shape.

## License

MIT, see [LICENSE](LICENSE).

## Author

Created and maintained by Saeed Mahdian (<saeed.mahdian@gmail.com>).
