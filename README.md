# FailSlice

> Status: early development (pre-alpha). Nothing is released yet.

FailSlice shrinks a multi-table dataset (CSV / Parquet) down to a small set of rows that
still triggers a data bug, keeps foreign keys intact, verifies the result independently,
and exports a ready-to-run pytest regression test. An MCP server exposes the same loop to
AI agents.

FailSlice builds on established ideas (delta debugging / ddmin, database subsetting); it
combines them for predicate-guided, multi-table, FK-consistent row reduction on local files.
It reports a result as "1-minimal (verified)" only when that check actually ran; otherwise
the result is "reduced".

Full documentation will follow as the project takes shape.

## License

MIT, see [LICENSE](LICENSE).

## Author

Created and maintained by Saeed Mahdian (<saeed.mahdian@gmail.com>).
