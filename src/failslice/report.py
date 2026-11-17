"""``report.md``: a human-readable summary of one run.

Wording rules (CLAUDE.md): "reduced" by default, "1-minimal (verified)" only as the
manifest's label says; observations, never causes; no claim of a smallest possible result.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from failslice.artifacts import Manifest
    from failslice.explain import Explanation

__all__ = ["render_report"]

_STATUS_TEXT = {
    "verified": "verified: the predicate is TRUE in an independent replay and no new "
    "dangling references were found",
    "replay-failed": "replay failed: the predicate was not TRUE in the independent replay",
    "integrity-failed": "integrity check failed in the independent replay",
    "unverified": "not verified: the independent replay did not finish",
}


def _predicate_text(predicate: dict[str, object]) -> str:
    kind = predicate.get("kind")
    query = str(predicate.get("sql") if kind == "sql" else predicate.get("query"))
    lines = [f"Form: `{kind}`", "", "```sql", query.strip(), "```"]
    if kind == "expect":
        mode = predicate.get("mode")
        lines.append("")
        source = "file" if predicate.get("expected_file") else "inline"
        lines.append(
            f"The failure is present when the result {mode} the expectation ({source}); "
            f"ordered: {predicate.get('ordered')}, tolerance: {predicate.get('float_tolerance')}."
        )
    if kind == "raises":
        lines.append("")
        lines.append(
            f"The failure is present when the query raises an error matching "
            f"`{predicate.get('match')}`"
            + (
                f" of class `{predicate.get('error_class')}`."
                if predicate.get("error_class")
                else "."
            )
        )
    return "\n".join(lines)


def _n(value: object) -> str:
    return "unknown" if value is None else str(value)


def render_report(manifest: Manifest, explanation: Explanation | None) -> str:
    """Render ``report.md`` from the manifest (and the explanation, if computed)."""
    m = manifest
    out: list[str] = [f"# FailSlice report: run {m.run_id}", ""]
    if m.status == "cancelled":
        replay = (m.verification or {}).get("status")
        if replay and replay not in ("cancelled", "unverified"):
            history = (
                f"it was cancelled after the independent replay had finished ({replay}), "
                "while artifacts were being written, so some artifacts may be missing"
            )
        elif replay == "unverified":
            history = (
                "the independent replay did not complete (skipped or timed out), and it was "
                "cancelled while artifacts were being written"
            )
        else:
            history = "nothing was recovered, replayed or verified"
        out += [
            f"**Cancelled.** The run was cancelled by the caller; {history}. "
            "No reproducer is claimed.",
            "",
        ]
    elif m.status == "no-candidate":
        out += [
            "**No reproducer.** The run stopped before any reduced configuration was "
            "confirmed to make the predicate TRUE.",
            "",
        ]
    else:
        v = m.verification or {}
        status = str(v.get("status", "unverified"))
        out += [
            f"**Result: {m.minimality}.** Replay: {_STATUS_TEXT.get(status, status)}.",
            "",
        ]
        if m.minimality == "1-minimal (verified)":
            out.append(
                "1-minimal means: deleting any single remaining row (together with the rows "
                "that would lose their parent, i.e. cascade deletion) makes the predicate "
                "FALSE. It is not a claim that no smaller reproducer exists."
            )
        elif m.minimality_detail:
            out.append(f"Minimality not verified: {m.minimality_detail}.")
        out.append("")
    out += ["## Rows", "", "| table | before | after |", "|---|---:|---:|"]
    for name, t in m.tables.items():
        before = "?" if t.get("rows_before") is None else f"{t['rows_before']:,}"
        after = "-" if t.get("rows_after") is None else f"{t['rows_after']:,}"
        out.append(f"| {name} | {before} | {after} |")
    if m.rows_before is not None:
        after_total = "-" if m.rows_after is None else f"{m.rows_after:,}"
        out.append(f"| **total** | **{m.rows_before:,}** | **{after_total}** |")
    out += ["", "## Predicate", "", _predicate_text(m.config.get("predicate", {})), ""]
    fks = m.config.get("foreign_keys") or []
    out += ["## Foreign keys", ""]
    if fks:
        out += [f"- `{fk}`" for fk in fks]
        out.append("")
        v_status = (m.verification or {}).get("status")
        if v_status == "verified":
            out.append(
                "The independent replay found no new dangling references; rows that already "
                "referenced a missing parent in the input (input orphans) may survive and are "
                "listed in `orphans.parquet`."
            )
        elif v_status == "integrity-failed":
            detail = (m.verification or {}).get("detail") or "see the verification report"
            out.append(f"The independent replay's integrity check failed: {detail}.")
        else:
            out.append("Referential integrity was not verified by an independent replay.")
    else:
        out.append("None declared.")
    out.append("")
    if explanation is not None:
        out += ["## Observations", "", explanation.render_text().rstrip(), ""]
    if m.stats is not None:
        s = m.stats
        outcomes = (
            ", ".join(f"{k}: {n}" for k, n in sorted(s["outcomes"].items())) or "none"
            if s.get("outcomes") is not None
            else "unknown (the reduction process stopped before reporting them)"
        )
        out += [
            "## Search",
            "",
            f"- predicate evaluations: {s['evaluations']}"
            + (
                f" (started: {s['evaluations_started']})"
                if s.get("evaluations_started") is not None
                else ""
            ),
            f"- outcomes: {outcomes}",
            f"- cache hits: {_n(s['cache_hits'])}, accepted reductions: {_n(s['accepted'])}, "
            f"sweeps: {_n(s['sweeps'])}",
            f"- stop reason: {s['stop_reason']}",
            f"- elapsed: {m.elapsed_s:.1f} s in total",
            "",
        ]
    if m.notes:
        out += ["## Notes and warnings", ""] + [f"- {n}" for n in m.notes] + [""]
    out += ["## Reproduce", ""]
    if m.status == "reduced":
        out += [
            "- replay this run: `failslice verify <this directory>`",
            "- reduced tables: `tables/*.parquet` (source of truth), `csv/*.csv` (convenience "
            "copy; types are not preserved)",
        ]
        if any(st.name == "pytest" and st.status == "done" for st in m.steps):
            out.append(
                "- regression test: `pytest pytest/` (needs only pytest and duckdb; edit "
                "`pytest/query.sql` after fixing your pipeline)"
            )
    else:
        out.append("- nothing was exported; see the notes above and adjust the budgets.")
    incomplete = [st for st in m.steps if st.status != "done"]
    if incomplete:
        out += ["", "## Incomplete steps", ""]
        out += [f"- {st.name}: {st.status} ({st.detail})" for st in incomplete]
    out += [
        "",
        "---",
        f"FailSlice {m.versions.get('failslice')}, DuckDB {m.versions.get('duckdb')}, "
        f"Python {m.versions.get('python')}. FailSlice isolates the rows; it does not "
        "determine the root cause.",
        "",
    ]
    return "\n".join(out)
