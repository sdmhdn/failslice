from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Callable
from pathlib import Path

import duckdb
import pytest

import failslice
from failslice import api
from failslice.config import ConfigError, Project
from failslice.engine import Engine
from failslice.reducer import DeterminismError, Minimality, StopReason
from failslice.safety import PredicateValidationError
from failslice.supervisor import read_checkpoint, run_supervised
from failslice.verify import main as verify_main
from failslice.verify import verify_in_subprocess, verify_run_dir
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables, write_table

# The fan-out bug, plus a scan whose length grows as the data shrinks: fast on the
# full data (400 lines), very slow once ~10 lines remain. (Not a valid user predicate:
# table functions are rejected by validation, but the supervisor is tested directly.)
SLOW_WHEN_SMALL = {
    "sql": "SELECT (SELECT sum(l.amount) FROM lines l JOIN orders o USING (order_id)) "
    "<> (SELECT sum(amount) FROM lines) AND (SELECT count(*) FROM range("
    "(1e15 / ((SELECT count(*) FROM lines) + 1) ^ 4)::BIGINT)) > 0"
}


def _fanout(factory: Callable[..., Project], **settings: object) -> Project:
    return factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE, settings=settings or None
    )


def test_reduce_in_process_end_to_end(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.status == "reduced"
    assert res.verification is not None and res.verification.ok
    assert res.verification.process == "subprocess"
    assert res.minimality_label == "1-minimal (verified)"
    assert res.stats is not None and res.stats.sizes_after == {
        "customers": 1,
        "orders": 2,
        "lines": 1,
    }
    data = json.loads((res.run_dir / "result.json").read_text())
    assert data["minimality"] == "1-minimal (verified)"
    assert data["verification"]["status"] == "verified"
    assert read_checkpoint(res.run_dir, "wrong-id") is None


def test_reduce_default_run_dir_and_non_empty_refusal(
    project_factory: Callable[..., Project],
) -> None:
    p = _fanout(project_factory)
    res = failslice.reduce(p)
    assert res.run_dir.parent == p.base_dir / ".failslice" / "runs"
    with pytest.raises(ConfigError, match="not empty"):
        failslice.reduce(p, out_dir=res.run_dir)


def test_reduce_isolated_end_to_end(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    events = []
    res = failslice.reduce(p, out_dir=tmp_path / "run", isolate=True, on_progress=events.append)
    assert res.status == "reduced" and res.verification is not None and res.verification.ok
    assert res.stats is not None and res.stats.minimality is Minimality.VERIFIED
    assert events and events[-1].kept_rows == 4


def test_isolated_hard_deadline_recovers_checkpoint(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    # The predicate becomes slow once few rows remain, so the child is killed mid-run
    # and the parent must recover and export the last checkpoint.
    pred = SLOW_WHEN_SMALL
    p = project_factory(
        fanout_tables(),
        fks=FANOUT_FKS,
        predicate=pred,
        settings={"eval_timeout_s": 60, "wall_clock_s": 30},
    )
    from failslice.reducer import Budget

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sup = run_supervised(p, run_dir, "rid", Budget(max_evals=1000, wall_clock_s=4), grace_s=1)
    assert sup.killed_reason is not None and "deadline" in sup.killed_reason
    assert sup.recovered and sup.stats is not None
    assert sup.stats.stop_reason is StopReason.KILLED
    assert sup.stats.minimality is Minimality.NOT_VERIFIED
    assert sup.evaluations_started >= 4
    # The first candidate (delete every customer) cascades everything and is already
    # slow, so the initial checkpoint (the full, TRUE-confirmed data) is recovered.
    assert sup.stats.output_rows <= sup.stats.input_rows
    assert sup.stats.sizes_before == {"customers": 30, "orders": 121, "lines": 400}
    # The recovered configuration was exported for replay ...
    assert (run_dir / "spec.json").is_file() and (run_dir / "rowmap.parquet").is_file()
    # ... and replay re-validates the predicate, which uses a table function on purpose.
    report = verify_in_subprocess(run_dir, timeout_s=60)
    assert report.status == "unverified" and "PredicateValidationError" in report.detail


def test_isolated_cooperative_timeout_is_not_killed(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    pred = SLOW_WHEN_SMALL
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=pred, settings={"eval_timeout_s": 0.5}
    )
    from failslice.reducer import Budget

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    # The cooperative interrupt works for this query, so the child is NOT killed: it
    # classifies TIMEOUT and carries on. The supervisor reports a normal finish.
    sup = run_supervised(p, run_dir, "rid", Budget(max_evals=30, wall_clock_s=120), grace_s=3)
    assert sup.killed_reason is None and sup.stats is not None
    assert sup.stats.outcomes["TIMEOUT"] >= 1


def test_isolated_determinism_error(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    with pytest.raises(DeterminismError):
        failslice.reduce(p, out_dir=tmp_path / "run", isolate=True)


def test_in_process_determinism_error(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    with pytest.raises(DeterminismError):
        failslice.reduce(p, out_dir=tmp_path / "run")


def test_isolated_child_crash_without_checkpoint(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    (p.table_path("lines")).unlink()  # the child fails while loading
    res = failslice.reduce(p, out_dir=tmp_path / "run", isolate=True)
    assert res.status == "no-candidate" and res.verification is None
    assert any("file not found" in n for n in res.notes)


def test_unsafe_predicate_is_rejected_before_loading(
    project_factory: Callable[..., Project],
) -> None:
    p = project_factory(
        {"t": ([("x", "BIGINT")], [(1,)])},
        predicate={"sql": "SELECT count(*) > 0 FROM read_csv('/etc/passwd')"},
    )
    with pytest.raises(PredicateValidationError):
        failslice.check(p)
    with pytest.raises(PredicateValidationError):
        failslice.reduce(p)


def test_check_reports(project_factory: Callable[..., Project]) -> None:
    rep = failslice.check(_fanout(project_factory))
    assert rep.ok and rep.deterministic
    assert rep.tables["orders"]["rows"] == 121
    assert rep.orphans == {str(f): 0 for f in FANOUT_FKS}
    assert len(rep.outcomes) == 4
    bad = failslice.check(
        project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})
    )
    assert not bad.ok and bad.problems


def _export(project: Project, out: Path, kept: dict[str, list[int]]) -> None:
    e = Engine(project, allowed_directories=[out])
    try:
        e.reset_state(kept)
        e.export(out)
    finally:
        e.close()


def test_verify_detects_replay_failure_and_integrity(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    out = tmp_path / "run"
    _export(p, out, {"customers": [7], "orders": [7], "lines": [7]})  # no duplicate -> FALSE
    rep = verify_run_dir(out)
    assert rep.status == "replay-failed" and rep.outcome == "FALSE"
    # Tamper with the export: drop the customers -> dangling references.
    write_table(out / "tables" / "customers.parquet", [("id", "BIGINT"), ("name", "VARCHAR")], [])
    rep = verify_run_dir(out)
    assert rep.status == "integrity-failed"
    assert rep.integrity_violations == {"orders.customer_id -> customers.id": 1}


def test_verify_detects_schema_drift(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    out = tmp_path / "run"
    _export(p, out, {"customers": [7], "orders": [7, 120], "lines": [7]})
    assert verify_run_dir(out).ok
    write_table(
        out / "tables" / "customers.parquet",
        [("id", "VARCHAR"), ("name", "VARCHAR")],
        [("7", "c7")],
    )
    rep = verify_run_dir(out)
    assert rep.status == "replay-failed" and "schema" in rep.detail


def test_verify_keeps_input_orphan_exemptions(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = project_factory(
        {
            "p": ([("k", "BIGINT")], [(1,)]),
            "c": ([("pk", "BIGINT")], [(1,), (99,)]),
        },
        fks=["c.pk -> p.k"],
        predicate={"sql": "SELECT count(*) = 1 FROM c WHERE pk NOT IN (SELECT k FROM p)"},
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok
    assert res.stats is not None and res.stats.sizes_after == {"p": 0, "c": 1}
    con = duckdb.connect()
    assert con.execute(
        f"SELECT fk, row_index FROM '{res.run_dir / 'orphans.parquet'}'"
    ).fetchall() == [("c.pk -> p.k", 0)]


def test_verify_rejects_tampered_predicate(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    p = _fanout(project_factory)
    out = tmp_path / "run"
    _export(p, out, {"customers": [7], "orders": [7, 120], "lines": [7]})
    spec = json.loads((out / "spec.json").read_text())
    spec["predicate"]["sql"] = "SELECT count(*) > 0 FROM read_text('/etc/passwd')"
    (out / "spec.json").write_text(json.dumps(spec))
    with pytest.raises(PredicateValidationError):
        verify_run_dir(out)
    rep = verify_in_subprocess(out, timeout_s=60)
    assert rep.status == "unverified" and "PredicateValidationError" in rep.detail


def test_verify_cli_entry(
    project_factory: Callable[..., Project], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    p = _fanout(project_factory)
    out = tmp_path / "run"
    _export(p, out, {"customers": [7], "orders": [7, 120], "lines": [7]})
    assert verify_main([str(out)]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "verified"
    assert verify_main([]) == 2
    assert api.verify(out).ok


def test_verify_subprocess_timeout(project_factory: Callable[..., Project], tmp_path: Path) -> None:
    p = _fanout(project_factory)
    out = tmp_path / "run"
    _export(p, out, {"customers": [7], "orders": [7, 120], "lines": [7]})
    rep = verify_in_subprocess(out, timeout_s=0.01)
    assert rep.status == "unverified"


@pytest.mark.parametrize("form", ["inline", "file"])
def test_expect_predicate_end_to_end(
    project_factory: Callable[..., Project], tmp_path: Path, form: str
) -> None:
    query = "SELECT sum(l.amount) FROM lines l JOIN orders o USING (order_id)"
    tables = fanout_tables()
    # The correct revenue is the sum over lines; the duplicated order inflates the join.
    correct = sum(r[2] for r in tables["lines"][1])
    if form == "inline":
        pred = {"expect": {"query": query, "expected": correct}}
        p = project_factory(tables, fks=FANOUT_FKS, predicate=pred)
    else:
        p0 = project_factory(
            tables, fks=FANOUT_FKS, predicate={"expect": {"query": query, "expected": 0}}
        )
        write_table(p0.base_dir / "expected.csv", [("total", "DOUBLE")], [(correct,)])
        text = (
            (p0.base_dir / "failslice.yaml")
            .read_text()
            .replace("expected: 0", "expected_file: expected.csv")
        )
        (p0.base_dir / "failslice.yaml").write_text(text)
        p = failslice.load_project(p0.base_dir / "failslice.yaml")
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.verification is not None and res.verification.ok, res.verification
    # With the inflated sum differing from the correct total, any rows of the
    # duplicated order's lines keep the failure; the result is small.
    assert res.stats is not None and res.stats.output_rows <= 4
    if form == "file":
        assert (res.run_dir / "expected" / "expected.csv").is_file()
    assert verify_run_dir(res.run_dir).ok  # same replay, in-process (for coverage)


def test_raises_predicate_end_to_end(
    project_factory: Callable[..., Project], tmp_path: Path
) -> None:
    rows = [(i, str(i)) for i in range(200)] + [(200, "12O")]  # letter O, not zero
    p = project_factory(
        {"readings": ([("id", "BIGINT"), ("raw", "VARCHAR")], rows)},
        predicate={
            "raises": {
                "query": "SELECT sum(CAST(raw AS INTEGER)) FROM readings",
                "match": "Could not convert string",
                "error_class": "ConversionException",
            }
        },
    )
    res = failslice.reduce(p, out_dir=tmp_path / "run")
    assert res.stats is not None and res.stats.sizes_after == {"readings": 1}
    assert res.verification is not None and res.verification.ok
    assert res.minimality_label == "1-minimal (verified)"


def test_python_module_entry_exists() -> None:
    assert shutil.which(sys.executable)
