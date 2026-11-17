"""Regression tests for the independent Phase 4 review (one block per finding)."""

from __future__ import annotations

import dataclasses
import json
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp.client import Client

import failslice
from failslice import mcp_server
from failslice.api import ReductionResult, classify
from failslice.config import Project
from failslice.mcp_server import Reply, ServerLimits, _reply, create_server
from failslice.pytest_export import ExportError, build_bundle
from failslice.verify import VerificationReport, verify_in_subprocess
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables, posix_only


@pytest.fixture
def project(project_factory: Callable[..., Project]) -> Project:
    return project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)


@pytest.fixture
def run(project: Project) -> failslice.ReductionResult:
    out = project.base_dir / ".failslice" / "runs" / failslice.api.new_run_id()
    return failslice.reduce(project, out_dir=out)


def _inline(project: Project, **extra: Any) -> dict[str, Any]:
    return {
        "tables": {name: {"path": spec.path} for name, spec in project.tables.items()},
        "foreign_keys": [str(fk) for fk in project.foreign_keys],
        "predicate": FANOUT_PREDICATE,
        **extra,
    }


async def _call(c: Client, tool: str, args: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    r = await c.call_tool(tool, args)
    assert r.structured_content is not None
    return bool(r.is_error), r.structured_content


def _with_client(project: Project, body: Callable[[Client], Any], **limits: Any) -> None:
    server = create_server(project.base_dir, ServerLimits(**limits))

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            await body(c)

    anyio.run(go)


# 1: resources never follow symlinks out of the root.
def test_symlinked_artifacts_are_not_followed(
    project: Project, run: failslice.ReductionResult, tmp_path: Path
) -> None:
    secret = tmp_path / "secret.md"
    secret.write_text("TOP SECRET")
    (run.run_dir / "report.md").unlink()
    (run.run_dir / "report.md").symlink_to(secret)

    async def body(c: Client) -> None:
        with pytest.raises(Exception, match="symlink"):
            await c.read_resource(f"failslice://runs/{run.run_dir.name}/report")

    _with_client(project, body)


def test_symlinked_run_dirs_are_skipped(project: Project, tmp_path: Path) -> None:
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (outside / "manifest.json").write_text('{"status": "reduced"}')
    runs = project.base_dir / ".failslice" / "runs"
    runs.mkdir(parents=True)
    (runs / "20260101T000000Z-abcdef").symlink_to(outside)
    assert mcp_server._list_runs(mcp_server._State(project.base_dir, ServerLimits())) == []


# 2: a dangling symlink at the export destination is refused.
def test_dangling_export_symlink(
    project: Project, run: failslice.ReductionResult, tmp_path: Path
) -> None:
    exports = project.base_dir / ".failslice" / "exports" / run.run_dir.name
    exports.mkdir(parents=True)
    target = tmp_path / "outside-bundle"
    (exports / "evil").symlink_to(target)

    async def body(c: Client) -> None:
        err, data = await _call(c, "export_pytest", {"run_id": run.run_dir.name, "name": "evil"})
        assert err and data["error"]["code"] == "exists"

    _with_client(project, body)
    assert not target.exists()


# 3: a FIFO spec.json cannot block the server.
@posix_only
def test_fifo_spec_does_not_block(project: Project, run: failslice.ReductionResult) -> None:
    (run.run_dir / "spec.json").unlink()
    os.mkfifo(run.run_dir / "spec.json")

    async def body(c: Client) -> None:
        with anyio.fail_after(30):
            err, data = await _call(c, "explain_reproducer", {"run_id": run.run_dir.name})
            assert err and data["error"]["code"] == "not_found"
            err, data = await _call(c, "check_predicate", {"config": _inline(project)})
            assert not err  # the slot was released

    _with_client(project, body)


# 4: a cancelled run is never recovered.
def test_recovery_respects_cancel(project: Project, tmp_path: Path) -> None:
    from failslice.supervisor import SupervisedOutcome, _recover

    run_dir = tmp_path / "r"
    run_dir.mkdir()
    (run_dir / "checkpoint.json").write_text(json.dumps({"run_id": "rid", "kept": {}}))
    cancel = threading.Event()
    cancel.set()
    result = SupervisedOutcome()
    start = time.monotonic()
    _recover(project, run_dir, "rid", result, time.monotonic() + 60, 1.0, cancel)
    assert time.monotonic() - start < 1 and not result.recovered and result.stats is None


# 5: a cancel during finalization makes the outcome "cancelled".
def test_cancel_during_finalize_is_recorded(run: failslice.ReductionResult) -> None:
    from failslice.api import _finish
    from failslice.artifacts import RunContext

    cancel = threading.Event()
    cancel.set()
    assert run.manifest is not None
    ctx = RunContext(run.run_dir.name, run.run_dir, "t", time.monotonic(), run.manifest.config, {})
    fresh = dataclasses.replace(run, manifest=None)
    done = _finish(ctx, fresh, time.monotonic() + 60, cancel)
    assert done.status == "cancelled" and done.failure == "cancelled"
    assert done.manifest is not None and done.manifest.status == "cancelled"
    assert done.minimality_label == "no reproducer"


# 6: a preset cancel or an elapsed deadline is honoured before accepting a replay.
def test_replay_honours_cancel_and_deadline(run: failslice.ReductionResult) -> None:
    cancel = threading.Event()
    cancel.set()
    start = time.monotonic()
    assert verify_in_subprocess(run.run_dir, timeout_s=60, cancel=cancel).status == "cancelled"
    assert time.monotonic() - start < 0.5  # nothing was spawned
    assert verify_in_subprocess(run.run_dir, timeout_s=0.001).status == "unverified"


# 7: the reply budget counts both copies of the JSON.
def test_reply_budget_counts_both_copies() -> None:
    big = Reply[Any](status="ok", data={"x": "y" * 40_000})
    result = _reply(big, ServerLimits(reply_bytes=64 * 1024))
    assert result.is_error and result.structured_content is not None
    assert result.structured_content["error"]["code"] == "reply_too_large"


# 8 + 9: partial settings keep defaults lowerable; replay timeouts respect the ceiling.
def test_partial_settings_and_timeout_ceiling(
    project: Project, run: failslice.ReductionResult
) -> None:
    async def body(c: Client) -> None:
        err, data = await _call(
            c, "check_predicate", {"config": _inline(project, settings={"seed": 1})}
        )
        assert not err and data["data"]["effective_settings"]["max_evals"] == 100
        assert data["data"]["effective_settings"]["seed"] == 1
        err, data = await _call(
            c, "verify_reproducer", {"run_id": run.run_dir.name, "timeout_s": 5000}
        )
        assert err and data["error"]["code"] == "over_ceiling"

    _with_client(project, body, max_evals=100, wall_clock_s=600)


# 10: replay failures are not reported as "reduced"; domain outcomes are not tool errors.
def test_outcome_classification(project_factory: Callable[..., Project]) -> None:
    r = ReductionResult(Path("x"), "reduced", None, VerificationReport("replay-failed"))
    assert classify(r) == ("replay-failed", "failed")
    p = project_factory({"t": ([("x", "BIGINT")], [(1,)])}, predicate={"sql": "SELECT false"})

    async def body(c: Client) -> None:
        err, data = await _call(
            c,
            "reduce_failure",
            {
                "config": {
                    "tables": {"t": {"path": "t.parquet"}},
                    "predicate": {"sql": "SELECT false"},
                }
            },
        )
        assert not err and data["status"] == "failed"
        assert data["error"]["code"] == "predicate_not_failing"

    _with_client(p, body)


# 11: names with a trailing newline are rejected everywhere.
def test_trailing_newline_names(project: Project, run: failslice.ReductionResult) -> None:
    with pytest.raises(ExportError):
        build_bundle(run.run_dir, project.base_dir / "b", name="demo\n")

    async def body(c: Client) -> None:
        err, data = await _call(c, "export_pytest", {"run_id": run.run_dir.name, "name": "demo\n"})
        assert err and data["error"]["code"] == "invalid_name"

    _with_client(project, body)


# 12 + 14: `latest` uses finalized runs ordered by finish time; the run list pages.
def _fake_run(runs: Path, run_id: str, finished: str | None, status: str = "reduced") -> None:
    d = runs / run_id
    d.mkdir(parents=True)
    if finished is not None:
        doc = {"run_id": run_id, "status": status, "finished_at": finished}
        (d / "manifest.json").write_text(json.dumps(doc))


def test_latest_and_pagination(project: Project) -> None:
    runs = project.base_dir / ".failslice" / "runs"
    _fake_run(runs, "20260101T000000Z-ffffff", "2026-01-01T00:00:00.100000+00:00")
    _fake_run(runs, "20260101T000000Z-000000", "2026-01-01T00:00:00.900000+00:00")
    _fake_run(runs, "20260102T000000Z-aaaaaa", None)  # incomplete, newer name
    for i in range(50):
        _fake_run(runs, f"20250101T0000{i:02d}Z-abcdef", f"2025-01-01T00:00:{i:02d}+00:00")
    state = mcp_server._State(project.base_dir, ServerLimits())
    assert mcp_server._run_dir(state, "latest").name == "20260101T000000Z-000000"
    page1 = json.loads(mcp_server._runs_page(state, 1))
    assert len(page1["runs"]) == 50 and page1["next"] == "failslice://runs/page/2"
    page2 = json.loads(mcp_server._runs_page(state, 2))
    assert page2["next"] is None and page2["runs"][-1]["status"] == "incomplete"


def test_latest_without_finalized_runs(project: Project) -> None:
    _fake_run(project.base_dir / ".failslice" / "runs", "20260102T000000Z-aaaaaa", None)
    state = mcp_server._State(project.base_dir, ServerLimits())
    with pytest.raises(mcp_server._Fail, match="no finalized runs"):
        mcp_server._run_dir(state, "latest")
