"""The MCP server through the SDK's in-memory client (real JSON-RPC, ``mode="legacy"``)."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import anyio
import pytest
from mcp.client import Client

from failslice.config import Project
from failslice.mcp_server import ServerLimits, create_server, main
from tests.conftest import FANOUT_FKS, FANOUT_PREDICATE, fanout_tables

TOOLS = {
    "inspect_dataset",
    "suggest_relationships",
    "check_predicate",
    "reduce_failure",
    "verify_reproducer",
    "explain_reproducer",
    "export_pytest",
}
SLOW = {"sql": "SELECT count(*) > 0 FROM lines a, lines b, lines c, lines d"}


def _inline(project: Project, predicate: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "tables": {name: {"path": spec.path} for name, spec in project.tables.items()},
        "foreign_keys": [str(fk) for fk in project.foreign_keys],
        "predicate": predicate or FANOUT_PREDICATE,
    }


@pytest.fixture
def project(project_factory: Callable[..., Project]) -> Project:
    return project_factory(fanout_tables(), fks=FANOUT_FKS, predicate=FANOUT_PREDICATE)


async def _call(client: Client, tool: str, args: dict[str, Any], **kw: Any) -> tuple[bool, dict]:  # type: ignore[type-arg]
    result = await client.call_tool(tool, args, **kw)
    data = result.structured_content
    assert data is not None and data["format"] == 1
    return bool(result.is_error), data


def _run(coro: Callable[[], Any]) -> None:
    anyio.run(coro)


def test_discovery(project: Project) -> None:
    server = create_server(project.base_dir)

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            tools = {t.name: t for t in (await c.list_tools()).tools}
            assert set(tools) == TOOLS
            schema = json.dumps(tools["reduce_failure"].input_schema)
            for word in ("SqlForm", "ExpectForm", "RaisesForm", "config_path"):
                assert word in schema  # the inline config is fully typed for agents
            templates = {
                t.uri_template for t in (await c.list_resource_templates()).resource_templates
            }
            assert "failslice://runs/{run_id}/manifest" in templates
            prompts = {p.name for p in (await c.list_prompts()).prompts}
            assert prompts == {"debug-data-bug"}
            text = await c.get_prompt(
                "debug-data-bug", {"bug": "revenue doubled", "directory": "d"}
            )
            assert "export_pytest" in text.messages[0].content.text  # type: ignore[union-attr]

    _run(go)


def test_full_workflow(project: Project) -> None:
    server = create_server(project.base_dir)
    progress: list[tuple[float, float | None, str | None]] = []

    async def on_progress(p: float, total: float | None, message: str | None) -> None:
        progress.append((p, total, message))

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            err, data = await _call(c, "check_predicate", {"config": _inline(project)})
            assert not err and data["status"] == "ok" and data["data"]["deterministic"]
            err, same = await _call(c, "check_predicate", {"config_path": "failslice.yaml"})
            assert same["data"]["orphans"] == data["data"]["orphans"]

            err, data = await _call(
                c,
                "reduce_failure",
                {"config": _inline(project), "seed": 0},
                progress_callback=on_progress,
            )
            assert not err and data["status"] == "reduced", data
            run = data["data"]
            assert run["minimality"] == "1-minimal (verified)"
            assert run["rows_before"] == 551 and run["rows_after"] == 4
            run_id = run["run_id"]
            assert (project.base_dir / ".failslice" / "runs" / run_id / "manifest.json").is_file()

            manifest = await c.read_resource(f"failslice://runs/{run_id}/manifest")
            assert json.loads(manifest.contents[0].text)["run_id"] == run_id  # type: ignore[union-attr]
            report = await c.read_resource("failslice://runs/latest/report")
            assert "1-minimal (verified)" in report.contents[0].text  # type: ignore[union-attr]
            runs = await c.read_resource("failslice://runs")
            assert json.loads(runs.contents[0].text)["runs"][0]["run_id"] == run_id  # type: ignore[union-attr]
            preview = await c.read_resource(f"failslice://runs/{run_id}/tables/orders/preview")
            assert json.loads(preview.contents[0].text)["rows"] == 2  # type: ignore[union-attr]

            err, v = await _call(c, "verify_reproducer", {"run_id": "latest"})
            assert v["status"] == "verified" and v["data"]["run_id"] == run_id
            err, ex = await _call(c, "explain_reproducer", {"run_id": run_id, "limit": 3})
            assert not err and ex["data"]["format"] == 1
            err, exp = await _call(c, "export_pytest", {"run_id": run_id})
            assert not err, exp
            assert exp["data"]["directory"].startswith(".failslice/exports/")
            assert (project.base_dir / exp["data"]["test_file"]).is_file()

    _run(go)
    assert progress and all(total is None for _, total, _ in progress)


def test_errors(project: Project, tmp_path: Path) -> None:
    server = create_server(project.base_dir, ServerLimits(max_evals=100))

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            err, data = await _call(c, "check_predicate", {"config_path": "../outside.yaml"})
            assert err and data["error"]["code"] == "ConfigError"
            err, data = await _call(c, "check_predicate", {})
            assert err and data["error"]["code"] == "invalid_arguments"
            err, data = await _call(
                c, "reduce_failure", {"config": _inline(project), "max_evals": 1000}
            )
            assert err and data["error"]["code"] == "over_ceiling"
            bad = _inline(project, {"sql": "SELECT count(*) > 0 FROM read_text('/etc/passwd')"})
            err, data = await _call(c, "check_predicate", {"config": bad})
            assert err and data["error"]["code"].startswith("predicate_"), data
            err, data = await _call(c, "verify_reproducer", {"run_id": "../../etc"})
            assert err and data["error"]["code"] == "invalid_run_id"
            err, data = await _call(c, "explain_reproducer", {"run_id": "latest"})
            assert err and data["error"]["code"] == "not_found"
            never = _inline(project, {"sql": "SELECT false"})
            err, data = await _call(c, "check_predicate", {"config": never})
            assert not err and data["status"] == "failed"

    _run(go)


def test_busy_and_cancellation(
    project_factory: Callable[..., Project],
) -> None:
    p = project_factory(
        fanout_tables(), fks=FANOUT_FKS, predicate=SLOW, settings={"eval_timeout_s": 25}
    )
    server = create_server(p.base_dir)

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            busy: dict[str, Any] = {}

            async def second() -> None:
                await anyio.sleep(1.5)
                busy["reply"] = await _call(c, "check_predicate", {"config": _inline(p, SLOW)})

            with anyio.move_on_after(4):  # cancels the reduce request
                async with anyio.create_task_group() as tg:
                    tg.start_soon(second)
                    await _call(c, "reduce_failure", {"config": _inline(p, SLOW)})
            err, data = busy["reply"]
            assert err and data["error"]["code"] == "busy" and data["error"]["retryable"]
            # The slot is released once the cancelled work has stopped.
            with anyio.fail_after(60):
                while True:
                    err, data = await _call(
                        c, "check_predicate", {"config": _inline(p, {"sql": "SELECT false"})}
                    )
                    if not (err and data["error"]["code"] == "busy"):
                        break
                    await anyio.sleep(0.5)
            assert data["status"] == "failed"

    _run(go)
    runs = list((p.base_dir / ".failslice" / "runs").iterdir())
    assert len(runs) == 1
    manifest = json.loads((runs[0] / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "cancelled" and manifest["failure"] == "cancelled"


def test_main_requires_existing_root(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        main([])
    with pytest.raises(SystemExit) as exc:
        main(["--root", str(tmp_path / "missing")])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "not an existing directory" in err and "Traceback" not in err


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--max-evals", "0"),
        ("--max-evals", "-1"),
        ("--max-evals", "x"),
        ("--wall-clock", "0"),
        ("--wall-clock", "-5"),
        ("--wall-clock", "inf"),
        ("--eval-timeout", "nan"),
        ("--eval-timeout", "soon"),
    ],
)
def test_main_rejects_bad_budgets(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], flag: str, value: str
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--root", str(tmp_path), flag, value])
    assert exc.value.code == 2
    assert flag in capsys.readouterr().err


def test_inspect_suggest_and_lowered_defaults(project: Project) -> None:
    server = create_server(project.base_dir, ServerLimits(max_evals=100))

    async def go() -> None:
        async with Client(server, mode="legacy") as c:
            err, data = await _call(
                c, "inspect_dataset", {"directory": ".", "foreign_keys": list(FANOUT_FKS)}
            )
            assert not err, data
            tables = {t["name"]: t for t in data["data"]["tables"]}
            assert tables["orders"]["rows"] == 121
            assert [f["orphans"] for f in data["data"]["foreign_keys"]] == [0, 0]
            assert "SELECT false" in data["data"]["config_template"]
            err, data = await _call(
                c, "inspect_dataset", {"tables": {"orders": "orders.parquet"}, "preview_rows": 2}
            )
            assert not err and len(data["data"]["tables"][0]["preview"]["rows"]) == 2
            err, data = await _call(c, "inspect_dataset", {})
            assert err and data["error"]["code"] == "invalid_arguments"
            err, data = await _call(c, "suggest_relationships", {"directory": "."})
            assert not err and "suggestion" in data["data"]["config_yaml"]
            # Defaults nobody set are lowered to the server's ceiling, and reported.
            err, data = await _call(c, "check_predicate", {"config": _inline(project)})
            assert not err and data["data"]["effective_settings"]["max_evals"] == 100

    _run(go)
