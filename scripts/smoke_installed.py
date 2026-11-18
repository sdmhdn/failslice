"""Smoke-test installed FailSlice wheels, outside the repository (ADR 0013).

Run with the Python of a fresh environment where the built ``failslice`` and
``failslice-mcp`` wheels (and pytest) are installed, from a directory outside the checkout:

    python smoke_installed.py --expect-version 0.1.0 WORKDIR

It uses only the installed console scripts and packaged resources: ``failslice demo``,
``failslice reduce``, the generated pytest bundle, and ``failslice-mcp`` over real stdio
with the MCP SDK client (discovery, check, reduce, verify, explain, export).
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import anyio
from mcp import Client
from mcp.client.stdio import StdioServerParameters

import failslice


def _script(name: str) -> str:
    """The console script next to this interpreter (not whatever is first on PATH)."""
    bindir = Path(sys.executable).parent
    for candidate in (bindir / name, bindir / f"{name}.exe"):
        if candidate.is_file():
            return str(candidate)
    found = shutil.which(name, path=str(bindir))
    if found is None:
        raise SystemExit(f"FAIL: console script {name} is not installed next to {sys.executable}")
    return found


def _run(args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    print("$", " ".join(args), flush=True)
    proc = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        print(proc.stdout, proc.stderr, sep="\n")
        raise SystemExit(f"FAIL: exit status {proc.returncode}")
    return proc


def _check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"FAIL: {message}")
    print("ok:", message, flush=True)


def check_not_from_checkout() -> None:
    location = Path(failslice.__file__).resolve()
    _check(
        "site-packages" in location.parts,
        f"failslice is imported from an installed wheel ({location})",
    )


def check_cli(work: Path, version: str) -> None:
    failslice = _script("failslice")
    out = _run([failslice, "--version"], work).stdout.strip()
    _check(out == f"failslice {version}", f"failslice --version prints {out!r}")
    out = _run([_script("failslice-mcp"), "--version"], work).stdout.strip()
    _check(out == f"failslice-mcp {version}", f"failslice-mcp --version prints {out!r}")

    _run([failslice, "demo", "shop"], work)
    shop = work / "shop"
    doc = json.loads(_run([failslice, "reduce", "--out", "run", "--json"], shop).stdout)
    data = doc["data"]
    replay = data["verification"]["status"]
    _check(doc["status"] == "reduced", f"reduce status is {doc['status']!r}")
    _check(
        data["minimality"] == "1-minimal (verified)" and replay == "verified",
        f"result is {data['minimality']!r}, replay {replay!r}",
    )
    rows = (data["manifest"]["rows_before"], data["manifest"]["rows_after"])
    _check(rows == (26001, 4), f"rows {rows[0]} -> {rows[1]}")
    out = _run([sys.executable, "-m", "pytest", "-q", "run/pytest"], shop).stdout
    _check("1 passed" in out, "the generated pytest bundle passes")


async def _mcp(work: Path) -> None:
    root = work / "shop"
    params = StdioServerParameters(command=_script("failslice-mcp"), args=["--root", str(root)])

    async def call(client: Any, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        result = await client.call_tool(tool, args)
        data = result.structured_content
        if result.is_error or data is None:
            raise SystemExit(f"FAIL: {tool} returned an error: {data}")
        return dict(data)

    async with Client(params) as client:
        tools = {t.name for t in (await client.list_tools()).tools}
        _check("reduce_failure" in tools and len(tools) == 7, f"{len(tools)} MCP tools")
        prompts = {p.name for p in (await client.list_prompts()).prompts}
        _check(prompts == {"debug-data-bug"}, "the debug-data-bug prompt is listed")

        data = await call(client, "check_predicate", {"config_path": "failslice.yaml"})
        _check(data["data"]["deterministic"], "check_predicate: deterministic")
        data = await call(client, "reduce_failure", {"config_path": "failslice.yaml"})
        run = data["data"]
        _check(
            run["minimality"] == "1-minimal (verified)" and run["rows_after"] == 4,
            f"reduce_failure: {run['minimality']}, {run['rows_after']} rows",
        )
        data = await call(client, "verify_reproducer", {"run_id": run["run_id"]})
        _check(data["status"] == "verified", "verify_reproducer: verified")
        await call(client, "explain_reproducer", {"run_id": run["run_id"]})
        data = await call(client, "export_pytest", {"run_id": run["run_id"]})
        test_file = root / data["data"]["test_file"]
        _check(test_file.is_file(), f"export_pytest wrote {data['data']['test_file']}")
        resource = await client.read_resource("failslice://runs/latest/report")
        text = resource.contents[0].text  # type: ignore[union-attr]
        _check("1-minimal (verified)" in text, "the run report resource is readable")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("workdir", type=Path, help="empty directory outside the checkout")
    parser.add_argument("--expect-version", required=True)
    args = parser.parse_args()
    work = args.workdir.resolve()
    work.mkdir(parents=True, exist_ok=True)
    check_not_from_checkout()
    check_cli(work, args.expect_version)
    anyio.run(_mcp, work)
    print("smoke test passed")


if __name__ == "__main__":
    main()
