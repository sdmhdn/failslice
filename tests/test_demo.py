"""``failslice demo`` (ADR 0013): packaged quickstart, staged creation, CLI contract."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

import failslice
from failslice import demo as demo_mod
from failslice.cli import app
from failslice.demo import DEMO_FILES, DemoError, write_demo

REPO = Path(__file__).resolve().parents[1]
PACKAGED = REPO / "src" / "failslice" / "_demo"
QUICKSTART = REPO / "examples" / "quickstart"


@pytest.mark.parametrize("name", DEMO_FILES)
def test_packaged_demo_matches_examples_quickstart(name: str) -> None:
    assert (PACKAGED / name).read_bytes() == (QUICKSTART / name).read_bytes(), (
        f"src/failslice/_demo/{name} and examples/quickstart/{name} must stay identical"
    )


def _rows(path: Path) -> int:
    con = duckdb.connect()
    try:
        literal = "'" + str(path).replace("'", "''") + "'"
        row = con.execute(f"SELECT count(*) FROM read_csv({literal})").fetchone()
        assert row is not None
        return int(row[0])
    finally:
        con.close()


def test_write_demo_creates_a_complete_project(tmp_path: Path) -> None:
    target = tmp_path / "it's a demo"  # quotes in the path must not break the generator
    result = write_demo(target)
    assert result.directory == target.resolve()
    assert all(f.is_file() for f in result.files)
    generated = target / "data" / "generated"
    assert {p.name: _rows(p) for p in generated.iterdir()} == {
        "customers.csv": 1000,
        "orders.csv": 5001,
        "lines.csv": 20000,
    }
    assert [p.name for p in tmp_path.iterdir()] == [target.name]  # no staging leftovers
    project = failslice.load_project(target / "failslice.yaml")
    assert failslice.check(project).ok


@pytest.mark.parametrize("make", ["file", "empty_dir"])
def test_write_demo_refuses_an_existing_target(tmp_path: Path, make: str) -> None:
    target = tmp_path / "taken"
    if make == "file":
        target.write_text("x", encoding="utf-8")
    else:
        target.mkdir()
    with pytest.raises(DemoError, match="already exists"):
        write_demo(target)


def test_write_demo_needs_an_existing_parent(tmp_path: Path) -> None:
    with pytest.raises(DemoError, match="not an existing directory"):
        write_demo(tmp_path / "missing" / "demo")


def test_failed_generation_leaves_nothing_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(path: str, run_name: str) -> dict[str, object]:
        def main(out: Path) -> None:
            raise OSError("disk full")

        return {"main": main}

    monkeypatch.setattr(demo_mod.runpy, "run_path", broken)
    with pytest.raises(OSError, match="disk full"):
        write_demo(tmp_path / "demo")
    assert list(tmp_path.iterdir()) == []


def test_missing_generated_file_is_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        demo_mod.runpy, "run_path", lambda path, run_name: {"main": lambda out: None}
    )
    with pytest.raises(DemoError, match=r"did not write customers\.csv"):
        write_demo(tmp_path / "demo")
    assert list(tmp_path.iterdir()) == []


def test_cli_demo_json_and_refusal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    first = runner.invoke(app, ["demo", "shop", "--json"])
    assert first.exit_code == 0, first.output
    doc = json.loads(first.stdout)
    assert doc["command"] == "demo" and doc["status"] == "ok"
    assert Path(doc["data"]["directory"]) == (tmp_path / "shop").resolve()
    assert len(doc["data"]["files"]) == 5

    again = runner.invoke(app, ["demo", "shop", "--json"])
    assert again.exit_code == 2
    err = json.loads(again.stdout)["error"]
    assert err["field"] == "directory" and "already exists" in err["message"]


def test_cli_demo_text_names_the_next_steps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    out = CliRunner().invoke(app, ["demo"])
    assert out.exit_code == 0, out.output
    assert (tmp_path / "failslice-demo" / "failslice.yaml").is_file()
    assert "failslice reduce --out run" in out.stdout
    assert "pytest run/pytest" in out.stdout
