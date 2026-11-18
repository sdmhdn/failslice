"""``failslice demo``: write the quickstart example into a new directory (ADR 0013).

The packaged copy in ``failslice/_demo/`` is byte-identical to ``examples/quickstart/``
(enforced by a test), so ``uvx failslice demo`` needs no repository checkout.
"""

from __future__ import annotations

import os
import runpy
import shutil
import tempfile
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

__all__ = ["DEMO_FILES", "DemoError", "DemoResult", "write_demo"]

DEMO_FILES = ("failslice.yaml", "make_data.py")
GENERATED = ("customers.csv", "orders.csv", "lines.csv")


class DemoError(Exception):
    """The demo directory could not be written."""


@dataclass(frozen=True)
class DemoResult:
    directory: Path
    files: tuple[Path, ...]


def _resource(name: str) -> resources.abc.Traversable:
    return resources.files("failslice") / "_demo" / name


def write_demo(directory: Path) -> DemoResult:
    """Create ``directory`` with the quickstart config, generator and generated CSVs.

    The directory must not exist. Everything is written to a temporary sibling first and
    renamed into place at the end, so a failure leaves nothing behind.
    """
    target = directory.resolve()
    if target.exists():
        raise DemoError(f"{target} already exists; choose a new directory")
    if not target.parent.is_dir():
        raise DemoError(f"{target.parent} is not an existing directory")
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        for name in DEMO_FILES:
            (staging / name).write_bytes(_resource(name).read_bytes())
        # Run the packaged generator (not the copy just written) on the staging directory.
        with resources.as_file(_resource("make_data.py")) as generator:
            module = runpy.run_path(str(generator), run_name="failslice_demo_data")
        module["main"](staging / "data" / "generated")
        for name in GENERATED:
            if not (staging / "data" / "generated" / name).is_file():
                raise DemoError(f"the generator did not write {name}")
        # Not atomic against a racing writer: on POSIX an empty directory created after the
        # check above would be replaced; a non-empty one makes the rename fail.
        os.rename(staging, target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    files = tuple(target / name for name in DEMO_FILES) + tuple(
        target / "data" / "generated" / name for name in GENERATED
    )
    return DemoResult(target, files)
