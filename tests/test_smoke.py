import re

import failslice


def test_version_is_pep440_like() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+(\.dev\d+)?", failslice.__version__)
