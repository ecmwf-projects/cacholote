from __future__ import annotations

import cacholote


def test_version() -> None:
    assert cacholote.__version__ != "999"
