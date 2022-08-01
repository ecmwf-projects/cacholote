import pytest

from cacholote import config


@pytest.fixture(autouse=True)
def set_tmpdir(tmpdir: str) -> None:
    config.set(directory=tmpdir)
