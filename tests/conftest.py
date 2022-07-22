import pytest

from callcache import config


@pytest.fixture(autouse=True)
def clear_cache(tmpdir: str) -> None:
    config.set(directory=tmpdir)
