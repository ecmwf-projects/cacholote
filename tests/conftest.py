import diskcache
import pytest

from callcache import config


@pytest.fixture(autouse=True)
def clear_cache(tmpdir: str) -> None:
    config.set(cache=diskcache.Cache(tmpdir, disk=diskcache.JSONDisk))
    config.SETTINGS["cache"].clear()
    config.SETTINGS["cache"].stats(reset=True)
