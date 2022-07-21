import diskcache
import pytest

import callcache


@pytest.fixture(autouse=True)
def clear_cache(tmpdir: str) -> None:
    callcache.config(cache=diskcache.Cache(tmpdir, disk=diskcache.JSONDisk))
    callcache.SETTINGS["cache"].clear()
    callcache.SETTINGS["cache"].stats(reset=True)
