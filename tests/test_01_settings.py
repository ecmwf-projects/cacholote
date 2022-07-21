import pytest
from diskcache import Cache

import callcache


def test_settings_change() -> None:
    old_cache = callcache.SETTINGS["cache"]
    new_cache = Cache()
    assert old_cache is not new_cache

    callcache.config(cache=new_cache)
    assert callcache.SETTINGS["cache"] is new_cache

    # Restore default
    callcache.config(cache=old_cache)
    assert callcache.SETTINGS["cache"] is old_cache

    with callcache.config(cache=new_cache):
        assert callcache.SETTINGS["cache"] is new_cache
    assert callcache.SETTINGS["cache"] is old_cache

    with pytest.raises(
        ValueError, match="The following settings do NOT exist: {'dummy'}"
    ):
        callcache.config(dummy="dummy")
