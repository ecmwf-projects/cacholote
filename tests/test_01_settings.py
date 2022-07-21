import pytest
from diskcache import Cache

from callcache import config


def test_settings_change() -> None:
    old_cache = config.SETTINGS["cache"]
    new_cache = Cache()
    assert old_cache is not new_cache

    # context manager
    with config.set(cache=new_cache):
        assert config.SETTINGS["cache"] is new_cache
    assert config.SETTINGS["cache"] is old_cache

    config.set(cache=new_cache)
    assert config.SETTINGS["cache"] is new_cache

    # Restore default
    config.set(cache=old_cache)
    assert config.SETTINGS["cache"] is old_cache

    with pytest.raises(KeyError, match="Wrong settings. Available settings: "):
        config.set(dummy="dummy")
