import os

import pytest
from diskcache import Cache

from cacholote import config


def test_set_cache(tmpdir: str) -> None:
    newdir = os.path.join(tmpdir, "dummy")
    old_cache = config.SETTINGS["cache_store"]
    new_cache = Cache(directory=newdir)
    assert old_cache is not new_cache

    with config.set(cache_store=new_cache):
        assert config.SETTINGS["cache_store"] is new_cache
        assert config.SETTINGS["directory"] == newdir
    assert config.SETTINGS["cache_store"] is old_cache
    assert config.SETTINGS["directory"] == tmpdir

    config.set(cache_store=new_cache)
    assert config.SETTINGS["cache_store"] is new_cache
    assert config.SETTINGS["directory"] == newdir

    with pytest.raises(
        ValueError, match=r"'cache_store' is mutually exclusive with all other settings"
    ):
        config.set(cache_store=new_cache, directory="dummy")


def test_change_settings(tmpdir: str) -> None:
    newdir = os.path.join(tmpdir, "dummy")
    config.set(directory=newdir)
    assert config.SETTINGS["cache_store"].directory == newdir

    with config.set(statistics=0):
        assert config.SETTINGS["cache_store"].statistics == 0
    assert config.SETTINGS["cache_store"].statistics == 1

    with pytest.raises(KeyError, match="Wrong settings. Available settings: "):
        config.set(dummy="dummy")
