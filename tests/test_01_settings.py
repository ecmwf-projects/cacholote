import os

import pytest
from diskcache import Cache

from cacholote import config


def test_set_cache(tmpdir: str) -> None:
    newdir = os.path.join(tmpdir, "dummy")
    old_cache = config.SETTINGS["cache_store"]
    new_cache = Cache(cache_store_directory=newdir)
    assert old_cache is not new_cache

    with config.set(cache_store=new_cache):
        assert config.SETTINGS["cache_store"] is new_cache
        assert config.SETTINGS["cache_store_directory"] is None
    assert config.SETTINGS["cache_store"] is old_cache
    assert config.SETTINGS["cache_store_directory"] == tmpdir

    config.set(cache_store=new_cache)
    assert config.SETTINGS["cache_store"] is new_cache
    assert config.SETTINGS["cache_store_directory"] is None

    with pytest.raises(
        ValueError,
        match=r"'cache_store' and 'cache_store_directory' are mutually exclusive",
    ):
        config.set(cache_store=new_cache, cache_store_directory="dummy")


def test_change_settings(tmpdir: str) -> None:
    newdir = os.path.join(tmpdir, "dummy")

    with config.set(cache_store_directory=newdir):
        assert config.SETTINGS["cache_store"].directory == newdir
    assert config.SETTINGS["cache_store"].directory == tmpdir

    config.set(cache_store_directory=newdir)
    assert config.SETTINGS["cache_store"].directory == newdir

    with pytest.raises(ValueError, match="Wrong settings. Available settings: "):
        config.set(dummy="dummy")
