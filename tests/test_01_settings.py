import os

import pytest
from diskcache import Cache

from cacholote import config


def test_set_cache() -> None:
    old_cache = config.SETTINGS["cache"]
    new_cache = Cache()
    assert old_cache is not new_cache

    # context manager
    with config.set(cache=new_cache):
        assert config.SETTINGS["cache"] is new_cache
    assert config.SETTINGS["cache"] is old_cache

    config.set(cache=new_cache)
    assert config.SETTINGS["cache"] is new_cache

    with pytest.raises(
        ValueError, match=r"'cache' is mutually exclusive with all other settings"
    ):
        config.set(cache=new_cache, directory="dummy")


def test_change_settings(tmpdir: str) -> None:
    newdir = os.path.join(tmpdir, "dummy")
    config.set(directory=newdir)
    assert config.SETTINGS["cache"].directory == newdir

    with config.set(statistics=0):
        assert config.SETTINGS["cache"].statistics == 0
    assert config.SETTINGS["cache"].statistics == 1

    with pytest.raises(KeyError, match="Wrong settings. Available settings: "):
        config.set(dummy="dummy")
