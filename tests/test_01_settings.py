import pytest

import callcache


def test_settings_change() -> None:
    filecache_root = callcache.SETTINGS["filecache_root"]
    callcache.config(filecache_root="dummy")
    assert callcache.SETTINGS["filecache_root"] == "dummy"

    # Restore default
    callcache.config(filecache_root=filecache_root)
    assert callcache.SETTINGS["filecache_root"] != "dummy"

    with pytest.raises(
        ValueError, match="The following settings do NOT exist: {'dummy'}"
    ):
        callcache.config(dummy="dummy")


def test_settings_context_manager() -> None:
    with callcache.config(filecache_root="dummy"):
        assert callcache.SETTINGS["filecache_root"] == "dummy"
    assert callcache.SETTINGS["filecache_root"] != "dummy"
