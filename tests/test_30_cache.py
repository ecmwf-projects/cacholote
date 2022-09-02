from typing import Any, Dict, List

import pytest

from cacholote import cache, config

SETTINGS_LIST: List[Dict[str, Any]] = [{}]
try:
    import redislite

    SETTINGS_LIST.append({"cache_store": redislite.Redis()})
    HAS_REDISLITE = True
except ImportError:
    HAS_REDISLITE = False


def func(a: Any, *args: Any, b: Any = None, **kwargs: Any) -> Any:
    if b is None:
        return locals()
    else:

        class LocalClass:
            pass

        return LocalClass()


def test_hexdigestify() -> None:
    text = "some random Unicode text \U0001f4a9"
    expected = "278a2cefeef9a3269f4ba1c41ad733a4c07101ae6859f607c8a42cf2"
    res = cache.hexdigestify(text)
    assert res == expected


@pytest.mark.parametrize("settings", SETTINGS_LIST)
def test_cacheable(settings: Dict[str, Any]) -> None:

    with config.set(**settings):
        cache_store = config.SETTINGS["cache_store"]
        is_redis = HAS_REDISLITE and isinstance(cache_store, redislite.Redis)

        print(config.SETTINGS)

        cfunc = cache.cacheable(func)
        res = cfunc("test")
        assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
        if is_redis:
            assert cache_store.info()["keyspace_hits"] == 0
            assert cache_store.info()["keyspace_misses"] == 1
        else:
            # diskcache
            assert cache_store.stats() == (0, 1)

        res = cfunc("test")
        assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
        if is_redis:
            assert cache_store.info()["keyspace_hits"] == 1
            assert cache_store.info()["keyspace_misses"] == 1
        else:
            # diskcache
            assert cache_store.stats() == (1, 1)

        class Dummy:
            pass

        inst = Dummy()
        with pytest.warns(UserWarning, match="bad input"):
            res = cfunc(inst)
        assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}

        with pytest.warns(UserWarning, match="bad output"):
            res = cfunc("test", b=1)
        assert res.__class__.__name__ == "LocalClass"
