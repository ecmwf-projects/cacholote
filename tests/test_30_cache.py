import datetime
import json
from typing import Any

import pytest

from cacholote import cache, config


def func(a: Any, *args: Any, b: Any = None, **kwargs: Any) -> Any:
    if b is None:
        return locals()
    else:

        class LocalClass:
            pass

        return LocalClass()


@pytest.mark.parametrize("set_cache", ["file", "redis"], indirect=True)
def test_cacheable(set_cache: str) -> None:

    cache_store = config.SETTINGS["cache_store"]

    cfunc = cache.cacheable(func)
    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    if set_cache == "redis":
        assert cache_store.info()["keyspace_hits"] == 0
        assert cache_store.info()["keyspace_misses"] == 1
    else:
        # diskcache
        assert cache_store.stats() == (0, 1)

    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    if set_cache == "redis":
        assert cache_store.info()["keyspace_hits"] == 1
        assert cache_store.info()["keyspace_misses"] == 1
    else:
        # diskcache
        assert cache_store.stats() == (1, 1)

    class Dummy:
        pass

    inst = Dummy()
    with pytest.warns(UserWarning, match="can NOT encode python call"):
        res = cfunc(inst)
    assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}

    with pytest.warns(UserWarning, match="can NOT encode output"):
        res = cfunc("test", b=1)
    assert res.__class__.__name__ == "LocalClass"


def test_hexdigestify_python_call() -> None:
    assert (
        cache.hexdigestify_python_call(func, 1)
        == cache.hexdigestify_python_call(func, a=1)
        == "c70ca47c460afc916aaf2804260271300aa7360d85018d1c6e9226d0"
    )


@pytest.mark.parametrize("set_cache", ["file", "redis"], indirect=True)
def test_append_info(set_cache: str) -> None:
    cfunc = cache.cacheable(func)
    cache_key = "c70ca47c460afc916aaf2804260271300aa7360d85018d1c6e9226d0"
    with config.set(append_info=True):
        cfunc(1)
        cache_dict = json.loads(config.SETTINGS["cache_store"][cache_key])
        assert cache_dict["info"]["count"] == 1
        atime0 = datetime.datetime.fromisoformat(cache_dict["info"]["atime"])

        cfunc(1)
        cache_dict = json.loads(config.SETTINGS["cache_store"][cache_key])
        assert cache_dict["info"]["count"] == 2
        atime1 = datetime.datetime.fromisoformat(cache_dict["info"]["atime"])
        assert atime0 < atime1
