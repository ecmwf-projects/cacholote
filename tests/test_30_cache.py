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
    res = cache.hexdigestify_python_call(sorted, "foo", reverse=True)
    assert res == "29a102cc6e599572ddadf8fdc05bc09e8bf793257b18ae2440b5fc42"


def test_same_name_differen_hash() -> None:
    def func():
        return 0

    assert (
        cache.hexdigestify_python_call(func)
        == "9fe6f81099fccf65388ec5e7cbac7919ed12ccace6f439ece0b3d345"
    )

    def func():
        return 1

    assert (
        cache.hexdigestify_python_call(func)
        == "816ac7ce78f6029aedff83841d65aa89925b7f5c5cda0ec92f62542a"
    )


def test_same_hash_usings_args_or_kwargs() -> None:
    def func(x):
        return x

    assert (
        cache.hexdigestify_python_call(func, 1)
        == cache.hexdigestify_python_call(func, x=1)
        == "f44fd64232b026c015d4f31d020dd829a1c04c04e760123e9c860119"
    )
