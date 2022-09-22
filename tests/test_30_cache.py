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


def test_same_name_differen_checksum() -> None:
    def func() -> int:
        return 0

    assert (
        cache.hexdigestify_python_call(func)
        == "63a7e09b221209162fffe521d5021732af13e8d5dc634e3fc8332c1e"
    )

    def func() -> int:  # type: ignore[no-redef]
        return 1

    assert (
        cache.hexdigestify_python_call(func)
        == "10e1cbdc13cd7736068ec5c529448d39198243199c9be4a9fc1e38a7"
    )


def test_same_key_usings_args_or_kwargs() -> None:
    def func(x: Any) -> Any:
        return x

    assert (
        cache.hexdigestify_python_call(func, 1)
        == cache.hexdigestify_python_call(func, x=1)
        == "9390663ca921ede05bf474348b924f9d3c2d43f20bb9b5f35d15d20b"
    )
