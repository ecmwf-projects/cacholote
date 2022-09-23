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


def test_same_name_different_checksum() -> None:
    def func() -> int:
        return 0

    assert (
        cache.hexdigestify_python_call(func)
        == "4f6501c8d23ac56958f4d123edee9f77e14b8cd3fbf1f96af8d1a51b"
    )

    def func() -> int:  # type: ignore[no-redef]
        return 1

    assert (
        cache.hexdigestify_python_call(func)
        == "990dd6a221d930bdfad1b0a9bb6de5ff3046c21e845974ca7b9ae94d"
    )


def test_same_key_using_args_or_kwargs() -> None:
    def func(x: Any) -> Any:
        return x

    assert (
        cache.hexdigestify_python_call(func, 1)
        == cache.hexdigestify_python_call(func, x=1)
        == "b62f186e782e4082598e0b062fc1925af84c08c2cb166cf6b3a113c3"
    )
