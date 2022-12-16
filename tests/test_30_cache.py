import contextvars
import datetime
import pathlib
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


@cache.cacheable
def cached_now() -> datetime.datetime:
    return datetime.datetime.now()


@cache.cacheable
def cached_error() -> None:
    raise ValueError("test error")


def test_cacheable(tmpdir: pathlib.Path) -> None:

    con = config.ENGINE.get().raw_connection()
    cur = con.cursor()

    cfunc = cache.cacheable(func)

    for counter in range(1, 3):
        before = datetime.datetime.utcnow()
        res = cfunc("test")
        after = datetime.datetime.utcnow()
        assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}

        cur.execute("SELECT key, expiration, result, counter FROM cache_entries", ())
        assert cur.fetchall() == [
            (
                "a8260ac3cdc1404aa64a6fb71e85304922e86bcab2eeb6177df5c933",
                "9999-12-31 23:59:59.999999",
                '{"a": "test", "b": null, "args": [], "kwargs": {}}',
                counter,
            )
        ]

        cur.execute("SELECT timestamp FROM cache_entries", ())
        (timestamp,) = cur.fetchone() or []
        assert before < datetime.datetime.fromisoformat(timestamp) < after


@pytest.mark.parametrize("raise_all_encoding_errors", [True, False])
def test_encode_errors(tmpdir: pathlib.Path, raise_all_encoding_errors: bool) -> None:
    config.set(raise_all_encoding_errors=raise_all_encoding_errors)

    cfunc = cache.cacheable(func)

    class Dummy:
        pass

    inst = Dummy()

    if raise_all_encoding_errors:
        with pytest.raises(AttributeError):
            cfunc(inst)
    else:
        with pytest.warns(UserWarning, match="can NOT encode python call"):
            res = cfunc(inst)
        assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}
        assert cache.LAST_PRIMARY_KEYS.get() == {}

    if raise_all_encoding_errors:
        with pytest.raises(AttributeError):
            cfunc("test", b=1)
    else:
        with pytest.warns(UserWarning, match="can NOT encode output"):
            res = cfunc("test", b=1)
        assert res.__class__.__name__ == "LocalClass"
        assert cache.LAST_PRIMARY_KEYS.get() == {}

    # cache-db must be empty
    con = config.ENGINE.get().raw_connection()
    cur = con.cursor()
    cur.execute("SELECT * FROM cache_entries", ())
    assert cur.fetchall() == []


def test_hexdigestify_python_call() -> None:
    assert (
        cache.hexdigestify_python_call(func, 1)
        == cache.hexdigestify_python_call(func, a=1)
        == "54f546036ae7dccdd0155893189154c029803b1f52a7bf5e6283296c"
    )


@pytest.mark.parametrize("use_cache", [True, False])
def test_use_cache(use_cache: bool) -> None:
    config.set(use_cache=use_cache)

    if use_cache:
        assert cached_now() == cached_now()
        assert cache.LAST_PRIMARY_KEYS.get() == {
            "key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6",
            "expiration": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
        }
    else:
        assert cached_now() < cached_now()
        assert cache.LAST_PRIMARY_KEYS.get() == {}


def test_expiration() -> None:
    first = cached_now()
    assert cache.LAST_PRIMARY_KEYS.get() == {
        "key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6",
        "expiration": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
    }

    with config.set(expiration="1908-03-09T00:00:00"):
        assert cached_now() != first
        assert cache.LAST_PRIMARY_KEYS.get() == {
            "key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6",
            "expiration": datetime.datetime(1908, 3, 9),
        }

    assert first == cached_now()
    assert cache.LAST_PRIMARY_KEYS.get() == {
        "key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6",
        "expiration": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
    }


def test_tag(tmpdir: pathlib.Path) -> None:
    con = config.ENGINE.get().raw_connection()
    cur = con.cursor()

    cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [(None, 1)]

    with config.set(tag="1"):
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("1", 2)]

    with config.set(tag="2"):
        # Overwrite
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("2", 3)]

    with config.set(tag=None):
        # Do not overwrite if None
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("2", 4)]


def test_contextvar() -> None:
    cache.LAST_PRIMARY_KEYS.set({})

    ctx = contextvars.copy_context()
    ctx.run(cached_now)
    assert ctx[cache.LAST_PRIMARY_KEYS] == {
        "key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6",
        "expiration": datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
    }

    assert cache.LAST_PRIMARY_KEYS.get() == {}


def test_cached_error() -> None:
    con = config.ENGINE.get().raw_connection()
    cur = con.cursor()

    with pytest.raises(ValueError, match="test error"):
        cached_error()

    cur.execute("SELECT * FROM cache_entries", ())
    assert cur.fetchall() == []


def test_context_argument() -> None:
    ctx = contextvars.copy_context()
    assert cached_now() == cached_now(__context__=ctx)  # type: ignore[call-arg]
