import datetime
import logging
import pathlib
import sqlite3
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


def test_cacheable(tmpdir: pathlib.Path) -> None:

    con = sqlite3.connect(str(tmpdir / "cacholote.db"))
    cur = con.cursor()

    cfunc = cache.cacheable(func)

    for counter in range(1, 3):
        before = datetime.datetime.now()
        res = cfunc("test")
        after = datetime.datetime.now()
        assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}

        cur.execute("SELECT key, expiration, result, counter FROM cache_entries")
        assert cur.fetchall() == [
            (
                "a8260ac3cdc1404aa64a6fb71e85304922e86bcab2eeb6177df5c933",
                "9999-12-31 23:59:59.999999",
                '{"a":"test","b":null,"args":[],"kwargs":{}}',
                counter,
            )
        ]

        cur.execute("SELECT timestamp FROM cache_entries")
        (timestamp,) = cur.fetchone()
        assert before < datetime.datetime.fromisoformat(timestamp) < after

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
        == "54f546036ae7dccdd0155893189154c029803b1f52a7bf5e6283296c"
    )


@pytest.mark.parametrize("use_cache", [True, False])
def test_use_cache(use_cache: bool) -> None:

    with config.set(use_cache=use_cache):
        if use_cache:
            assert cached_now() == cached_now()
        else:
            assert cached_now() < cached_now()


def test_expiration() -> None:

    first = cached_now()
    with config.set(expiration=datetime.datetime.now()):
        second = cached_now()
        assert second != first
    assert first == cached_now()


def test_logging(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO):
        cached_now()
        assert caplog.text.endswith(
            '{"key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6", '
            '"expiration": "9999-12-31T23:59:59.999999"}\n'
        )

        with config.set(expiration=datetime.datetime(1492, 12, 10)):
            cached_now()
            assert caplog.text.endswith(
                '{"key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6", '
                '"expiration": "1492-12-10T00:00:00"}\n'
            )

        cached_now()
        assert caplog.text.endswith(
            '{"key": "c3d9e414d0d32337c3672cb29b1b3cc9408001bf2d1b2a71c5e45fb6", '
            '"expiration": "9999-12-31T23:59:59.999999"}\n'
        )
