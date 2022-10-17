import pathlib
import sqlite3
from typing import Any

import pytest

from cacholote import cache


def func(a: Any, *args: Any, b: Any = None, **kwargs: Any) -> Any:
    if b is None:
        return locals()
    else:

        class LocalClass:
            pass

        return LocalClass()


def test_cacheable(tmpdir: pathlib.Path) -> None:

    con = sqlite3.connect(str(tmpdir / "cacholote.db"))
    cur = con.cursor()

    cfunc = cache.cacheable(func)
    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    cur.execute("SELECT key, value, count FROM cacholote")
    assert cur.fetchall() == [
        (
            "a8260ac3cdc1404aa64a6fb71e85304922e86bcab2eeb6177df5c933",
            '{"a":"test","b":null,"args":[],"kwargs":{}}',
            1,
        )
    ]

    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    cur.execute("SELECT key, value, count FROM cacholote")
    assert cur.fetchall() == [
        (
            "a8260ac3cdc1404aa64a6fb71e85304922e86bcab2eeb6177df5c933",
            '{"a":"test","b":null,"args":[],"kwargs":{}}',
            2,
        )
    ]

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
