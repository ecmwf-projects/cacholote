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


def test_hexdigestify() -> None:
    text = "some random Unicode text \U0001f4a9"
    expected = "278a2cefeef9a3269f4ba1c41ad733a4c07101ae6859f607c8a42cf2"
    res = cache.hexdigestify(text)
    assert res == expected


def test_cacheable() -> None:
    cfunc = cache.cacheable(func)
    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    assert config.SETTINGS["cache_store"].stats() == (1, 1)

    class Dummy:
        pass

    inst = Dummy()
    with pytest.warns(UserWarning, match="bad input"):
        res = cfunc(inst)
    assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}

    with pytest.warns(UserWarning, match="bad output"):
        res = cfunc("test", b=1)
    assert res.__class__.__name__ == "LocalClass"
