from callcache import cache


def func(a, *args, b=None, **kwargs):
    if b is None:
        return locals()
    else:

        class LocalClass:
            pass

        return LocalClass()


def test_hexdigestify():
    text = "some random Unicode text \U0001f4a9"
    expected = "278a2cefeef9a3269f4ba1c41ad733a4c07101ae6859f607c8a42cf2"
    res = cache.hexdigestify(text)
    assert res == expected


def test_cacheable():
    cfunc = cache.cacheable(func)
    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    assert cache.CACHE_STATS["miss"] == 1

    res = cfunc("test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    assert cache.CACHE_STATS["hit"] == 1

    res = cfunc(b=None, a="test")
    assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}
    assert cache.CACHE_STATS["hit"] == 2

    class Dummy:
        pass

    inst = Dummy()
    res = cfunc(inst)
    assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}
    assert cache.CACHE_STATS["uncacheable_input"] == 1

    res = cfunc("test", b=1)
    assert res.__class__.__name__ == "LocalClass"
    assert cache.CACHE_STATS["uncacheable_output"] == 1
