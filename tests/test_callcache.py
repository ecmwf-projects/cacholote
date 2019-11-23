import datetime

import callcache


def func(a, b, *args, c=None, d=False, **kwargs):
    pass


def test_uniquify_arguments():
    expected_1 = ((1, 2), {"c": None, "d": False, "e": 4})

    assert callcache.uniquify_arguments(func, 1, 2, e=4) == expected_1
    assert callcache.uniquify_arguments(func, e=4, b=2, a=1) == expected_1
    assert callcache.uniquify_arguments(func, c=None, e=4, b=2, a=1) == expected_1

    expected_2 = ((1, 2, 3), {"c": None, "d": False, "e": 4})
    assert callcache.uniquify_arguments(func, 1, 2, 3, e=4) == expected_2
    assert callcache.uniquify_arguments(func, 1, 2, 3, e=4, c=None) == expected_2

    assert callcache.uniquify_arguments(len, "test") == (("test",), {})

    expected_2 = (("2019-01-01",), {})
    assert (
        callcache.uniquify_arguments(datetime.datetime.isoformat, "2019-01-01")
        == expected_2
    )


def test_uniquify_arguments_order():
    expected = [("c", None), ("d", False), ("e", 4), ("f", 5)]

    _, res = callcache.uniquify_arguments(func, 1, 2, e=4, f=5)

    assert list(res.items()) == expected

    _, res = callcache.uniquify_arguments(func, 1, 2, f=5, e=4)

    assert list(res.items()) == expected
