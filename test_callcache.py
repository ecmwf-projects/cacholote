import callcache


def func(a, b, *args, c=None, d=False, **kwargs):
    pass


def test_make_unique_arguments():
    expected_1 = ((1, 2), {"c": None, "d": False, "e": 4})

    assert callcache.make_unique_arguments(func, 1, 2, e=4) == expected_1
    assert callcache.make_unique_arguments(func, e=4, b=2, a=1) == expected_1
    assert callcache.make_unique_arguments(func, c=None, e=4, b=2, a=1) == expected_1

    expected_2 = ((1, 2, 3), {"c": None, "d": False, "e": 4})
    assert callcache.make_unique_arguments(func, 1, 2, 3, e=4) == expected_2
    assert callcache.make_unique_arguments(func, 1, 2, 3, e=4, c=None) == expected_2

    assert callcache.make_unique_arguments(len, "test") == (("test",), {})


def test_make_unique_arguments_order():
    expected = [("c", None), ("d", False), ("e", 4), ("f", 5)]

    _, res = callcache.make_unique_arguments(func, 1, 2, e=4, f=5)

    assert list(res.items()) == expected

    _, res = callcache.make_unique_arguments(func, 1, 2, f=5, e=4)

    assert list(res.items()) == expected


def test_inspect_fully_qualified_name():
    res = callcache.inspect_fully_qualified_name(func)
    assert res == "test_callcache:func"

    res = callcache.inspect_fully_qualified_name(len)
    assert res == "builtins:len"


def test_import_object():
    obj = callcache.import_object("test_callcache:func")
    assert obj is func

    obj = callcache.import_object("builtins:len")
    assert obj is len


def test_make_unique_call_signature_json():
    expected = '{"callable":"test_callcache:func","args":[1,2],"kwargs":{"c":null,"d":false,"e":4,"f":5}}'

    res = callcache.make_unique_call_signature_json(func, 1, 2, e=4, f=5)
    assert res == expected

    res = callcache.make_unique_call_signature_json(func, f=5, d=False, b=2, a=1, e=4)
    assert res == expected


def test_make_unique_call_signatures():
    expected_json = '{"callable":"test_callcache:func","args":[1,2],"kwargs":{"c":null,"d":false,"e":4,"f":5}}'
    expected_hexdigest = "fe799885cb9de3c6c8fae87fa7783577eea267e721a70fefb3aaea95"

    _, res_json, res_hexdigest = callcache.make_unique_call_signatures(
        func, 1, 2, e=4, f=5
    )
    assert res_json == expected_json
    assert res_hexdigest == expected_hexdigest

    _, res_json, res_hexdigest = callcache.make_unique_call_signatures(
        func, f=5, d=False, b=2, a=1, e=4
    )
    assert res_json == expected_json
    assert res_hexdigest == expected_hexdigest
