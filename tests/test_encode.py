from callcache import encode


def func(a, b, *args, c=None, d=False, **kwargs):
    pass


def test_inspect_fully_qualified_name():
    res = encode.inspect_fully_qualified_name(func)
    assert res == "test_encode:func"

    res = encode.inspect_fully_qualified_name(len)
    assert res == "builtins:len"


def test_dictify_python_object():
    res = encode.dictify_python_object(len)
    assert res == {"type": "python_object", "fully_qualified_name": "builtins:len"}


def test_dictify_python_call():
    expected0 = {
        "type": "python_call",
        "callable": {"type": "python_object", "fully_qualified_name": "builtins:int"},
    }
    res0 = encode.dictify_python_call(int)
    assert res0 == expected0

    expected1 = {
        "type": "python_call",
        "callable": {"type": "python_object", "fully_qualified_name": "builtins:len"},
        "args": ("test",),
    }
    res1 = encode.dictify_python_call(len, "test")
    assert res1 == expected1

    expected1 = {
        "type": "python_call",
        "callable": {"type": "python_object", "fully_qualified_name": "builtins:int"},
        "args": ("2",),
        "kwargs": {"base": 2},
    }
    res1 = encode.dictify_python_call(int, "2", base=2)
    assert res1 == expected1
