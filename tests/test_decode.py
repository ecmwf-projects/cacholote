
import pytest

from callcache import decode


def func(a, b, *args, c=None, d=False, **kwargs):
    pass


def test_import_object():
    obj = decode.import_object("test_decode:func")
    assert obj is func

    obj = decode.import_object("builtins:len")
    assert obj is len

    with pytest.raises(ValueError):
        decode.import_object("builtins.len")


def test_call_object_hook():
    maxyear_simple = {
        "type": "python_object",
        "fully_qualified_name": "builtins:ValueError",
    }
    res = decode.call_object_hook(maxyear_simple)
    assert res is ValueError

    len_simple = {"type": "python_object", "fully_qualified_name": "builtins:len"}
    res = decode.call_object_hook(len_simple)
    assert res is len

    len_call_simple = {"type": "python_call", "callable": len, "args": ["test"]}
    res = decode.call_object_hook(len_call_simple)
    assert res == len("test")


def test_loads():
    len_call_json = '{"type":"python_call","callable":{"type":"python_object","fully_qualified_name":"builtins:int"}}'

    res = decode.loads(len_call_json)
    assert res == int()
