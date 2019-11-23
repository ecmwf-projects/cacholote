import pytest

from callcache import decode


def test_import_object():
    # Goedel-style self-reference :)
    obj = decode.import_object("test_decode:test_import_object")
    assert obj is test_import_object

    obj = decode.import_object("builtins:len")
    assert obj is len

    with pytest.raises(ValueError):
        decode.import_object("builtins.len")


def test_call_object_hook():
    maxyear_simple = {
        "type": "python_object",
        "fully_qualified_name": "builtins:OSError",
    }
    res = decode.call_object_hook(maxyear_simple)
    assert res is OSError

    len_simple = {"type": "python_object", "fully_qualified_name": "builtins:len"}
    res = decode.call_object_hook(len_simple)
    assert res is len

    len_call_simple = {"type": "python_call", "callable": len, "args": ["test"]}
    res = decode.call_object_hook(len_call_simple)
    assert res == len("test")

    unsupported_object = {"key": 1}
    res = decode.call_object_hook(unsupported_object)
    assert res is unsupported_object


def test_loads():
    len_call_json = '{"type":"python_call","callable":{"type":"python_object","fully_qualified_name":"builtins:int"}}'

    res = decode.loads(len_call_json)
    assert res == int()
