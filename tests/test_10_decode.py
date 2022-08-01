import pytest

from cacholote import decode


def test_import_object() -> None:
    # Goedel-style self-reference :)
    res = decode.import_object("test_10_decode:test_import_object")
    assert res is test_import_object

    res = decode.import_object("builtins:len")
    assert res is len

    with pytest.raises(
        ValueError, match=r"'builtins.len' not in the form 'module:qualname'"
    ):
        decode.import_object("builtins.len")


def test_object_hook() -> None:
    object_simple = {
        "type": "python_object",
        "fully_qualified_name": "builtins:OSError",
    }
    res = decode.object_hook(object_simple)
    assert res is OSError

    len_simple = {"type": "python_object", "fully_qualified_name": "builtins:len"}
    res = decode.object_hook(len_simple)
    assert res is len

    len_call_simple = {"type": "python_call", "callable": len, "args": ["test"]}
    res = decode.object_hook(len_call_simple)
    assert res == len("test")

    len_call_simple = {
        "type": "python_call",
        "callable": "builtins:len",
        "args": ["test"],
    }
    res = decode.object_hook(len_call_simple)
    assert res == len("test")

    unsupported_object = {"key": 1}
    res = decode.object_hook(unsupported_object)
    assert res is unsupported_object

    unsupported_type = {"type": "unsupported_type"}
    res = decode.object_hook(unsupported_type)
    assert res is unsupported_type


def test_loads() -> None:
    len_call_json = (
        r'{"type":"python_call",'
        r'"callable":{"type":"python_object","fully_qualified_name":"builtins:int"}}'
    )

    res = decode.loads(len_call_json)
    assert res == int()
