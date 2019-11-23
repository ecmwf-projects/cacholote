import datetime

import pytest

from callcache import decode
from callcache import encode


def test_inspect_fully_qualified_name():
    # Goedel-style self-reference :)
    res = encode.inspect_fully_qualified_name(test_inspect_fully_qualified_name)
    assert res == "test_encode:test_inspect_fully_qualified_name"

    res = encode.inspect_fully_qualified_name(len)
    assert res == "builtins:len"


def test_dictify_python_object():
    res = encode.dictify_python_object(len)
    assert res == {"type": "python_object", "fully_qualified_name": "builtins:len"}

    expected = {
        "type": "python_object",
        "fully_qualified_name": "datetime:datetime.isoformat",
    }
    res = encode.dictify_python_object("datetime:datetime.isoformat")
    assert res == expected

    with pytest.raises(ValueError):
        encode.dictify_python_object("datetime.datetime.isoformat")


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


def test_filecache_default():
    date = datetime.datetime.now()
    expected = {
        "type": "python_call",
        "callable": {
            "type": "python_object",
            "fully_qualified_name": "datetime:datetime.fromisoformat",
        },
        "args": (date.isoformat(),),
    }
    res = encode.filecache_default(date)
    assert res == expected

    data = bytes(list(range(255)))
    expected = {
        "type": "python_call",
        "callable": {"type": "python_object", "fully_qualified_name": "builtins:bytes"},
        "args": (list(range(255)),),
    }
    res = encode.filecache_default(data)
    assert res == expected

    class Dummy:
        pass

    with pytest.raises(TypeError):
        encode.filecache_default(Dummy())


def test_roundtrip():
    data = len
    assert decode.loads(encode.dumps(data)) == data

    data = bytes(list(range(255)))
    assert decode.loads(encode.dumps(data)) == data

    data = datetime.datetime.now()
    assert decode.loads(encode.dumps(data)) == data

    data = datetime.date.today()
    assert decode.loads(encode.dumps(data)) == data

    data = datetime.timedelta(234, 23, 128736)
    assert decode.loads(encode.dumps(data)) == data
