import datetime

import pytest

from callcache import decode, encode


def func(a, b, *args, c=None, d=False, **kwargs):
    pass


def test_inspect_fully_qualified_name():
    # Goedel-style self-reference :)
    res = encode.inspect_fully_qualified_name(test_inspect_fully_qualified_name)
    assert res == "test_20_encode:test_inspect_fully_qualified_name"

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
    expected0 = {"type": "python_call", "callable": "builtins:int"}
    res0 = encode.dictify_python_call(int)
    assert res0 == expected0

    expected0 = {"type": "python_call", "callable": "builtins:int", "version": "0.1"}
    res0 = encode.dictify_python_call(int, _callable_version="0.1")
    assert res0 == expected0

    expected1 = {"type": "python_call", "callable": "builtins:len", "args": ("test",)}
    res1 = encode.dictify_python_call(len, "test")
    assert res1 == expected1

    expected1 = {
        "type": "python_call",
        "callable": "builtins:int",
        "args": ("2",),
        "kwargs": {"base": 3},
    }
    res1 = encode.dictify_python_call(int, "2", base=3)
    assert res1 == expected1


def test_filecache_default():
    date = datetime.datetime.now()
    expected = {
        "type": "python_call",
        "callable": "datetime:datetime.fromisoformat",
        "args": (date.isoformat(),),
    }
    res = encode.filecache_default(date)
    assert res == expected

    data = bytes(list(range(20)) + list(range(225, 256)))
    expected = {
        "type": "python_call",
        "callable": "binascii:a2b_base64",
        "args": (
            "AAECAwQFBgcICQoLDA0ODxAREhPh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/\n",
        ),
    }
    res = encode.filecache_default(data)
    assert res == expected

    data = datetime.timezone(datetime.timedelta(seconds=3600))
    expected = {
        "type": "python_call",
        "callable": "_pickle:loads",
        "args": (
            b"\x80\x04\x958\x00\x00\x00\x00\x00\x00\x00\x8c\x08datetime\x94\x8c\x08timezone\x94\x93\x94h\x00\x8c\ttimedelta\x94\x93\x94K\x00M\x10\x0eK\x00\x87\x94R\x94\x85\x94R\x94.",
        ),
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

    # pickle encode / decode
    data = datetime.timezone(datetime.timedelta(seconds=3600))
    assert decode.loads(encode.dumps(data)) == data


def test_dumps_python_call():
    expected = r'{"type":"python_call","callable":"datetime:datetime","args":[2019,1,1],"kwargs":{"tzinfo":{"type":"python_call","callable":"_pickle:loads","args":[{"type":"python_call","callable":"binascii:a2b_base64","args":["gASVOAAAAAAAAACMCGRhdGV0aW1llIwIdGltZXpvbmWUk5RoAIwJdGltZWRlbHRhlJOUSwBNEA5LAIeUUpSFlFKULg==\n"]}]}}}'
    tzinfo = datetime.timezone(datetime.timedelta(seconds=3600))
    res = encode.dumps_python_call("datetime:datetime", 2019, 1, 1, tzinfo=tzinfo)
    assert res == expected

    res_decoded = decode.loads(res)
    assert res_decoded == datetime.datetime(2019, 1, 1, tzinfo=tzinfo)
