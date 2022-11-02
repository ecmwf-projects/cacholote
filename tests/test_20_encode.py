import datetime
from typing import Any

import pytest

from cacholote import config, decode, encode


def func(
    a: Any, b: Any, *args: Any, c: Any = None, d: Any = False, **kwargs: Any
) -> None:
    pass


def test_inspect_fully_qualified_name() -> None:
    # Goedel-style self-reference :)
    res = encode.inspect_fully_qualified_name(test_inspect_fully_qualified_name)
    assert res == "test_20_encode:test_inspect_fully_qualified_name"

    res = encode.inspect_fully_qualified_name(len)
    assert res == "builtins:len"


def test_dictify_python_object() -> None:
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


def test_dictify_python_call() -> None:
    expected0 = {"type": "python_call", "callable": "builtins:int"}
    res0 = encode.dictify_python_call(int)
    assert res0 == expected0

    expected1 = {"type": "python_call", "callable": "builtins:len", "args": ("test",)}
    res1 = encode.dictify_python_call(len, "test")
    assert res1 == expected1

    expected2 = {
        "type": "python_call",
        "callable": "builtins:int",
        "args": ("2",),
        "kwargs": {"base": 3},
    }
    res2 = encode.dictify_python_call(int, "2", base=3)
    assert res2 == expected2


def test_filecache_default() -> None:
    date = datetime.datetime.now()
    expected0 = {
        "type": "python_call",
        "callable": "datetime:datetime.fromisoformat",
        "args": (date.isoformat(),),
    }
    res0 = encode.filecache_default(date)

    assert res0 == expected0

    data1 = bytes(list(range(20)) + list(range(225, 256)))
    expected1 = {
        "type": "python_call",
        "callable": "binascii:a2b_base64",
        "args": (
            "AAECAwQFBgcICQoLDA0ODxAREhPh4uPk5ebn6Onq6+zt7u/w8fLz9PX29/j5+vv8/f7/\n",
        ),
    }
    res1 = encode.filecache_default(data1)
    assert res1 == expected1

    data2 = datetime.timezone(datetime.timedelta(seconds=3600))
    expected2 = {
        "type": "python_call",
        "callable": "_pickle:loads",
        "args": (
            b"\x80\x04\x958\x00\x00\x00\x00\x00\x00\x00\x8c\x08"
            b"datetime\x94\x8c\x08timezone\x94\x93\x94h\x00\x8c\t"
            b"timedelta\x94\x93\x94K\x00M\x10\x0eK\x00\x87\x94R\x94\x85\x94R\x94.",
        ),
    }
    res2 = encode.filecache_default(data2)
    assert res2 == expected2


@pytest.mark.parametrize("raise_all_encoding_errors", [True, False])
def test_filecache_default_error(raise_all_encoding_errors: bool) -> None:
    config.set(raise_all_encoding_errors=raise_all_encoding_errors)

    class Dummy:
        pass

    if raise_all_encoding_errors:
        with pytest.raises(AttributeError, match="Can't pickle local object"):
            encode.filecache_default(Dummy())
    else:
        with pytest.warns(UserWarning, match="Can't pickle local object"):
            with pytest.raises(encode.EncodeError):
                encode.filecache_default(Dummy())


@pytest.mark.parametrize(
    "data",
    [
        len,
        bytes(list(range(255))),
        datetime.datetime.now(),
        datetime.date.today(),
        datetime.timedelta(234, 23, 128736),
        datetime.timezone(datetime.timedelta(seconds=3600)),  # pickle encode / decode
    ],
    ids=[f"data{i}" for i in range(6)],
)
def test_roundtrip(data: Any) -> None:
    assert decode.loads(encode.dumps(data)) == data


def test_dumps_python_call() -> None:
    expected = (
        r'{"type":"python_call","callable":"datetime:datetime",'
        r'"args":[2019,1,1],"kwargs":{"tzinfo":{"type":"python_call",'
        r'"callable":"_pickle:loads","args":[{"type":"python_call",'
        r'"callable":"binascii:a2b_base64","args":["gASVOAAAAAAAAACMCGRhdGV0aW1ll'
        r'IwIdGltZXpvbmWUk5RoAIwJdGltZWRlbHRhlJOUSwBNEA5LAIeUUpSFlFKULg==\n"]}]}}}'
    )
    tzinfo = datetime.timezone(datetime.timedelta(seconds=3600))
    res = encode.dumps_python_call("datetime:datetime", 2019, 1, 1, tzinfo=tzinfo)
    assert res == expected

    res_decoded = decode.loads(res)
    assert res_decoded == datetime.datetime(2019, 1, 1, tzinfo=tzinfo)


def test_dumps_json_serializable() -> None:
    expected = "1"
    actual = encode.dumps(1)
    assert expected == actual
