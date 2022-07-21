import os.path
from typing import TypeVar

import pytest

import callcache
from callcache import cache, decode, encode, extra_encoders

xr = pytest.importorskip("xarray")
T = TypeVar("T")


def func(a: T) -> T:
    return a


def test_dictify_xr_dataset() -> None:
    data_name = "a6c9d74e563abf0d5527a1c3bad999bde7d10ab0e66cfe33c2969098.nc"
    data_path = os.path.join(callcache.SETTINGS["cache"].directory, data_name)
    data = xr.Dataset(data_vars={"data": [0]})
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path,),
    }
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected

    data_name1 = "58249f0d51d51f386c8180e2b6ca2cb2907cc15c26852efc9ecf2be0.nc"
    data_path1 = os.path.join(callcache.SETTINGS["cache"].directory, data_name1)
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path1,),
    }
    data = xr.open_dataset(data_path)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected

    # skip saving a file already present on disk
    data = xr.open_dataset(data_path1)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected

    data["data"].values[0] = 1
    with pytest.raises(RuntimeError):
        extra_encoders.dictify_xr_dataset(data, data_path1)


def test_roundtrip() -> None:
    data = xr.Dataset(data_vars={"data": [0]})
    date_json = encode.dumps(data)
    assert decode.loads(date_json).identical(data)


def test_cacheable() -> None:
    cfunc = cache.cacheable()(func)
    callcache.SETTINGS["cache"].clear()
    callcache.SETTINGS["cache"].stats(reset=True)

    data = xr.Dataset(data_vars={"data": [0]})
    res = cfunc(data)
    assert res.identical(data)
    assert callcache.SETTINGS["cache"].stats() == (0, 1)

    # FIXME: why do we get two misses?
    res = cfunc(data)
    assert res.identical(data)
    assert callcache.SETTINGS["cache"].stats() == (0, 2)

    res = cfunc(data)
    assert res.identical(data)
    assert callcache.SETTINGS["cache"].stats() == (1, 2)
