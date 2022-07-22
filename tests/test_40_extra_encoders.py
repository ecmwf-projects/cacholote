import os.path
from typing import TypeVar

import pytest

from callcache import cache, config, decode, encode, extra_encoders

xr = pytest.importorskip("xarray")
T = TypeVar("T")


def func(a: T) -> T:
    return a


def test_dictify_xr_dataset() -> None:
    data_name = "a7279f6557c7eb114f8287b308a5eb43b4a5567628369892d27291de.nc"
    data_path = os.path.join(config.SETTINGS["cache"].directory, data_name)
    data = xr.Dataset(data_vars={"data": [0]})
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path,),
    }
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected

    data_name1 = "2f621f66051eee9e5edc3e9c6e3642c82e5b24ff3e72b579ab9bb2ab.nc"
    data_path1 = os.path.join(config.SETTINGS["cache"].directory, data_name1)
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


def test_roundtrip() -> None:
    data = xr.Dataset(data_vars={"data": [0]})
    date_json = encode.dumps(data)
    assert decode.loads(date_json).identical(data)


def test_cacheable() -> None:
    cfunc = cache.cacheable(func)

    data = xr.Dataset(data_vars={"data": [0]})
    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache"].stats() == (0, 1)

    # FIXME: why do we get two misses?
    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache"].stats() == (0, 2)

    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache"].stats() == (1, 2)
