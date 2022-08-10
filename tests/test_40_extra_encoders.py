import os
from typing import TypeVar

import pytest

from cacholote import cache, config, decode, encode, extra_encoders

xr = pytest.importorskip("xarray")
T = TypeVar("T")


def func(a: T) -> T:
    return a


def test_dictify_xr_dataset() -> None:
    data_name = "1dd1448f0d6de747f46e528dc156981434ff6d92dbf1b84383bc5784.nc"
    data_path = os.path.join(config.SETTINGS["cache_store"].directory, data_name)
    data = xr.Dataset(data_vars={"data": [0]})
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path,),
    }
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected
    assert os.path.exists(data_path)

    data_name1 = "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc"
    data_path1 = os.path.join(config.SETTINGS["cache_store"].directory, data_name1)
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path1,),
    }
    data = xr.open_dataset(data_path)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected
    assert os.path.exists(data_path1)

    # skip saving a file already present on disk
    mtime = os.path.getmtime(data_path1)
    data = xr.open_dataset(data_path1)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected
    assert mtime == os.path.getmtime(data_path1)


def test_roundtrip() -> None:
    data = xr.Dataset(data_vars={"data": [0]})
    date_json = encode.dumps(data)
    assert decode.loads(date_json).identical(data)


def test_cacheable() -> None:
    cfunc = cache.cacheable(func)

    data = xr.Dataset(data_vars={"data": [0]})
    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    # FIXME: why do we get two misses?
    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache_store"].stats() == (0, 2)

    res = cfunc(data)
    assert res.identical(data)
    assert config.SETTINGS["cache_store"].stats() == (1, 2)


def test_copy_file_to_cache_directory(tmpdir: str) -> None:
    tmpfile = os.path.join(tmpdir, "dummy.txt")
    with open(tmpfile, "w") as f:
        f.write("dummy")
    cfunc = cache.cacheable(func)

    assert cfunc(open(tmpfile)).read() == "dummy"
    cached_file = os.path.join(
        tmpdir, "6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7"
    )
    assert os.path.exists(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    # skip copying a file already in cache directory
    mtime = os.path.getmtime(cached_file)
    assert cfunc(open(tmpfile)).read() == "dummy"
    assert mtime == os.path.getmtime(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
