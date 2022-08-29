import os
from typing import TypeVar

import pytest

from cacholote import cache, config, decode, encode, extra_encoders

xr = pytest.importorskip("xarray")
T = TypeVar("T")


def func(a: T) -> T:
    return a


def test_dictify_xr_dataset() -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "1dd1448f0d6de747f46e528dc156981434ff6d92dbf1b84383bc5784.nc",
    )
    data = xr.Dataset(data_vars={"data": [0]})
    expected = {
        "type": "netcdf",
        "href": "./1dd1448f0d6de747f46e528dc156981434ff6d92dbf1b84383bc5784.nc",
        "file:checksum": "1dd1448f0d6de747f46e528dc156981434ff6d92dbf1b84383bc5784",
        "file:size": 8,
        "file:local_path": local_path,
        "xarray:open_kwargs": {},
        "xarray:storage_options": {},
    }
    res = extra_encoders.dictify_xr_dataset(data)
    print(res)
    assert res == expected
    assert os.path.exists(local_path)

    local_path1 = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
    )
    expected = {
        "type": "netcdf",
        "href": "./e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
        "file:checksum": "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852",
        "file:size": 8,
        "file:local_path": local_path1,
        "xarray:open_kwargs": {},
        "xarray:storage_options": {},
    }
    data = xr.open_dataset(local_path)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected
    assert os.path.exists(local_path1)

    # skip saving a file already present on disk
    mtime = os.path.getmtime(local_path1)
    data = xr.open_dataset(local_path1)
    res = extra_encoders.dictify_xr_dataset(data)
    assert res == expected
    assert mtime == os.path.getmtime(local_path1)


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
    cached_file = os.path.join(
        tmpdir, "6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7.txt"
    )

    with open(tmpfile, "w") as f:
        f.write("dummy")
    cfunc = cache.cacheable(func)

    res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    with open(cached_file, "r") as f:
        assert f.read() == "dummy"
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    # skip copying a file already in cache directory
    mtime = os.path.getmtime(cached_file)
    res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    assert mtime == os.path.getmtime(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)

    # do not crash if cached file is removed
    os.remove(cached_file)
    with pytest.warns(UserWarning):
        res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    assert os.path.exists(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (2, 1)
