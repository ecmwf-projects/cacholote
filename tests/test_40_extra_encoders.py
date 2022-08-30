import os
from typing import TypeVar

import pytest

from cacholote import cache, config, decode, encode, extra_encoders

pytest.importorskip("xarray")
import xarray as xr  # noqa: E402 (import xarray after importorskip for mypy)

T = TypeVar("T")


def func(a: T) -> T:
    return a


@pytest.fixture
def ds() -> xr.Dataset:
    # See: https://github.com/pydata/xarray/issues/6970
    return xr.Dataset(data_vars={"data": [0]}, attrs={})


def test_dictify_xr_dataset(ds: xr.Dataset) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
    )
    expected = {
        "type": "application/netcdf",
        "href": "./e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
        "file:checksum": "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852",
        "file:size": 8,
        "file:local_path": local_path,
        "xarray:open_kwargs": {},
        "xarray:storage_options": {},
    }
    res = extra_encoders.dictify_xr_dataset(ds)
    assert res == expected
    assert os.path.exists(local_path)

    ds1 = xr.open_dataset(local_path)
    res = extra_encoders.dictify_xr_dataset(ds1)
    assert res == expected


def test_roundtrip(ds: xr.Dataset) -> None:
    date_json = encode.dumps(ds)
    xr.testing.assert_identical(decode.loads(date_json), ds)


def test_cacheable(ds: xr.Dataset) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
    )
    cfunc = cache.cacheable(func)

    res = cfunc(ds)
    mtime = os.path.getmtime(local_path)
    assert res.identical(ds)
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    res = cfunc(ds)
    assert mtime == os.path.getmtime(local_path)
    assert res.identical(ds)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)


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
