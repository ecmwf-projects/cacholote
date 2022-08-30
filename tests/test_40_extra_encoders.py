import glob
import os
from typing import TypeVar

import pytest

from cacholote import cache, config, decode, encode, extra_encoders

try:
    import xarray as xr
except ImportError:
    pytest.importorskip("xarray")

T = TypeVar("T")


def func(a: T) -> T:
    return a


@pytest.fixture
def ds() -> xr.Dataset:
    import pooch

    fname = pooch.retrieve(
        url="https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib",
        known_hash="c144fde61ca5d53702bf6f212775ef2cc783bdd66b6865160bf597c1b35ed898",
    )
    ds = xr.open_dataset(fname)
    del ds.attrs["history"]

    return ds.sel(number=0)


xr_parametrize = (
    "xarray_cache_type,extension",
    [("application/netcdf", ".nc"), ("application/wmo-GRIB2", ".grb2")],
)


@pytest.mark.parametrize(*xr_parametrize)
def test_dictify_xr_dataset(
    ds: xr.Dataset, xarray_cache_type: str, extension: str
) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        f"285ee3a510a225620bb32d96ec20d19d9d91ae82be881e0b4c8320e4{extension}",
    )
    expected = {
        "type": xarray_cache_type,
        "href": f"./285ee3a510a225620bb32d96ec20d19d9d91ae82be881e0b4c8320e4{extension}",
        "file:checksum": "285ee3a510a225620bb32d96ec20d19d9d91ae82be881e0b4c8320e4",
        "file:size": 470024,
        "file:local_path": local_path,
        "xarray:open_kwargs": {},
        "xarray:storage_options": {},
    }
    with config.set(xarray_cache_type=xarray_cache_type):
        res = extra_encoders.dictify_xr_dataset(ds)
    assert res == expected
    assert os.path.exists(local_path)


@pytest.mark.parametrize(*xr_parametrize)
def test_xr_roundtrip(ds: xr.Dataset, xarray_cache_type: str, extension: str) -> None:
    with config.set(xarray_cache_type=xarray_cache_type):
        ds_json = encode.dumps(ds)
        res = decode.loads(ds_json)

    if xarray_cache_type == "application/wmo-GRIB2":
        xr.testing.assert_equal(res, ds)
    else:
        xr.testing.assert_identical(res, ds)


@pytest.mark.parametrize(*xr_parametrize)
def test_xr_cacheable(ds: xr.Dataset, xarray_cache_type: str, extension: str) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        f"285ee3a510a225620bb32d96ec20d19d9d91ae82be881e0b4c8320e4{extension}",
    )

    with config.set(xarray_cache_type=xarray_cache_type):
        cfunc = cache.cacheable(func)

        # 1: create cached data
        res = cfunc(ds)
        assert config.SETTINGS["cache_store"].stats() == (0, 1)
        mtime = os.path.getmtime(local_path)

        if xarray_cache_type == "application/wmo-GRIB2":
            xr.testing.assert_equal(res, ds)
        else:
            xr.testing.assert_identical(res, ds)

        # 2: use cached data
        res = cfunc(ds)
        assert config.SETTINGS["cache_store"].stats() == (1, 1)
        assert mtime == os.path.getmtime(local_path)
        assert glob.glob(
            os.path.join(config.SETTINGS["cache_store"].directory, f"*{extension}")
        ) == [local_path]

        if xarray_cache_type == "application/wmo-GRIB2":
            xr.testing.assert_equal(res, ds)
        else:
            xr.testing.assert_identical(res, ds)


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
