import glob
import os
from typing import TypeVar

import fsspec
import pytest

from cacholote import cache, config, decode, encode, extra_encoders

try:
    import xarray as xr
finally:
    pytest.importorskip("xarray")
    pytest.importorskip("dask")

T = TypeVar("T")


def func(a: T) -> T:
    return a


@pytest.fixture
def ds() -> xr.Dataset:
    with fsspec.open(
        "filecache::https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib"
    ) as of:
        fname = of.name
    ds = xr.open_dataset(fname, engine="cfgrib")
    del ds.attrs["history"]

    return ds.sel(number=0)


xr_parametrize = (
    "xarray_cache_type,extension",
    [("application/x-netcdf", ".nc"), ("application/x-grib", ".grb")],
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
        "href": local_path,
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

    if xarray_cache_type == "application/x-grib":
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

        if xarray_cache_type == "application/x-grib":
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

        if xarray_cache_type == "application/x-grib":
            xr.testing.assert_equal(res, ds)
        else:
            xr.testing.assert_identical(res, ds)
