import glob
import os
from typing import TypeVar

import pytest

from cacholote import cache, config, decode, encode, extra_encoders

try:
    import xarray as xr
finally:
    pytest.importorskip("dask")
    pytest.importorskip("xarray")
    pytest.importorskip("zarr")

T = TypeVar("T")


def func(a: T) -> T:
    return a


@pytest.fixture
def ds() -> xr.Dataset:
    return xr.Dataset({"foo": 0}, attrs={})


def test_dictify_xr_dataset(ds: xr.Dataset) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "8671e37e1cb2cbfdc00e80ecf2efcf3ed972385eeb5d4bc58d329af0.zarr",
    )
    expected = {
        "type": "application/vnd+zarr",
        "href": local_path,
        "file:checksum": "8671e37e1cb2cbfdc00e80ecf2efcf3ed972385eeb5d4bc58d329af0",
        "file:size": 8,
        "file:local_path": local_path,
        "xarray:open_kwargs": {"consolidated": True, "engine": "zarr"},
        "xarray:storage_options": {},
    }
    res = extra_encoders.dictify_xr_dataset(ds)
    assert res == expected
    assert os.path.exists(local_path)


def test_xr_roundtrip(ds: xr.Dataset) -> None:
    ds_json = encode.dumps(ds)
    res = decode.loads(ds_json)
    xr.testing.assert_identical(res, ds)


def test_xr_cacheable(ds: xr.Dataset) -> None:
    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "8671e37e1cb2cbfdc00e80ecf2efcf3ed972385eeb5d4bc58d329af0.zarr",
    )

    cfunc = cache.cacheable(func)

    # 1: create cached data
    res = cfunc(ds)
    assert config.SETTINGS["cache_store"].stats() == (0, 1)
    mtime = os.path.getmtime(local_path)
    xr.testing.assert_identical(res, ds)
    assert ds["foo"].encoding == {}
    assert set(res["foo"].encoding) == {
        "chunks",
        "compressor",
        "dtype",
        "filters",
        "preferred_chunks",
    }

    # 2: use cached data
    res = cfunc(ds)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
    assert mtime == os.path.getmtime(local_path)
    assert glob.glob(
        os.path.join(config.SETTINGS["cache_store"].directory, "*.zarr")
    ) == [local_path]
    xr.testing.assert_identical(res, ds)
    assert ds["foo"].encoding == {}
    assert set(res["foo"].encoding) == {
        "chunks",
        "compressor",
        "dtype",
        "filters",
        "preferred_chunks",
    }
