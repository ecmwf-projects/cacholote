import tempfile
from typing import Any, Dict

import fsspec
import pytest

from cacholote import cache, config, extra_encoders, utils

try:
    import xarray as xr
except ImportError:
    pytest.importorskip("xarray")


def get_grib_ds() -> "xr.Dataset":
    url = "https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib"
    with fsspec.open(f"simplecache::{url}", simplecache={"same_names": True}) as of:
        fname = of.name
    ds = xr.open_dataset(fname, engine="cfgrib")
    del ds.attrs["history"]
    return ds.sel(number=0)


@pytest.mark.parametrize("set_cache", ["file", "s3"], indirect=True)
def test_dictify_xr_dataset(tmpdir: str, set_cache: str) -> None:
    pytest.importorskip("netCDF4")

    ds = xr.Dataset({"foo": [0]}, attrs={})
    actual = extra_encoders.dictify_xr_dataset(ds)
    if set_cache == "s3":
        href = actual["href"]
        assert href.startswith(
            "http://127.0.0.1:5555/test-bucket/247fd17e087ae491996519c097e70e48.nc"
        )
        checksum = fsspec.filesystem("http").checksum(href)
        local_dir = "test-bucket"
    else:
        href = f"{set_cache}://{tmpdir}/247fd17e087ae491996519c097e70e48.nc"
        checksum = fsspec.filesystem(set_cache).checksum(href)
        local_dir = tmpdir
    expected = {
        "type": "application/netcdf",
        "href": href,
        "file:checksum": checksum,
        "file:size": 669,
        "file:local_path": f"{local_dir}/247fd17e087ae491996519c097e70e48.nc",
        "xarray:storage_options": {},
        "xarray:open_kwargs": {"chunks": "auto"},
    }
    assert actual == expected


@pytest.mark.parametrize(
    "xarray_cache_type,ext,importorskip",
    [
        ("application/netcdf", ".nc", "netCDF4"),
        ("application/x-grib", ".grib", "cfgrib"),
        ("application/vnd+zarr", ".zarr", "zarr"),
    ],
)
@pytest.mark.parametrize("set_cache", ["file", "ftp"], indirect=True)
def test_xr_cacheable(
    tmpdir: str,
    xarray_cache_type: str,
    ext: str,
    importorskip: str,
    set_cache: Dict[str, Any],
) -> None:
    pytest.importorskip(importorskip)

    cache_is_local = config.SETTINGS["cache_files_urlpath"] is None
    if xarray_cache_type == "application/vnd+zarr" and not cache_is_local:
        pytest.xfail(
            "fsspec mapper does not play well with pyftpdlib: 550 No such file or directory"
        )

    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)

    infos = []
    for expected_stats in ((0, 1), (1, 1)):
        with config.set(xarray_cache_type=xarray_cache_type):
            dirfs = utils.get_cache_files_dirfs()
            actual = cfunc()

        # Check hit & miss
        assert config.SETTINGS["cache_store"].stats() == expected_stats

        infos.append(dirfs.info(f"06810be7ce1f5507be9180bfb9ff14fd{ext}"))

        # Check result
        if xarray_cache_type == "application/x-grib":
            xr.testing.assert_equal(actual, expected)
        else:
            xr.testing.assert_identical(actual, expected)

        # Check source file
        if xarray_cache_type != "application/vnd+zarr":
            # zarr mapper is not added to encoding
            if cache_is_local:
                assert (
                    actual.encoding["source"]
                    == f"{tmpdir}/06810be7ce1f5507be9180bfb9ff14fd{ext}"
                )
            else:
                # read from tmp local file
                assert actual.encoding["source"].startswith(tempfile.gettempdir())
                assert actual.encoding["source"].endswith(
                    f"/06810be7ce1f5507be9180bfb9ff14fd{ext}"
                )

        # Check opened with dask
        assert dict(actual.chunks) == {
            "time": (4,),
            "isobaricInhPa": (2,),
            "latitude": (61,),
            "longitude": (120,),
        }

    # Check cached file is not modified
    assert infos[0] == infos[1]


@pytest.mark.parametrize(
    "xarray_cache_type,ext,importorskip",
    [
        ("application/netcdf", ".nc", "netCDF4"),
        ("application/vnd+zarr", ".zarr", "zarr"),
    ],
)
def test_xr_corrupted_files(
    tmpdir: str,
    xarray_cache_type: str,
    ext: str,
    importorskip: str,
) -> None:
    pytest.importorskip(importorskip)

    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)

    with config.set(xarray_cache_type=xarray_cache_type):
        dirfs = utils.get_cache_files_dirfs()
        cfunc()

    # Warn if file is corrupted
    dirfs.touch(f"06810be7ce1f5507be9180bfb9ff14fd{ext}", truncate=False)
    touched_info = dirfs.info(f"06810be7ce1f5507be9180bfb9ff14fd{ext}")
    with config.set(xarray_cache_type=xarray_cache_type), pytest.warns(
        UserWarning, match="checksum mismatch"
    ):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert dirfs.info(f"06810be7ce1f5507be9180bfb9ff14fd{ext}") != touched_info

    # Warn if file is deleted
    dirfs.rm(f"06810be7ce1f5507be9180bfb9ff14fd{ext}", recursive=True)
    with config.set(xarray_cache_type=xarray_cache_type), pytest.warns(
        UserWarning, match="No such file or directory"
    ):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert dirfs.exists(f"06810be7ce1f5507be9180bfb9ff14fd{ext}")
