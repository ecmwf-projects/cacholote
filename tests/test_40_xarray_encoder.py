import tempfile

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


@pytest.mark.parametrize("set_cache", ["file", "ftp", "s3"], indirect=True)
def test_dictify_xr_dataset(tmpdir: str, set_cache: str) -> None:
    pytest.importorskip("netCDF4")

    ds = xr.Dataset({"foo": [0]}, attrs={})
    actual = extra_encoders.dictify_xr_dataset(ds)

    if set_cache == "s3":
        href = actual["href"]
        assert href.startswith(
            "http://127.0.0.1:5555/test-bucket/247fd17e087ae491996519c097e70e48.nc"
        )
        fs = fsspec.filesystem("http")
        local_prefix = "s3://test-bucket"
        storage_options = {}
    elif set_cache == "ftp":
        href = "ftp:///247fd17e087ae491996519c097e70e48.nc"
        storage_options = {
            "host": "localhost",
            "port": 2121,
            "username": "user",
            "password": "pass",
        }
        fs = fsspec.filesystem(set_cache, **storage_options)
        local_prefix = "ftp://"
    else:
        href = f"{set_cache}://{tmpdir}/247fd17e087ae491996519c097e70e48.nc"
        fs = fsspec.filesystem(set_cache)
        local_prefix = tmpdir
        storage_options = {}

    expected = {
        "type": "application/netcdf",
        "href": href,
        "file:checksum": fs.checksum(href),
        "file:size": 669,
        "file:local_path": f"{local_prefix}/247fd17e087ae491996519c097e70e48.nc",
        "xarray:storage_options": storage_options,
        "xarray:open_kwargs": {"engine": "netcdf4", "chunks": "auto"},
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
@pytest.mark.parametrize("set_cache", ["file", "ftp", "s3"], indirect=True)
def test_xr_cacheable(
    tmpdir: str,
    xarray_cache_type: str,
    ext: str,
    importorskip: str,
    set_cache: str,
) -> None:
    pytest.importorskip(importorskip)

    if xarray_cache_type == "application/vnd+zarr" and set_cache == "ftp":
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
            if set_cache == "file":
                assert (
                    actual.encoding["source"]
                    == f"{tmpdir}/06810be7ce1f5507be9180bfb9ff14fd{ext}"
                )
            else:
                # read from tmp local file
                assert actual.encoding["source"].startswith(tempfile.gettempdir())
                assert (
                    f"/06810be7ce1f5507be9180bfb9ff14fd{ext}"
                    in actual.encoding["source"]
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
