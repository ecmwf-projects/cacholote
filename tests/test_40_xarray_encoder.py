import pathlib

import fsspec
import pytest

from cacholote import cache, config, decode, encode, extra_encoders, utils

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


@pytest.mark.filterwarnings(
    "ignore:distutils Version classes are deprecated. Use packaging.version instead."
)
def test_dictify_xr_dataset(tmpdir: pathlib.Path) -> None:
    pytest.importorskip("netCDF4")

    readonly_dir = tmpdir / "readonly"
    fsspec.filesystem("file").mkdir(readonly_dir)

    ds = xr.Dataset({"foo": [0]}, attrs={})
    with config.set(cache_files_urlpath_readonly=readonly_dir):
        actual = extra_encoders.dictify_xr_dataset(ds)

    href = f"{readonly_dir}/247fd17e087ae491996519c097e70e48.nc"
    local_path = f"{tmpdir}/247fd17e087ae491996519c097e70e48.nc"
    expected = {
        "type": "application/netcdf",
        "href": href,
        "file:checksum": fsspec.filesystem("file").checksum(local_path),
        "file:size": 669,
        "file:local_path": local_path,
        "xarray:storage_options": {},
        "xarray:open_kwargs": {"chunks": "auto"},
    }
    assert actual == expected

    fsspec.filesystem("file").mv(local_path, href)
    xr.testing.assert_identical(ds, decode.loads(encode.dumps(actual)))


@pytest.mark.parametrize(
    "xarray_cache_type,ext,importorskip",
    [
        ("application/netcdf", ".nc", "netCDF4"),
        ("application/x-grib", ".grib", "cfgrib"),
        ("application/vnd+zarr", ".zarr", "zarr"),
    ],
)
@pytest.mark.parametrize("set_cache", ["file", "ftp", "s3"], indirect=True)
@pytest.mark.filterwarnings(
    "ignore:GRIB write support is experimental, DO NOT RELY ON IT!"
)
@pytest.mark.filterwarnings(
    "ignore:distutils Version classes are deprecated. Use packaging.version instead."
)
def test_xr_cacheable(
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

        infos.append(dirfs.info(f"71b1251a1f7f7ce64c1e1a436613c023{ext}"))

        # Check result
        if xarray_cache_type == "application/x-grib":
            xr.testing.assert_equal(actual, expected)
        else:
            xr.testing.assert_identical(actual, expected)

        # Check opened with dask (i.e., read from file)
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
    dirfs.touch(f"71b1251a1f7f7ce64c1e1a436613c023{ext}", truncate=False)
    touched_info = dirfs.info(f"71b1251a1f7f7ce64c1e1a436613c023{ext}")
    with config.set(xarray_cache_type=xarray_cache_type), pytest.warns(
        UserWarning, match="checksum mismatch"
    ):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert dirfs.info(f"71b1251a1f7f7ce64c1e1a436613c023{ext}") != touched_info

    # Warn if file is deleted
    dirfs.rm(f"71b1251a1f7f7ce64c1e1a436613c023{ext}", recursive=True)
    with config.set(xarray_cache_type=xarray_cache_type), pytest.warns(
        UserWarning, match="No such file or directory"
    ):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert dirfs.exists(f"71b1251a1f7f7ce64c1e1a436613c023{ext}")
