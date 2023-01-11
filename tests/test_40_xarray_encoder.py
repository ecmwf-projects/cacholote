import pathlib

import fsspec
import pytest

from cacholote import cache, config, database, decode, encode, extra_encoders, utils

try:
    import xarray as xr
except ImportError:
    pytest.importorskip("xarray")


def get_grib_ds() -> "xr.Dataset":
    pytest.importorskip("cfgrib")
    eccodes = pytest.importorskip("eccodes")
    filename = pathlib.Path(eccodes.codes_samples_path()) / "GRIB2.tmpl"
    ds = xr.open_dataset(filename, engine="cfgrib")
    del ds.attrs["history"]
    return ds


@pytest.mark.filterwarnings(
    "ignore:distutils Version classes are deprecated. Use packaging.version instead."
)
def test_dictify_xr_dataset(tmpdir: pathlib.Path) -> None:
    pytest.importorskip("netCDF4")

    # Define readonly dir
    readonly_dir = str(tmpdir / "readonly")
    fsspec.filesystem("file").mkdir(readonly_dir)
    config.set(cache_files_urlpath_readonly=readonly_dir)

    # Create sample dataset
    ds = xr.Dataset({"foo": [0]}, attrs={})

    # Check dict
    actual = extra_encoders.dictify_xr_dataset(ds)
    href = f"{readonly_dir}/247fd17e087ae491996519c097e70e48.nc"
    local_path = f"{tmpdir}/cache_files/247fd17e087ae491996519c097e70e48.nc"
    expected = {
        "type": "python_call",
        "callable": "cacholote.extra_encoders:decode_xr_dataset",
        "args": (
            {
                "type": "application/netcdf",
                "href": href,
                "file:checksum": fsspec.filesystem("file").checksum(local_path),
                "file:size": 669,
                "file:local_path": local_path,
            },
            {},
        ),
        "kwargs": {"chunks": "auto"},
    }
    assert actual == expected

    # Use href when local_path is missing or corrupted
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
@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
@pytest.mark.filterwarnings(
    "ignore:GRIB write support is experimental, DO NOT RELY ON IT!"
)
@pytest.mark.filterwarnings(
    "ignore:distutils Version classes are deprecated. Use packaging.version instead."
)
def test_xr_cacheable(
    tmpdir: pathlib.Path,
    xarray_cache_type: str,
    ext: str,
    importorskip: str,
    set_cache: str,
) -> None:
    pytest.importorskip(importorskip)

    config.set(xarray_cache_type=xarray_cache_type)

    # cache-db to check
    con = database.ENGINE.get().raw_connection()
    cur = con.cursor()

    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)

    for expected_counter in (1, 2):
        actual = cfunc()

        # Check hits
        cur.execute("SELECT counter FROM cache_entries", ())
        assert cur.fetchall() == [(expected_counter,)]

        # Check result
        if xarray_cache_type == "application/x-grib":
            xr.testing.assert_equal(actual, expected)
        else:
            xr.testing.assert_identical(actual, expected)

        # Check opened with dask (i.e., read from file)
        assert dict(actual.chunks) == {"longitude": (16,), "latitude": (31,)}


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

    config.set(xarray_cache_type=xarray_cache_type)

    # Cache file
    fs, dirname = utils.get_cache_files_fs_dirname()
    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)
    cfunc()

    # Warn if file is corrupted
    fs.touch(f"{dirname}/b8094ae0691cfa42c9b839679e162e3d{ext}", truncate=False)
    touched_info = fs.info(f"{dirname}/b8094ae0691cfa42c9b839679e162e3d{ext}")
    with pytest.warns(UserWarning, match="checksum mismatch"):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert fs.info(f"{dirname}/b8094ae0691cfa42c9b839679e162e3d{ext}") != touched_info

    # Warn if file is deleted
    fs.rm(f"{dirname}/b8094ae0691cfa42c9b839679e162e3d{ext}", recursive=True)
    with pytest.warns(UserWarning, match="No such file or directory"):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert fs.exists(f"{dirname}/b8094ae0691cfa42c9b839679e162e3d{ext}")
