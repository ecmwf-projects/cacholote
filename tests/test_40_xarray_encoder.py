import pathlib

import fsspec
import pytest
import structlog

from cacholote import cache, config, decode, encode, extra_encoders, utils

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
                "file:checksum": f"{fsspec.filesystem('file').checksum(local_path):x}",
                "file:size": 669,
                "file:local_path": local_path,
            },
            {},
        ),
        "kwargs": {"chunks": {}},
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
    con = config.get().engine.raw_connection()
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
    import dask

    config.set(xarray_cache_type=xarray_cache_type)

    # Cache file
    fs, dirname = utils.get_cache_files_fs_dirname()
    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)
    cfunc()

    # Get cached file path
    with dask.config.set({"tokenize.ensure-deterministic": True}):
        root = dask.base.tokenize(expected)  # type: ignore[no-untyped-call]
    cached_path = f"{dirname}/{root}{ext}"
    assert fs.exists(cached_path)

    # Warn if file is corrupted
    fs.touch(cached_path, truncate=False)
    touched_info = fs.info(cached_path)
    with pytest.warns(UserWarning, match="checksum mismatch"):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert fs.info(cached_path) != touched_info

    # Warn if file is deleted
    fs.rm(cached_path, recursive=True)
    with pytest.warns(UserWarning, match="No such file or directory"):
        actual = cfunc()
    xr.testing.assert_identical(actual, expected)
    assert fs.exists(cached_path)


def test_xr_logging(capsys: pytest.CaptureFixture[str]) -> None:
    config.set(logger=structlog.get_logger())

    # Cache dataset
    cfunc = cache.cacheable(get_grib_ds)
    cached_ds = cfunc()
    captured = iter(capsys.readouterr().out.splitlines())

    line = next(captured)
    assert "start write tmp file" in line
    assert "urlpath=" in line

    line = next(captured)
    assert "end write tmp file" in line
    assert "urlpath=" in line
    assert "write_tmp_file_time=" in line

    line = next(captured)
    assert "start upload" in line
    assert f"urlpath=file://{cached_ds.encoding['source']}" in line
    assert "size=22597" in line

    line = next(captured)
    assert "end upload" in line
    assert f"urlpath=file://{cached_ds.encoding['source']}" in line
    assert "upload_time=" in line
    assert "size=22597" in line

    line = next(captured)
    assert "retrieve cache file" in line
    assert f"urlpath=file://{cached_ds.encoding['source']}" in line
