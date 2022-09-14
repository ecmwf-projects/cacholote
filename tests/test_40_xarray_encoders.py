import fsspec
import pytest

from cacholote import cache, config, extra_encoders

try:
    import xarray as xr
except ImportError:
    pytest.importorskip("xarray")
pytest.importorskip("dask")


def get_grib_ds() -> "xr.Dataset":
    url = "https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib"
    with fsspec.open(f"simplecache::{url}", simplecache={"same_names": True}) as of:
        fname = of.name
    ds = xr.open_dataset(fname, engine="cfgrib")
    del ds.attrs["history"]
    return ds.sel(number=0)


def test_dictify_xr_dataset(tmpdir: str) -> None:
    pytest.importorskip("netCDF4")

    ds = xr.Dataset({"foo": [0]}, attrs={})
    actual = extra_encoders.dictify_xr_dataset(ds)
    checksum = fsspec.filesystem("file").checksum(
        f"{tmpdir}/247fd17e087ae491996519c097e70e48.nc"
    )
    expected = {
        "type": "application/x-netcdf",
        "href": f"file://{tmpdir}/247fd17e087ae491996519c097e70e48.nc",
        "file:checksum": checksum,
        "file:size": 669,
        "file:local_path": f"{tmpdir}/247fd17e087ae491996519c097e70e48.nc",
        "xarray:storage_options": {},
        "xarray:open_kwargs": {"chunks": "auto"},
    }
    assert actual == expected


@pytest.mark.parametrize(
    "xarray_cache_type,ext,identical,check_source,importorskip",
    [
        ("application/x-netcdf", ".nc", True, True, "netCDF4"),
        ("application/x-grib", ".grib", False, True, "cfgrib"),
        ("application/vnd+zarr", ".zarr", True, False, "zarr"),
    ],
)
def test_xr_cacheable(
    tmpdir: str,
    xarray_cache_type: str,
    ext: str,
    identical: bool,
    check_source: bool,
    importorskip: str,
) -> None:
    pytest.importorskip(importorskip)

    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)

    infos = []
    for expected_stats in ((0, 1), (1, 1)):
        with config.set(xarray_cache_type=xarray_cache_type):
            actual = cfunc()

        # Check hit & miss
        assert config.SETTINGS["cache_store"].stats() == expected_stats

        # Check result
        if identical:
            xr.testing.assert_identical(actual, expected)
        else:
            xr.testing.assert_equal(actual, expected)

        # Check source file
        if check_source:
            assert (
                actual.encoding["source"]
                == f"{tmpdir}/06810be7ce1f5507be9180bfb9ff14fd{ext}"
            )

        # Check opened with dask
        assert dict(actual.chunks) == {
            "time": (4,),
            "isobaricInhPa": (2,),
            "latitude": (61,),
            "longitude": (120,),
        }

        infos.append(
            fsspec.filesystem("file").info(
                f"{tmpdir}/06810be7ce1f5507be9180bfb9ff14fd{ext}"
            )
        )

    # Check cached file is not modified
    assert infos[0] == infos[1]
