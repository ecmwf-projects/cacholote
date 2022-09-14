import tempfile
from typing import Any, Dict

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
    "xarray_cache_type,ext,importorskip",
    [
        ("application/x-netcdf", ".nc", "netCDF4"),
        ("application/x-grib", ".grib", "cfgrib"),
        ("application/vnd+zarr", ".zarr", "zarr"),
    ],
)
@pytest.mark.parametrize("ftp_config_settings", [False, True], indirect=True)
def test_xr_cacheable(
    tmpdir: str,
    xarray_cache_type: str,
    ext: str,
    importorskip: str,
    ftp_config_settings: Dict[str, Any],
) -> None:
    pytest.importorskip(importorskip)

    if xarray_cache_type == "application/vnd+zarr" and ftp_config_settings:
        pytest.xfail(
            "fsspec mapper does not play well with pyftpdlib: 550 No such file or directory."
        )

    expected = get_grib_ds()
    cfunc = cache.cacheable(get_grib_ds)

    infos = []
    for expected_stats in ((0, 1), (1, 1)):
        with config.set(xarray_cache_type=xarray_cache_type, **ftp_config_settings):
            dirfs = config.get_cache_files_dirfs()
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
            if ftp_config_settings:
                # read from tmp local file
                assert actual.encoding["source"].startswith(tempfile.gettempdir())
                assert actual.encoding["source"].endswith(
                    f"/06810be7ce1f5507be9180bfb9ff14fd{ext}"
                )
            else:
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

    # Check cached file is not modified
    assert infos[0] == infos[1]

    if not ftp_config_settings:
        # Warn but don't fail if file is corrupted
        dirfs.touch(f"06810be7ce1f5507be9180bfb9ff14fd{ext}", truncate=False)
        touched_checksum = dirfs.checksum(f"06810be7ce1f5507be9180bfb9ff14fd{ext}")
        with config.set(xarray_cache_type=xarray_cache_type):
            with pytest.warns(UserWarning, match="checksum mismatch"):
                actual = cfunc()
                if xarray_cache_type == "application/x-grib":
                    xr.testing.assert_equal(actual, expected)
                else:
                    xr.testing.assert_identical(actual, expected)

                assert (
                    dirfs.checksum(f"06810be7ce1f5507be9180bfb9ff14fd{ext}")
                    != touched_checksum
                )

        # Warn but don't fail if file is deleted
        dirfs.rm(f"06810be7ce1f5507be9180bfb9ff14fd{ext}", recursive=True)
        with config.set(xarray_cache_type=xarray_cache_type):
            with pytest.warns(UserWarning, match="No such file or directory"):
                actual = cfunc()
                if xarray_cache_type == "application/x-grib":
                    xr.testing.assert_equal(actual, expected)
                else:
                    xr.testing.assert_identical(actual, expected)

                assert dirfs.exists(f"06810be7ce1f5507be9180bfb9ff14fd{ext}")
