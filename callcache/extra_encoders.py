import pathlib
from typing import TYPE_CHECKING, Any, Dict, Union

try:
    import netCDF4  # noqa
    import xarray as xr
except ImportError:  # pragma: no cover
    if TYPE_CHECKING:
        import xarray as xr

try:
    import s3fs
    import zarr  # noqa

    s3 = s3fs.S3FileSystem()
except ImportError:  # pragma: no cover
    pass


from . import cache, encode


def open_zarr(s3_path: str, *args: Any, **kwargs: Any) -> "xr.Dataset":
    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
    return xr.open_zarr(store=store, *args, **kwargs)  # type: ignore


def dictify_xr_dataset_s3(
    o: "xr.Dataset",
    filecache_root: str = "s3://callcache",
    file_name_template: str = "{uuid}.zarr",
    **kwargs: Any,
) -> Dict[str, Any]:
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))  # type: ignore[no-untyped-call]
    file_name = file_name_template.format(**locals())
    s3_path = f"{filecache_root}/{file_name}"
    print(s3_path)
    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
    try:
        orig = xr.open_zarr(store=store, consolidated=True)  # type: ignore[no-untyped-call]
    except:
        orig = None
        o.to_zarr(store=store, consolidated=True)
    if orig is not None and not o.identical(orig):
        raise RuntimeError(f"inconsistent array in file {s3_path}")
    return encode.dictify_python_call(open_zarr, s3_path)


def dictify_xr_dataset(
    o: Union["xr.DataArray", "xr.Dataset"],
    filecache_root: str = ".",
    file_name_template: str = "{uuid}.nc",
    **kwargs: Any,
) -> Dict[str, Any]:
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))  # type: ignore[no-untyped-call]
    file_name = file_name_template.format(**locals())
    path = str(pathlib.Path(filecache_root).absolute() / file_name)
    try:
        orig = xr.open_dataset(path)  # type: ignore
    except:
        orig = None
        o.to_netcdf(path)
    if orig is not None and not o.identical(orig):
        raise RuntimeError(f"inconsistent array in file {path}")
    return encode.dictify_python_call(xr.open_dataset, path)


def register_all() -> None:
    try:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
    except NameError:
        pass
