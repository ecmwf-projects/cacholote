import pathlib

try:
    import netCDF4  # noqa
    import xarray as xr
except ImportError:  # pragma: no cover
    pass

try:
    import zarr  # noqa
    import s3fs

    s3 = s3fs.S3FileSystem()
except ImportError:  # pragma: no cover
    pass


from . import cache
from . import encode


def open_zarr(s3_path, *args, **kwargs):
    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
    return xr.open_zarr(store=store, *args, **kwargs)


def dictify_xr_dataset_s3(
    o, cache_root="s3://callcache", file_name_template="{uuid}.zarr", **kwargs
):
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))
    file_name = file_name_template.format(**locals())
    s3_path = f"{cache_root}/{file_name}"
    print(s3_path)
    store = s3fs.S3Map(root=s3_path, s3=s3, check=False)
    try:
        orig = xr.open_zarr(store=store, consolidated=True)
    except:
        orig = None
        o.to_zarr(store=store, consolidated=True)
    if orig is not None and not o.identical(orig):
        raise RuntimeError(f"inconsistent array in file {s3_path}")
    return encode.dictify_python_call(open_zarr, s3_path)


def dictify_xr_dataset(o, cache_root=".", file_name_template="{uuid}.nc", **kwargs):
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))
    file_name = file_name_template.format(**locals())
    path = str(pathlib.Path(cache_root).absolute() / file_name)
    try:
        orig = xr.open_dataset(path)
    except:
        orig = None
        o.to_netcdf(path)
    if orig is not None and not o.identical(orig):
        raise RuntimeError(f"inconsistent array in file {path}")
    return encode.dictify_python_call(xr.open_dataset, path)


def register_all():
    try:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
    except NameError:
        pass
