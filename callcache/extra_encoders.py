# Copyright 2019, B-Open Solutions srl.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pathlib
from typing import Any, Dict, Union

from . import cache, encode, settings

try:
    import s3fs

    S3 = s3fs.S3FileSystem()
except ImportError:
    pass

try:
    import xarray as xr
except ImportError:
    pass


def open_zarr(s3_path: str, *args: Any, **kwargs: Any) -> "xr.Dataset":
    store = s3fs.S3Map(root=s3_path, s3=S3, check=False)
    return xr.open_zarr(store=store, *args, **kwargs)  # type: ignore


def dictify_xr_dataset_s3(
    o: "xr.Dataset",
    file_name_template: str = "{uuid}.zarr",
    **kwargs: Any,
) -> Dict[str, Any]:
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))  # type: ignore[no-untyped-call]
    file_name = file_name_template.format(**locals())
    s3_path = f"{settings.SETTINGS['cache'].directory}/{file_name}"
    store = s3fs.S3Map(root=s3_path, s3=S3, check=False)
    try:
        orig = xr.open_zarr(store=store)  # type: ignore[no-untyped-call]
    except:  # noqa: E722
        orig = None
        o.to_zarr(store=store)
    if orig is not None and not o.identical(orig):
        raise RuntimeError(f"inconsistent array in file {s3_path}")
    return encode.dictify_python_call(open_zarr, s3_path)


def dictify_xr_dataset(
    o: Union["xr.DataArray", "xr.Dataset"],
    file_name_template: str = "{uuid}.nc",
    **kwargs: Any,
) -> Dict[str, Any]:
    # xarray >= 0.14.1 provide stable hashing
    uuid = cache.hexdigestify(str(o.__dask_tokenize__()))  # type: ignore[no-untyped-call]
    file_name = file_name_template.format(**locals())
    path = str(
        pathlib.Path(settings.SETTINGS["cache"].directory).absolute() / file_name
    )
    try:
        orig = xr.open_dataset(path)  # type: ignore
    except:  # noqa: E722
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
