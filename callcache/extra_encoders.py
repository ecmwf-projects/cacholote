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

from . import cache, config, encode

try:
    import s3fs

    S3 = s3fs.S3FileSystem()
except ImportError:
    pass

try:
    import xarray as xr
except ImportError:
    pass

try:
    import dask
except ImportError:
    pass


def open_zarr(s3_path: str, *args: Any, **kwargs: Any) -> "xr.Dataset":
    store = s3fs.S3Map(root=s3_path, s3=S3, check=False)
    return xr.open_zarr(store=store, *args, **kwargs)  # type: ignore


def tokenize_xr_object(obj: Union["xr.DataArray", "xr.Dataset"]) -> str:
    with dask.config.set({"tokenize.ensure-deterministic": True}):
        return str(dask.base.tokenize(obj))  # type: ignore[no-untyped-call]


def dictify_xr_dataset_s3(
    obj: "xr.Dataset",
    file_name_template: str = "{uuid}.zarr",
) -> Dict[str, Any]:
    token = tokenize_xr_object(obj)
    uuid = cache.hexdigestify(token)
    file_name = file_name_template.format(**locals())
    s3_path = f"{config.SETTINGS['cache'].directory}/{file_name}"
    store = s3fs.S3Map(root=s3_path, s3=S3, check=False)
    try:
        orig = xr.open_zarr(store=store)  # type: ignore[no-untyped-call]
    except:  # noqa: E722
        orig = None
        obj.to_zarr(store=store)
    return encode.dictify_python_call(open_zarr, s3_path)


def dictify_xr_dataset(
    obj: Union["xr.DataArray", "xr.Dataset"],
    file_name_template: str = "{uuid}.nc",
) -> Dict[str, Any]:
    token = tokenize_xr_object(obj)
    uuid = cache.hexdigestify(token)
    file_name = file_name_template.format(**locals())
    path = str(pathlib.Path(config.SETTINGS["cache"].directory).absolute() / file_name)
    try:
        orig = xr.open_dataset(path)  # type: ignore[no-untyped-call]
    except:  # noqa: E722
        orig = None
        obj.to_netcdf(path)
    return encode.dictify_python_call(xr.open_dataset, path)


def register_all() -> None:
    try:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
    except NameError:
        pass
