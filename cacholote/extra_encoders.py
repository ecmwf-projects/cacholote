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


import hashlib
import inspect
import io
import os
import pathlib
import shutil
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
    s3_path = f"{config.SETTINGS['directory']}/{file_name}"
    store = s3fs.S3Map(root=s3_path, s3=S3, check=False)
    try:
        xr.open_zarr(store=store)  # type: ignore[no-untyped-call]
    except:  # noqa: E722
        obj.to_zarr(store=store)
    return encode.dictify_python_call(open_zarr, s3_path)


def dictify_xr_dataset(
    obj: Union["xr.DataArray", "xr.Dataset"],
    file_name_template: str = "./{uuid}.nc",
) -> Dict[str, Any]:
    token = tokenize_xr_object(obj)
    uuid = cache.hexdigestify(token)
    href = file_name_template.format(**locals())
    local_path = str(pathlib.Path(config.SETTINGS["directory"]).absolute() / href)
    try:
        xr.open_dataset(local_path)
    except:  # noqa: E722
        obj.to_netcdf(local_path)
    return encode.dictify_xarray_asset(
        filetype="application/netcdf", checksum=uuid, size=obj.nbytes
    )


def hexdigestify_file(
    f: Union[io.TextIOWrapper, io.BufferedReader],
    buf_size: int = io.DEFAULT_BUFFER_SIZE,
) -> str:
    hash_req = hashlib.sha3_224()
    while True:
        data = f.read(buf_size)
        if not data:
            break
        hash_req.update(data.encode() if isinstance(data, str) else data)
    return hash_req.hexdigest()


def dictify_io_object(
    obj: Union[io.TextIOWrapper, io.BufferedReader]
) -> Dict[str, Any]:
    if obj.closed:
        with open(obj.name, "rb") as f:
            hexdigest = hexdigestify_file(f)
    else:
        hexdigest = hexdigestify_file(obj)
    _, ext = os.path.splitext(obj.name)
    path = str(
        pathlib.Path(config.SETTINGS["directory"]).absolute() / (hexdigest + ext)
    )

    if "w" in obj.mode:
        raise ValueError("write-mode objects can NOT be cached.")

    params = inspect.signature(open).parameters
    kwargs = {k: getattr(obj, k) for k in params.keys() if hasattr(obj, k)}

    try:
        open(path, **kwargs)
    except:  # noqa: E722
        shutil.copyfile(obj.name, path)

    return encode.dictify_python_call(open, path, **kwargs)


def register_all() -> None:
    encode.FILECACHE_ENCODERS.append((io.TextIOWrapper, dictify_io_object))
    encode.FILECACHE_ENCODERS.append((io.BufferedReader, dictify_io_object))
    try:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
    except NameError:
        pass
