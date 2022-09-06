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
import mimetypes
from typing import Any, Dict, Union

import fsspec
import fsspec.implementations.local

from . import cache, config, encode

try:
    import dask
    import xarray as xr

    HAS_XARRAY_AND_DASK = True
except ImportError:
    HAS_XARRAY_AND_DASK = False

try:
    import magic

    HAS_MAGIC = True
except ImportError:
    HAS_MAGIC = False

for ext in (".grib", ".grb", ".grb1", ".grb2"):
    if ext not in mimetypes.types_map:
        mimetypes.add_type("application/x-grib", ext, strict=False)


def tokenize_xr_object(obj: Union["xr.DataArray", "xr.Dataset"]) -> str:
    with dask.config.set({"tokenize.ensure-deterministic": True}):
        return str(dask.base.tokenize(obj))  # type: ignore[no-untyped-call]


def dictify_xr_dataset(
    obj: Union["xr.DataArray", "xr.Dataset"],
) -> Dict[str, Any]:
    token = tokenize_xr_object(obj)
    checksum = cache.hexdigestify(token)
    xr_json = encode.dictify_xarray_asset(checksum=checksum, size=obj.nbytes)
    try:
        xr.open_dataset(xr_json["file:local_path"], chunks="auto")
    except:  # noqa: E722
        if xr_json["type"] == "application/x-netcdf":
            obj.to_netcdf(xr_json["file:local_path"])
        elif xr_json["type"] == "application/x-grib":
            import cfgrib.xarray_to_grib

            cfgrib.xarray_to_grib.to_grib(obj, xr_json["file:local_path"])
        else:
            # Should never get here! xarray_cache_type is checked in config.py
            raise ValueError(f"type {xr_json['type']} is NOT supported.")
    return xr_json


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
    obj: Union[io.TextIOWrapper, io.BufferedReader],
    delete_original: bool = False,
) -> Dict[str, Any]:

    if "w" in obj.mode:
        raise ValueError("write-mode objects can NOT be cached.")

    filetype = mimetypes.guess_type(obj.name)[0]
    if filetype is None and HAS_MAGIC:
        with fsspec.open(obj.name, "rb") as f:
            filetype = magic.from_buffer(f.read(), mime=True)
    if filetype is None or filetype == "application/octet-stream":
        filetype = "unknown"

    with fsspec.open(obj.name, "rb") as f:
        checksum = hexdigestify_file(f)

    with fsspec.open(obj.name, "rb") as f:
        size = f.size

    extension = f".{obj.name.rsplit('.', 1)[-1]}" if "." in obj.name else ""

    params = inspect.signature(open).parameters
    open_kwargs = {k: getattr(obj, k) for k in params.keys() if hasattr(obj, k)}
    io_json = encode.dictify_io_asset(
        filetype=filetype,
        checksum=checksum,
        size=size,
        extension=extension,
        open_kwargs=open_kwargs,
    )

    cache_dir_fs = config.get_cache_files_directory()
    local_path = io_json.get("file:local_path", None)
    basename = io_json["href"].rsplit("/", 1)[-1]
    try:
        cache_dir_fs.open(
            basename, **io_json["tmp:open_kwargs"], **io_json["tmp:storage_options"]
        )
    except:  # noqa: E722
        if local_path is not None and delete_original:
            fsspec.filesystem("file").move(obj.name, local_path)
        else:
            cache_dir_fs.put_file(obj.name, basename)
            if delete_original:
                fsspec.filesystem("file").rm(obj.name)

    return io_json


def register_all() -> None:
    encode.FILECACHE_ENCODERS.append((io.TextIOWrapper, dictify_io_object))
    encode.FILECACHE_ENCODERS.append((io.BufferedReader, dictify_io_object))
    if HAS_XARRAY_AND_DASK:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
