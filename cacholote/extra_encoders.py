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
import shutil
from typing import Any, Dict, Union

from . import cache, encode

try:
    import xarray as xr
except ImportError:
    pass


def tokenize_xr_object(obj: Union["xr.DataArray", "xr.Dataset"]) -> str:
    import dask

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
        if xr_json["type"] == "application/netcdf":
            obj.to_netcdf(xr_json["file:local_path"])
        elif xr_json["type"] == "application/wmo-GRIB2":
            import cfgrib.xarray_to_grib

            cfgrib.xarray_to_grib.to_grib(
                obj, xr_json["file:local_path"], grib_keys={"edition": 2}
            )
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
    obj: Union[io.TextIOWrapper, io.BufferedReader]
) -> Dict[str, Any]:
    import magic

    if "w" in obj.mode:
        raise ValueError("write-mode objects can NOT be cached.")

    filetype = magic.from_file(obj.name, mime=True)

    if obj.closed:
        with open(obj.name, "rb") as f:
            checksum = hexdigestify_file(f)
    else:
        checksum = hexdigestify_file(obj)

    size = os.path.getsize(obj.name)

    _, extension = os.path.splitext(obj.name)

    params = inspect.signature(open).parameters
    open_kwargs = {k: getattr(obj, k) for k in params.keys() if hasattr(obj, k)}

    io_json = encode.dictify_io_asset(
        filetype=filetype,
        checksum=checksum,
        size=size,
        extension=extension,
        open_kwargs=open_kwargs,
    )

    try:
        open(io_json["file:local_path"], **open_kwargs)
    except:  # noqa: E722
        shutil.copyfile(obj.name, io_json["file:local_path"])

    return io_json


def register_all() -> None:
    encode.FILECACHE_ENCODERS.append((io.TextIOWrapper, dictify_io_object))
    encode.FILECACHE_ENCODERS.append((io.BufferedReader, dictify_io_object))
    try:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
    except NameError:
        pass
