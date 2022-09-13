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


import inspect
import io
import mimetypes
import os
import tempfile
from typing import Any, Dict, Optional, Union

import fsspec
import fsspec.implementations.arrow
import fsspec.implementations.local

from . import config, encode

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
if ".zarr" not in mimetypes.types_map:
    mimetypes.add_type("application/vnd+zarr", ".zarr", strict=False)

UNION_IO_TYPES = Union[
    io.BufferedReader,
    io.TextIOWrapper,
    fsspec.spec.AbstractBufferedFile,
    fsspec.implementations.arrow.ArrowFile,
    fsspec.implementations.local.LocalFileOpener,
]


def copy_buffer(
    f_in: fsspec.spec.AbstractBufferedFile,
    f_out: fsspec.spec.AbstractBufferedFile,
    buffer_size: Optional[int] = None,
) -> None:
    buffer_size = io.DEFAULT_BUFFER_SIZE if buffer_size is None else buffer_size
    while True:
        data = f_in.read(buffer_size)
        if not data:
            break
        f_out.write(data)


def fs_from_json(xr_or_io_json: Dict[str, Any]) -> fsspec.spec.AbstractFileSystem:
    protocol = fsspec.utils.get_protocol(xr_or_io_json["href"])
    try:
        storage_options = xr_or_io_json["tmp:storage_options"]
    except KeyError:
        storage_options = xr_or_io_json["xarray:storage_options"]
    return fsspec.filesystem(protocol, **storage_options)


def decode_xr_dataset(xr_json: Dict[str, Any]) -> "xr.Dataset":
    if xr_json["type"] == "application/vnd+zarr":
        fs = fs_from_json(xr_json)
        filename_or_obj = fs.get_mapper(xr_json["href"])
    else:
        if "file:local_path":
            filename_or_obj = xr_json["file:local_path"]
        else:
            # Download local copy
            protocol = fsspec.utils.get_protocol(xr_json["href"])
            storage_options = {protocol: xr_json["xarray:storage_options"]}
            with fsspec.open(
                f"filecache::{xr_json['href']}",
                filecache={"same_names": True},
                **storage_options,
            ) as of:
                filename_or_obj = of.name
    return xr.open_dataset(filename_or_obj, **xr_json["xarray:open_kwargs"])


def dictify_xr_dataset(
    obj: "xr.Dataset",
) -> Dict[str, Any]:
    with dask.config.set({"tokenize.ensure-deterministic": True}):
        root = dask.base.tokenize(obj)  # type: ignore[no-untyped-call]
    xr_json = encode.dictify_xarray_asset(root=root)

    fs_out = fs_from_json(xr_json)
    if not fs_out.exists(xr_json["href"]):
        if xr_json["type"] == "application/vnd+zarr":
            # Write directly on any filesystem
            mapper = fsspec.get_mapper(
                xr_json["href"], **xr_json["xarray:storage_options"]
            )
            obj.to_zarr(mapper, consolidated=True)
        else:
            # Need a tmp local copy to write on a different filesystem
            with tempfile.TemporaryDirectory() as tmpdirname:
                basename = os.path.basename(xr_json["href"])
                tmpfilename = os.path.join(tmpdirname, basename)

                if xr_json["type"] == "application/x-netcdf":
                    obj.to_netcdf(tmpfilename)
                elif xr_json["type"] == "application/x-grib":
                    import cfgrib.xarray_to_grib

                    cfgrib.xarray_to_grib.to_grib(obj, tmpfilename)
                else:
                    # Should never get here! xarray_cache_type is checked in config.py
                    raise ValueError(f"type {xr_json['type']} is NOT supported.")

                if "file:local_path" in xr_json:
                    fsspec.filesystem("file").mv(
                        tmpfilename, xr_json["file:local_path"]
                    )
                else:
                    with fsspec.open(tmpfilename, "rb") as f_in, fs_out.open(
                        xr_json["href"], "wb"
                    ) as f_out:
                        copy_buffer(f_in, f_out)

    xr_json["file:checksum"] = fs_out.checksum(xr_json["href"])
    xr_json["file:size"] = fs_out.size(xr_json["href"])
    return xr_json


def dictify_io_object(
    obj: UNION_IO_TYPES,
) -> Dict[str, Any]:
    if "w" in obj.mode:
        raise ValueError("write-mode objects can NOT be cached.")

    if isinstance(obj, (io.BufferedReader, io.TextIOWrapper)):
        path_in = obj.name
        fs_in = fsspec.filesystem("file")
    else:
        path_in = obj.path
        fs_in = obj.fs

    filetype = mimetypes.guess_type(path_in, strict=False)[0]
    if filetype is None and HAS_MAGIC:
        with fs_in.open(path_in, "rb") as f:
            filetype = magic.from_buffer(f.read(), mime=True)
            if filetype == "application/octet-stream":
                filetype = None
    filetype = filetype or "unknown"

    params = inspect.signature(open).parameters
    open_kwargs = {k: getattr(obj, k) for k in params.keys() if hasattr(obj, k)}

    io_json = encode.dictify_io_asset(
        filetype=filetype,
        root=fs_in.checksum(path_in),
        size=fs_in.size(path_in),
        ext=os.path.splitext(path_in)[-1],
        open_kwargs=open_kwargs,
    )

    fs_out = fs_from_json(io_json)
    if not fs_out.exists(io_json["href"]):
        if fs_in == fs_out:
            if config.SETTINGS["io_delete_original"]:
                fs_in.mv(path_in, io_json["href"])
            else:
                fs_in.cp(path_in, io_json["href"])
        else:
            fs_out = fs_from_json(io_json)
            with fs_in.open(path_in, "rb") as f_in, fs_out.open(
                io_json["href"], "wb"
            ) as f_out:
                copy_buffer(f_in, f_out)

            if config.SETTINGS["io_delete_original"]:
                fs_in.rm(path_in)

    io_json["file:checksum"] = fs_out.checksum(io_json["href"])
    return io_json


def register_all() -> None:
    for type_ in (
        io.BufferedReader,
        io.TextIOWrapper,
        fsspec.spec.AbstractBufferedFile,
        fsspec.implementations.arrow.ArrowFile,
        fsspec.implementations.local.LocalFileOpener,
    ):
        encode.FILECACHE_ENCODERS.append((type_, dictify_io_object))
    if HAS_XARRAY_AND_DASK:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
