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
import posixpath
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
    if buffer_size is None:
        buffer_size = io.DEFAULT_BUFFER_SIZE
    while True:
        data = f_in.read(buffer_size)
        if not data:
            break
        f_out.write(data)


def get_filesystem(
    urlpath: str, storage_options: Optional[Dict[str, Any]] = None
) -> fsspec.AbstractFileSystem:
    if storage_options is None:
        storage_options = config.SETTINGS["cache_files_storage_options"]
    protocol = fsspec.utils.get_protocol(urlpath)
    return fsspec.filesystem(protocol, **storage_options)


def dictify_file(urlpath: str) -> Dict[str, Any]:
    fs = get_filesystem(urlpath)

    # Add grib and zarr to mimetypes
    for ext in (".grib", ".grb", ".grb1", ".grb2"):
        if ext not in mimetypes.types_map:
            mimetypes.add_type("application/x-grib", ext, strict=False)
    if ".zarr" not in mimetypes.types_map:
        mimetypes.add_type("application/vnd+zarr", ".zarr", strict=False)

    filetype = mimetypes.guess_type(urlpath, strict=False)[0]
    if filetype is None and HAS_MAGIC:
        with fs.open(urlpath, "rb") as f:
            filetype = magic.from_buffer(f.read(), mime=True)
            if filetype == "application/octet-stream":
                filetype = None
    filetype = filetype or "unknown"

    file_dict = {
        "type": filetype,
        "href": fs.unstrip_protocol(urlpath),
        "file:checksum": fs.checksum(urlpath),
        "file:size": fs.size(urlpath),
        "file:local_path": fs._strip_protocol(urlpath),
    }

    return file_dict


def decode_xr_dataset(xr_json: Dict[str, Any]) -> "xr.Dataset":
    if xr_json["type"] == "application/vnd+zarr":
        fs = get_filesystem(xr_json["href"], xr_json["xarray:storage_options"])
        filename_or_obj = fs.get_mapper(xr_json["href"])
    else:
        if fsspec.utils.get_protocol(xr_json["href"]) == "file":
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

    filetype = config.SETTINGS["xarray_cache_type"]
    ext = config.EXTENSIONS[filetype]
    urlpath_out = posixpath.join(config.get_cache_files_directory(), f"{root}{ext}")

    fs_out = get_filesystem(urlpath_out)
    if not fs_out.exists(urlpath_out):
        if filetype == "application/vnd+zarr":
            # Write directly on any filesystem
            mapper = fs_out.get_mapper(urlpath_out)
            obj.to_zarr(mapper, consolidated=True)
        else:
            # Need a tmp local copy to write on a different filesystem
            with tempfile.TemporaryDirectory() as tmpdirname:
                basename = os.path.basename(urlpath_out)
                tmpfilename = os.path.join(tmpdirname, basename)

                if filetype == "application/x-netcdf":
                    obj.to_netcdf(tmpfilename)
                elif filetype == "application/x-grib":
                    import cfgrib.xarray_to_grib

                    cfgrib.xarray_to_grib.to_grib(obj, tmpfilename)
                else:
                    # Should never get here! xarray_cache_type is checked in config.py
                    raise ValueError(f"type {filetype!r} is NOT supported.")

                if fs_out == fsspec.filesystem("file"):
                    fsspec.filesystem("file").mv(tmpfilename, urlpath_out)
                else:
                    with fsspec.open(tmpfilename, "rb") as f_in, fs_out.open(
                        urlpath_out, "wb"
                    ) as f_out:
                        copy_buffer(f_in, f_out)

    xr_json = dictify_file(urlpath_out)
    xr_json["xarray:storage_options"] = config.SETTINGS["cache_files_storage_options"]
    if filetype == "application/vnd+zarr":
        xr_json["xarray:open_kwargs"] = {
            "engine": "zarr",
            "consolidated": True,
            "chunks": "auto",
        }
    else:
        xr_json["xarray:open_kwargs"] = {"chunks": "auto"}

    return xr_json


def dictify_io_object(
    obj: UNION_IO_TYPES,
) -> Dict[str, Any]:
    if "w" in obj.mode:
        raise ValueError("write-mode objects can NOT be cached.")

    if isinstance(obj, (io.BufferedReader, io.TextIOWrapper)):
        urlpath_in = obj.name
        fs_in = fsspec.filesystem("file")
    else:
        urlpath_in = obj.path
        fs_in = obj.fs

    root = fs_in.checksum(urlpath_in)
    _, ext = os.path.splitext(urlpath_in)
    urlpath_out = posixpath.join(config.get_cache_files_directory(), f"{root}{ext}")

    fs_out = get_filesystem(urlpath_out)
    if not fs_out.exists(urlpath_out):
        if fs_in == fs_out:
            if config.SETTINGS["io_delete_original"]:
                fs_in.mv(urlpath_in, urlpath_out)
            else:
                fs_in.cp(urlpath_in, urlpath_out)
        else:
            with fs_in.open(urlpath_in, "rb") as f_in, fs_out.open(
                urlpath_out, "wb"
            ) as f_out:
                copy_buffer(f_in, f_out)
            if config.SETTINGS["io_delete_original"]:
                fs_in.rm(urlpath_in)

    io_json = dictify_file(urlpath_out)
    params = inspect.signature(open).parameters
    open_kwargs = {k: getattr(obj, k) for k in params.keys() if hasattr(obj, k)}
    io_json.update(
        {
            "tmp:storage_options": config.SETTINGS["cache_files_storage_options"],
            "tmp:open_kwargs": open_kwargs,
        }
    )

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
