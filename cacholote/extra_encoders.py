"""Additional encoders that need optional dependencies."""
# Copyright 2019, B-Open Solutions srl.
# Copyright 2022, European Union.
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


import functools
import inspect
import io
import mimetypes
import os
import posixpath
import tempfile
from typing import Any, Callable, Dict, Tuple, TypeVar, Union, cast

import fsspec
import fsspec.implementations.local

from . import config, decode, encode, utils

try:
    import dask
    import xarray as xr

    _HAS_XARRAY_AND_DASK = True
except ImportError:
    _HAS_XARRAY_AND_DASK = False

try:
    import magic

    _HAS_MAGIC = True
except ImportError:
    _HAS_MAGIC = False

F = TypeVar("F", bound=Callable[..., Any])

_UNION_IO_TYPES = Union[
    io.BufferedReader,
    io.TextIOWrapper,
    fsspec.spec.AbstractBufferedFile,
    fsspec.implementations.local.LocalFileOpener,
]

# Add netcdf, grib, and zarr to mimetypes
mimetypes.add_type("application/netcdf", ".nc", strict=True)
for ext in (".grib", ".grb", ".grb1", ".grb2"):
    mimetypes.add_type("application/x-grib", ext, strict=False)
mimetypes.add_type("application/vnd+zarr", ".zarr", strict=False)


def _requires_xarray_and_dask(func: F) -> F:
    """Raise an error if `xarray` or `dask` are not installed."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _HAS_XARRAY_AND_DASK:
            raise ValueError("please install 'xarray' and 'dask'")
        return func(*args, **kwargs)

    return cast(F, wrapper)


def _dictify_file(fs: fsspec.AbstractFileSystem, local_path: str) -> Dict[str, Any]:

    filetype = mimetypes.guess_type(local_path, strict=False)[0]
    if filetype is None and _HAS_MAGIC:
        with fs.open(local_path, "rb") as f:
            filetype = magic.from_buffer(f.read(), mime=True)
            if filetype == "application/octet-stream":
                filetype = None
    filetype = filetype or "unknown"

    file_dict = {
        "type": filetype,
        "href": posixpath.join(
            utils.get_cache_files_directory_readonly(), posixpath.basename(local_path)
        ),
        "file:checksum": fs.checksum(local_path),
        "file:size": fs.size(local_path),
        "file:local_path": local_path,
    }

    return file_dict


def _get_fs_and_urlpath_to_decode(
    cache_dict: Dict[str, Any]
) -> Tuple[fsspec.AbstractFileSystem, str]:
    urlpath = cache_dict["file:local_path"]
    for k, v in cache_dict.items():
        if k.endswith(":storage_options"):
            storage_options = v
            break
    else:
        storage_options = {}

    # Attempt to read from local_path
    try:
        fs, _, _ = fsspec.get_fs_token_paths(urlpath, storage_options=storage_options)
    except:  # noqa: E722
        pass
    else:
        if fs.exists(urlpath):
            if fs.checksum(urlpath) == cache_dict["file:checksum"]:
                return (fs, urlpath)
            # Delete corrupted files
            recursive = cache_dict.get("type") == "application/vnd+zarr"
            fs.rm(cache_dict["file:local_path"], recursive=recursive)
            raise ValueError("checksum mismatch")

    # Attempt to read from href
    urlpath = cache_dict["href"]
    fs, _, _ = fsspec.get_fs_token_paths(urlpath)
    if fs.exists(urlpath):
        return (fs, urlpath)

    # Nothing worked
    raise ValueError(
        f"No such file or directory: {cache_dict['file:local_path']!r} nor {cache_dict['href']!r}"
    )


@_requires_xarray_and_dask
def decode_xr_dataset(obj: Dict[str, Any]) -> "xr.Dataset":
    if not {"xarray:open_kwargs", "xarray:storage_options"} <= set(obj):
        raise decode.DecodeError

    fs, urlpath = _get_fs_and_urlpath_to_decode(obj)

    if obj["type"] == "application/vnd+zarr":
        filename_or_obj = fs.get_mapper(urlpath)
    else:
        if fsspec.utils.get_protocol(urlpath) == "file":
            filename_or_obj = urlpath
        else:
            # Download local copy
            protocol = fsspec.utils.get_protocol(urlpath)
            with fsspec.open(
                f"filecache::{urlpath}",
                filecache={"same_names": True},
                **{protocol: fs.storage_options},
            ) as of:
                filename_or_obj = of.name
    return xr.open_dataset(filename_or_obj, **obj["xarray:open_kwargs"])


def decode_io_object(obj: Dict[str, Any]) -> _UNION_IO_TYPES:
    if {"tmp:open_kwargs", "tmp:storage_options"} <= set(obj):
        fs, urlpath = _get_fs_and_urlpath_to_decode(obj)
        return fs.open(urlpath)
    raise decode.DecodeError


@_requires_xarray_and_dask
def _store_xr_dataset(
    obj: "xr.Dataset", fs: fsspec.AbstractFileSystem, urlpath: str, filetype: str
) -> None:
    if filetype == "application/vnd+zarr":
        # Write directly on any filesystem
        mapper = fs.get_mapper(urlpath)
        obj.to_zarr(mapper, consolidated=True)
    else:
        # Need a tmp local copy to write on a different filesystem
        with tempfile.TemporaryDirectory() as tmpdirname:
            basename = os.path.basename(urlpath)
            tmpfilename = os.path.join(tmpdirname, basename)

            if filetype == "application/netcdf":
                obj.to_netcdf(tmpfilename)
            elif filetype == "application/x-grib":
                import cfgrib.xarray_to_grib

                cfgrib.xarray_to_grib.to_grib(obj, tmpfilename)
            else:
                # Should never get here! xarray_cache_type is checked in config.py
                raise ValueError(f"type {filetype!r} is NOT supported.")

            if fs == fsspec.filesystem("file"):
                fsspec.filesystem("file").mv(tmpfilename, urlpath)
            else:
                with fsspec.open(tmpfilename, "rb") as f_in, fs.open(
                    urlpath, "wb"
                ) as f_out:
                    utils.copy_buffered_file(f_in, f_out)


@_requires_xarray_and_dask
def dictify_xr_dataset(obj: "xr.Dataset") -> Dict[str, Any]:
    """Encode a ``xr.Dataset`` to JSON deserialized data (``dict``)."""
    with dask.config.set({"tokenize.ensure-deterministic": True}):
        root = dask.base.tokenize(obj)  # type: ignore[no-untyped-call]

    filetype = config.SETTINGS["xarray_cache_type"]
    ext = mimetypes.guess_extension(filetype, strict=False)
    urlpath_out = posixpath.join(utils.get_cache_files_directory(), f"{root}{ext}")

    fs_out, _, _ = fsspec.get_fs_token_paths(
        urlpath_out, storage_options=config.SETTINGS["cache_files_storage_options"]
    )
    if not fs_out.exists(urlpath_out):
        _store_xr_dataset(obj, fs_out, urlpath_out, filetype)

    xr_dict = _dictify_file(fs_out, urlpath_out)
    xr_dict["xarray:storage_options"] = config.SETTINGS["cache_files_storage_options"]
    if filetype == "application/vnd+zarr":
        xr_dict["xarray:open_kwargs"] = {
            "engine": "zarr",
            "consolidated": True,
            "chunks": "auto",
        }
    else:
        xr_dict["xarray:open_kwargs"] = {"chunks": "auto"}

    return xr_dict


def _store_io_object(
    fs_in: fsspec.AbstractFileSystem,
    urlpath_in: str,
    fs_out: fsspec.AbstractFileSystem,
    urlpath_out: str,
) -> None:
    if fs_in == fs_out:
        if config.SETTINGS["io_delete_original"]:
            fs_in.mv(urlpath_in, urlpath_out)
        else:
            fs_in.cp(urlpath_in, urlpath_out)
    else:
        with fs_in.open(urlpath_in, "rb") as f_in, fs_out.open(
            urlpath_out, "wb"
        ) as f_out:
            utils.copy_buffered_file(f_in, f_out)
        if config.SETTINGS["io_delete_original"]:
            fs_in.rm(urlpath_in)


def dictify_io_object(obj: _UNION_IO_TYPES) -> Dict[str, Any]:
    """Encode a file object to JSON deserialized data (``dict``)."""
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
    urlpath_out = posixpath.join(utils.get_cache_files_directory(), f"{root}{ext}")

    fs_out, _, _ = fsspec.get_fs_token_paths(
        urlpath_out, storage_options=config.SETTINGS["cache_files_storage_options"]
    )
    if not fs_out.exists(urlpath_out):
        _store_io_object(fs_in, urlpath_in, fs_out, urlpath_out)

    io_json = _dictify_file(fs_out, urlpath_out)
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
    """Register extra encoders if optional dependencies are installed."""
    for type_ in (
        io.BufferedReader,
        io.TextIOWrapper,
        fsspec.spec.AbstractBufferedFile,
        fsspec.implementations.local.LocalFileOpener,
    ):
        encode.FILECACHE_ENCODERS.append((type_, dictify_io_object))
    decode.FILECACHE_DECODERS.append(decode_io_object)
    if _HAS_XARRAY_AND_DASK:
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
        decode.FILECACHE_DECODERS.append(decode_xr_dataset)
