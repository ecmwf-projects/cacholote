"""
Handle global settings.

SETTINGS can be imported elsewhere to use global settings.
"""

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

import os
import tempfile
from types import MappingProxyType, TracebackType
from typing import Any, Dict, Optional, Type

import diskcache
import fsspec
import fsspec.implementations.dirfs

EXTENSIONS = MappingProxyType(
    {
        "application/x-netcdf": ".nc",
        "application/x-grib": ".grib",
        "application/vnd+zarr": ".zarr",
    }
)
_SETTINGS: Dict[str, Any] = {
    "cache_store_directory": os.path.join(tempfile.gettempdir(), "cacholote"),
    "cache_files_urlpath": None,
    "cache_files_storage_options": {},
    "xarray_cache_type": list(EXTENSIONS)[0],
    "io_delete_original": False,
}


def _initialize_cache_store() -> None:
    _SETTINGS["cache_store"] = diskcache.Cache(
        _SETTINGS["cache_store_directory"], disk=diskcache.JSONDisk, statistics=1
    )


_initialize_cache_store()

# Immutable settings to be used by other modules
SETTINGS = MappingProxyType(_SETTINGS)


class set:
    # TODO: Add docstring
    def __init__(self, **kwargs: Any):

        if "cache_store" in kwargs:
            if "cache_store_directory" in kwargs:
                raise ValueError(
                    "'cache_store' and 'cache_store_directory' are mutually exclusive"
                )
            kwargs["cache_store_directory"] = None

        if (
            "xarray_cache_type" in kwargs
            and kwargs["xarray_cache_type"] not in EXTENSIONS
        ):
            raise ValueError(f"'xarray_cache_type' must be one of {list(EXTENSIONS)}")

        try:
            self._old = {key: _SETTINGS[key] for key in kwargs}
        except KeyError as ex:
            raise KeyError(
                f"Wrong settings. Available settings: {list(_SETTINGS)}"
            ) from ex

        _SETTINGS.update(kwargs)
        if kwargs.get("cache_store_directory", None) is not None:
            self._old["cache_store"] = _SETTINGS["cache_store"]
            _initialize_cache_store()

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _SETTINGS.update(self._old)


def get_cache_files_directory() -> str:
    if SETTINGS["cache_files_urlpath"] is SETTINGS["cache_store_directory"] is None:
        raise ValueError(
            "Please set 'cache_files_urlpath' and 'cache_files_storage_options'"
        )
    if SETTINGS["cache_files_urlpath"] is None:
        return str(SETTINGS["cache_store_directory"])
    return str(SETTINGS["cache_files_urlpath"])


def get_cache_files_dirfs() -> fsspec.implementations.dirfs.DirFileSystem:
    cache_files_directory = get_cache_files_directory()
    protocol = fsspec.utils.get_protocol(cache_files_directory)
    fs = fsspec.filesystem(protocol, **SETTINGS["cache_files_storage_options"])
    return fsspec.implementations.dirfs.DirFileSystem(cache_files_directory, fs)
