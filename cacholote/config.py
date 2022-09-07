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
import fsspec.implementations.local

_SETTINGS: Dict[str, Any] = {
    "cache_store_directory": os.path.join(tempfile.gettempdir(), "cacholote"),
    "cache_files_urlpath": None,
    "cache_files_storage_options": {},
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

        if "cache_store" in kwargs and "cache_store_directory" in kwargs:
            raise ValueError(
                "'cache_store' and 'cache_store_directory' are mutually exclusive"
            )

        if "cache_files_directory" in kwargs and not isinstance(
            kwargs["cache_files_directory"],
            fsspec.implementations.local.LocalFileSystem,
        ):
            raise ValueError(
                "'cache_files_directory' must be of type 'LocalFileSystem'"
            )

        if "cache_store" in kwargs:
            kwargs["cache_store_directory"] = None

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


def get_cache_files_directory() -> fsspec.implementations.dirfs.DirFileSystem:

    if _SETTINGS["cache_files_urlpath"] is _SETTINGS["cache_store_directory"] is None:
        raise ValueError("Please set 'cache_files_directory'")

    if _SETTINGS["cache_files_urlpath"] is None:
        urlpath = _SETTINGS["cache_store_directory"]
    else:
        urlpath = _SETTINGS["cache_files_urlpath"]
    urlpath = str(urlpath)

    protocol = fsspec.utils.get_protocol(urlpath)
    fs = fsspec.filesystem(protocol, **_SETTINGS["cache_files_storage_options"])
    return fsspec.implementations.dirfs.DirFileSystem(path=urlpath, fs=fs)
