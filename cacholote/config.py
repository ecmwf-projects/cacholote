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

import inspect
import os
import pickle
import tempfile
from types import MappingProxyType, TracebackType
from typing import Any, Dict, Optional, Type

import diskcache

_SETTINGS: Dict[str, Any] = {
    "directory": os.path.join(tempfile.gettempdir(), "cacholote"),  # cache directory
    "xarray_cache_type": "application/netcdf",
    "timeout": 60,  # SQLite connection timeout
    "statistics": 1,  # True
    "tag_index": 0,  # False
    "eviction_policy": "least-recently-stored",
    "size_limit": 2**30,  # 1gb
    "cull_limit": 10,
    "sqlite_auto_vacuum": 1,  # FULL
    "sqlite_cache_size": 2**13,  # 8,192 pages
    "sqlite_journal_mode": "wal",
    "sqlite_mmap_size": 2**26,  # 64mb
    "sqlite_synchronous": 1,  # NORMAL
    "disk_min_file_size": 2**15,  # 32kb
    "disk_pickle_protocol": pickle.HIGHEST_PROTOCOL,
}
EXTENSIONS = {"application/netcdf": ".nc", "application/wmo-GRIB2": ".grb2"}


def initialize_cache_store() -> diskcache.Cache:
    sig = inspect.signature(diskcache.Cache.__init__)
    kwargs = {
        k: v
        for k, v in _SETTINGS.items()
        if k in diskcache.DEFAULT_SETTINGS or k in sig.parameters.keys()
    }
    return diskcache.Cache(**kwargs, disk=diskcache.JSONDisk)


_SETTINGS["cache_store"] = initialize_cache_store()

# Immutable settings to be used by other modules
SETTINGS = MappingProxyType(_SETTINGS)


class set:
    # TODO: Add docstring
    def __init__(self, **kwargs: Any):

        if "cache_store" in kwargs:
            if len(kwargs) != 1:
                raise ValueError(
                    "'cache_store' is mutually exclusive with all other settings"
                )

            # infer settings from cache_store properties
            new_cache_store = kwargs["cache_store"]
            for key in _SETTINGS.keys() - kwargs.keys():
                if isinstance(getattr(type(new_cache_store), key, None), property):
                    kwargs[key] = getattr(new_cache_store, key)

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
        if "cache_store" not in kwargs:
            self._old["cache_store"] = _SETTINGS["cache_store"]
            _SETTINGS["cache_store"] = initialize_cache_store()

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _SETTINGS.update(self._old)
