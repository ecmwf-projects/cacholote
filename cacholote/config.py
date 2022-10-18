"""Global settings."""

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

import os
import tempfile
from types import MappingProxyType, TracebackType
from typing import Any, Dict, List, Optional, Type

import diskcache

_ALLOWED_SETTINGS: Dict[str, List[Any]] = {
    "xarray_cache_type": [
        "application/netcdf",
        "application/x-grib",
        "application/vnd+zarr",
    ]
}

_SETTINGS: Dict[str, Any] = {
    "cache_store_directory": os.path.join(tempfile.gettempdir(), "cacholote"),
    "cache_files_urlpath": None,
    "cache_files_storage_options": {},
    "cache_files_urlpath_readonly": None,
    "xarray_cache_type": "application/netcdf",
    "io_delete_original": False,
    "raise_all_encoding_errors": False,
}


def _initialize_cache_store() -> None:
    _SETTINGS["cache_store"] = diskcache.Cache(
        _SETTINGS["cache_store_directory"], disk=diskcache.JSONDisk, statistics=1
    )


_initialize_cache_store()

# Immutable settings to be used by other modules
SETTINGS = MappingProxyType(_SETTINGS)


class set:
    """Customize cacholote settings.

    It is possible to use it either as a context manager, or to configure global settings.

    Parameters
    ----------
    cache_store_directory : str, default: "system-specific-tmpdir/cacholote"
        Directory for the cache store. Mutually exclusive with ``cache_store``.
    cache_files_urlpath : str, None, default: None
        URL for cache files.
        None: same as ``cache_store_directory``
    cache_files_storage_options : dict, default: {}
        ``fsspec`` storage options for storing cache files.
    cache_files_urlpath_readonly : str, None, default: None
        URL for cache files accessible in read-only mode.
        None: same as ``cache_files_urlpath``
    xarray_cache_type : {"application/netcdf", "application/x-grib", "application/vnd+zarr"}, \
        default: "application/netcdf"
        Type for ``xarray`` cache files.
    io_delete_original: bool, default: False
        Whether to delete the original copy of cached files.
    raise_all_encoding_errors: bool, default: False
        Raise an error if an encoder does not work (i.e., do not return results).
    cache_store:
        Key-value store object for the cache. Mutually exclusive with ``cache_store_directory``.
    """

    def __init__(self, **kwargs: Any):

        for k, v in kwargs.items():
            if k in _ALLOWED_SETTINGS and v not in _ALLOWED_SETTINGS[k]:
                raise ValueError(f"{k!r} must be one of {_ALLOWED_SETTINGS[k]!r}")

        if "cache_store" in kwargs:
            if "cache_store_directory" in kwargs:
                raise ValueError(
                    "'cache_store' and 'cache_store_directory' are mutually exclusive"
                )
            kwargs["cache_store_directory"] = None

        try:
            self._old = {key: _SETTINGS[key] for key in kwargs}
        except KeyError as ex:
            raise ValueError(
                f"Wrong settings. Available settings: {list(_SETTINGS)}"
            ) from ex

        _SETTINGS.update(kwargs)
        if kwargs.get("cache_store_directory") is not None:
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
