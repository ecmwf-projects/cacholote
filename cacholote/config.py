"""Global settings."""

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
from typing import Any, Dict, List, Optional, Type

import fsspec
import sqlalchemy
import sqlalchemy.orm

TMPDIR = os.path.join(tempfile.gettempdir(), "cacholote")
FILES_DIR = os.path.join(TMPDIR, "cache_files")
os.makedirs(FILES_DIR, exist_ok=True)

Base = sqlalchemy.orm.declarative_base()


class CacheEntry(Base):
    __tablename__ = "cacholote"
    key = sqlalchemy.Column(sqlalchemy.String(56), primary_key=True)
    value = sqlalchemy.Column(sqlalchemy.String)
    timestamp = sqlalchemy.Column(sqlalchemy.DateTime)
    count = sqlalchemy.Column(sqlalchemy.Integer)


_ALLOWED_SETTINGS: Dict[str, List[Any]] = {
    "xarray_cache_type": [
        "application/netcdf",
        "application/x-grib",
        "application/vnd+zarr",
    ]
}

_SETTINGS: Dict[str, Any] = {
    "cache_db_urlpath": "sqlite:///" + os.path.join(TMPDIR, "cacholote.db"),
    "cache_files_urlpath": os.path.join(TMPDIR, "cache_files"),
    "cache_files_urlpath_readonly": None,
    "cache_files_storage_options": {},
    "xarray_cache_type": "application/netcdf",
    "io_delete_original": False,
    "raise_all_encoding_errors": False,
}


def _create_engine() -> None:
    _SETTINGS["engine"] = sqlalchemy.create_engine(
        _SETTINGS["cache_db_urlpath"],
        future=True,
    )
    Base.metadata.create_all(_SETTINGS["engine"])


_create_engine()

# Immutable settings to be used by other modules
SETTINGS = MappingProxyType(_SETTINGS)


class set:
    """Customize cacholote settings.

    It is possible to use it either as a context manager, or to configure global settings.

    Parameters
    ----------
    cache_db_urlpath: str, default:"sqlite:////system_tmp_dir/cacholote.db"
        URL for cache database.
    cache_files_urlpath : str, default:"/system_tmp_dir/cache_files"
        URL for cache files.
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
    engine:
        `sqlalchemy` Engine. Mutually exclusive with ``cache_db_urlpath``.
    """

    def __init__(self, **kwargs: Any):

        for k, v in kwargs.items():
            if k in _ALLOWED_SETTINGS and v not in _ALLOWED_SETTINGS[k]:
                raise ValueError(f"{k!r} must be one of {_ALLOWED_SETTINGS[k]!r}")

        if "engine" in kwargs:
            if "cache_db_urlpath" in kwargs:
                raise ValueError(
                    "'engine' and 'cache_db_urlpath' are mutually exclusive"
                )
            kwargs["cache_db_urlpath"] = None

        try:
            self._old = {key: _SETTINGS[key] for key in kwargs}
        except KeyError as ex:
            raise ValueError(
                f"Wrong settings. Available settings: {list(_SETTINGS)}"
            ) from ex

        _SETTINGS.update(kwargs)

        # Create engine
        if kwargs.get("cache_db_urlpath") is not None:
            self._old["engine"] = _SETTINGS["engine"]
            _create_engine()

        # Create cache files directory
        fs, _, (urlpath, *_) = fsspec.get_fs_token_paths(
            SETTINGS["cache_files_urlpath"],
            storage_options=SETTINGS["cache_files_storage_options"],
        )
        fs.mkdirs(urlpath, exist_ok=True)

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _SETTINGS.update(self._old)
