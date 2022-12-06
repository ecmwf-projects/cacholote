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

import builtins
import datetime
import distutils.util
import json
import os
import pathlib
import tempfile
from types import MappingProxyType, TracebackType
from typing import Any, Dict, List, Optional, Type

import fsspec
import sqlalchemy
import sqlalchemy.orm

Base = sqlalchemy.orm.declarative_base()


class CacheEntry(Base):
    __tablename__ = "cache_entries"

    key = sqlalchemy.Column(sqlalchemy.String(56), primary_key=True)
    expiration = sqlalchemy.Column(
        sqlalchemy.DateTime, default=datetime.datetime.max, primary_key=True
    )
    result = sqlalchemy.Column(sqlalchemy.JSON)
    timestamp = sqlalchemy.Column(
        sqlalchemy.DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )
    counter = sqlalchemy.Column(sqlalchemy.Integer, default=0)
    tag = sqlalchemy.Column(sqlalchemy.String)

    constraint = sqlalchemy.UniqueConstraint(key, expiration)

    @property
    def _result_as_string(self) -> str:
        return json.dumps(self.result)

    @property
    def _primary_keys(self) -> Dict[str, Any]:
        return {name: getattr(self, name) for name in ["key", "expiration"]}

    def __repr__(self) -> str:
        return str(self._primary_keys)


@sqlalchemy.event.listens_for(CacheEntry, "before_insert")  # type: ignore[misc]
def set_epiration_to_max(
    mapper: sqlalchemy.orm.Mapper,
    connection: sqlalchemy.engine.Connection,
    target: CacheEntry,
) -> None:
    expiration = target.expiration or datetime.datetime.max
    if isinstance(expiration, str):
        expiration = datetime.datetime.fromisoformat(expiration)
    target.expiration = expiration


_ALLOWED_SETTINGS: Dict[str, List[Any]] = {
    "xarray_cache_type": [
        "application/netcdf",
        "application/x-grib",
        "application/vnd+zarr",
    ]
}

_DEFAULT_CACHE_DIR = pathlib.Path(tempfile.gettempdir()) / "cacholote"
_DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_DEFAULTS: Dict[str, Any] = {
    "use_cache": True,
    "cache_db_urlpath": f"sqlite:///{_DEFAULT_CACHE_DIR / 'cacholote.db'}",
    "cache_files_urlpath": f"{_DEFAULT_CACHE_DIR / 'cache_files'}",
    "cache_files_urlpath_readonly": None,
    "cache_files_storage_options": {},
    "xarray_cache_type": "application/netcdf",
    "io_delete_original": False,
    "raise_all_encoding_errors": False,
    "expiration": None,
    "tag": None,
    "engine": None,
}

# Private and public (immutable) settings
_SETTINGS: Dict[str, Any] = {}
SETTINGS = MappingProxyType(_SETTINGS)


class set:
    """Customize cacholote settings.

    It is possible to use it either as a context manager, or to configure global settings.

    Parameters
    ----------
    use_cache: bool, default: True
        Enable/disable cache.
    cache_db_urlpath: str, default:"sqlite:////system_tmp_dir/cacholote/cacholote.db"
        URL for cache database (driver://user:pass@host/database).
    cache_files_urlpath: str, default:"/system_tmp_dir/cacholote/cache_files"
        URL for cache files (protocol://location).
    cache_files_storage_options: dict, default: {}
        ``fsspec`` storage options for storing cache files.
    cache_files_urlpath_readonly: str, None, default: None
        URL for cache files accessible in read-only mode.
        None: same as ``cache_files_urlpath``
    xarray_cache_type: {"application/netcdf", "application/x-grib", "application/vnd+zarr"}, \
        default: "application/netcdf"
        Type for ``xarray`` cache files.
    io_delete_original: bool, default: False
        Whether to delete the original copy of cached files.
    raise_all_encoding_errors: bool, default: False
        Raise an error if an encoder does not work (i.e., do not return results).
    expiration: datetime, optional, default: None
        Expiration for cached results.
    tag: str, optional, default: None
        Tag for the cache entry. If None, do NOT tag.
        Note that existing tags are overwritten.
    engine:
        `sqlalchemy` Engine. Mutually exclusive with ``cache_db_urlpath``.
    """

    def __init__(self, **kwargs: Any):

        extra_settings = builtins.set(kwargs) - builtins.set(_DEFAULTS)
        if extra_settings:
            raise ValueError(
                f"Wrong settings: {extra_settings!r}. Available settings: {list(_SETTINGS)!r}"
            )

        for k in builtins.set(_ALLOWED_SETTINGS) & builtins.set(kwargs):
            if kwargs[k] not in _ALLOWED_SETTINGS[k]:
                raise ValueError(f"{k!r} must be one of {_ALLOWED_SETTINGS[k]!r}")

        if hasattr(kwargs.get("expiration"), "isoformat"):
            # Store datetime as string
            kwargs["expiration"] = kwargs["expiration"].isoformat()

        # Cache DB
        if kwargs.get("engine") and kwargs.get("cache_db_urlpath"):
            raise ValueError("'engine' and 'cache_db_urlpath' are mutually exclusive")
        if kwargs.get("engine"):
            kwargs["cache_db_urlpath"] = None
        elif kwargs.get("cache_db_urlpath"):
            engine = sqlalchemy.create_engine(kwargs["cache_db_urlpath"], future=True)
            Base.metadata.create_all(engine)
            kwargs["engine"] = engine

        # Update
        self._old = dict(SETTINGS)
        _SETTINGS.update(kwargs)

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


def json_dumps() -> str:
    """Serialize configuration to a JSON formatted string."""
    if SETTINGS["cache_db_urlpath"] is None:
        raise ValueError("Can NOT dump to JSON when `engine` has been directly set.")
    return json.dumps({k: v for k, v in SETTINGS.items() if k != "engine"})


def _initialize_settings() -> None:

    settings = {}
    for key, default in _DEFAULTS.items():
        value = os.getenv(f"CACHOLOTE_{key.upper()}", default)
        if isinstance(default, bool):
            value = bool(distutils.util.strtobool(str(value)))
        settings[key] = value
    set(**settings)


_initialize_settings()
