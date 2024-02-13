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
from __future__ import annotations

import datetime
import logging
import pathlib
import tempfile
from types import TracebackType
from typing import Any, Dict, Literal, Optional, Tuple, Type, Union

import fsspec
import pydantic
import pydantic_settings
import sqlalchemy as sa
import sqlalchemy.orm
import structlog

from . import database

_SETTINGS: Optional[Settings] = None
_DEFAULT_CACHE_DIR = pathlib.Path(tempfile.gettempdir()) / "cacholote"
_DEFAULT_CACHE_DIR.mkdir(exist_ok=True)
_DEFAULT_CACHE_DB_URLPATH = f"sqlite:///{_DEFAULT_CACHE_DIR / 'cacholote.db'}"
_DEFAULT_CACHE_FILES_URLPATH = f"{_DEFAULT_CACHE_DIR / 'cache_files'}"
_DEFAULT_LOGGER = structlog.get_logger(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING)
)


class Settings(pydantic_settings.BaseSettings):
    use_cache: bool = True
    cache_db_urlpath: Optional[str] = _DEFAULT_CACHE_DB_URLPATH
    create_engine_kwargs: Dict[str, Any] = {}
    sessionmaker: Optional[sa.orm.sessionmaker] = None  # type: ignore[type-arg]
    cache_files_urlpath: str = _DEFAULT_CACHE_FILES_URLPATH
    cache_files_urlpath_readonly: Optional[str] = None
    cache_files_storage_options: Dict[str, Any] = {}
    xarray_cache_type: Literal[
        "application/netcdf", "application/x-grib", "application/vnd+zarr"
    ] = "application/netcdf"
    io_delete_original: bool = False
    raise_all_encoding_errors: bool = False
    expiration: Optional[datetime.datetime] = None
    tag: Optional[str] = None
    return_cache_entry: bool = False
    logger: Union[
        structlog.BoundLogger, structlog._config.BoundLoggerLazyProxy
    ] = _DEFAULT_LOGGER
    lock_timeout: Optional[float] = None

    @pydantic.field_validator("create_engine_kwargs")
    def validate_create_engine_kwargs(
        cls: pydantic_settings.BaseSettings, create_engine_kwargs: Dict[str, Any]
    ) -> Dict[str, Any]:
        poolclass = create_engine_kwargs.get("poolclass")
        if isinstance(poolclass, str):
            create_engine_kwargs["poolclass"] = getattr(sa.pool, poolclass)
        return create_engine_kwargs

    @pydantic.field_validator("expiration")
    def validate_expiration(
        cls: pydantic_settings.BaseSettings, expiration: Optional[datetime.datetime]
    ) -> Optional[datetime.datetime]:
        if expiration is not None and expiration.tzinfo is None:
            raise ValueError(f"Expiration is missing the timezone info. {expiration=}")
        return expiration

    @pydantic.model_validator(mode="after")
    def make_cache_dir(self) -> "Settings":
        fs, _, (urlpath, *_) = fsspec.get_fs_token_paths(
            self.cache_files_urlpath,
            storage_options=self.cache_files_storage_options,
        )
        fs.mkdirs(urlpath, exist_ok=True)
        return self

    @pydantic.model_validator(mode="after")
    def check_mutually_exclusive(self) -> "Settings":
        if self.sessionmaker and (self.cache_db_urlpath or self.create_engine_kwargs):
            raise ValueError(
                "`sessionmaker` is mutually exclusive with `cache_db_urlpath` and `create_engine_kwargs`."
            )
        if not (self.sessionmaker or self.cache_db_urlpath):
            raise ValueError(
                "Please provide either `sessionmaker` or `cache_db_urlpath`."
            )
        return self

    @property
    def instantiated_sessionmaker(self) -> sa.orm.sessionmaker:  # type: ignore[type-arg]
        if self.sessionmaker is None:
            self.sessionmaker = database.cached_sessionmaker(
                self.cache_db_urlpath, **self.create_engine_kwargs
            )
            self.cache_db_urlpath = None
            self.create_engine_kwargs = {}
        return self.sessionmaker

    @property
    def engine(self) -> sa.engine.Engine:
        engine = self.instantiated_sessionmaker.kw["bind"]
        assert isinstance(engine, sa.engine.Engine)
        return engine

    model_config = pydantic_settings.SettingsConfigDict(
        case_sensitive=False, env_prefix="cacholote_"
    )


class set:
    """Customize cacholote settings.

    It is possible to use it either as a context manager, or to configure global settings.

    Parameters
    ----------
    use_cache: bool, default: True
        Enable/disable cache.
    cache_db_urlpath: str, None, default:"sqlite:////system_tmp_dir/cacholote/cacholote.db"
        URL for cache database (driver://user:pass@host/database).
    create_engine_kwargs: dict, default: {}
        Keyword arguments for ``sqlalchemy.create_engine``
    sessionmaker: sessionmaker, optional
        sqlalchemy.sessionamaker, mutually exclusive with cache_db_urlpath and create_engine_kwargs
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
    return_cache_entry: bool, default: False
        Whether to return the cache database entry rather than decoded results.
    lock_timeout: fload, optional, default: None
        Time to wait before raising an error if a cache file is locked.
    """

    def __init__(self, **kwargs: Any):
        self._old_settings = get()

        model_dump = self._old_settings.model_dump()
        if kwargs.get("cache_db_urlpath") or kwargs.get("create_engine_kwargs"):
            model_dump["sessionmaker"] = None
        if kwargs.get("sessionmaker") is not None:
            model_dump["cache_db_urlpath"] = None
            model_dump["create_engine_kwargs"] = {}

        global _SETTINGS
        _SETTINGS = Settings(**{**model_dump, **kwargs})

    def __enter__(self) -> Settings:
        return get()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        global _SETTINGS
        _SETTINGS = self._old_settings


def reset(env_file: Optional[Union[str, Tuple[str]]] = None) -> None:
    """Reset cacholote settings.

    Priority:
    1. Environment variables with prefix `CACHOLOTE_`
    2. Dotenv file(s)
    3. Cacholote defaults

    Parameters
    ----------
    env_file: str, tuple[str], default=None
        Dot env file(s).
    """
    global _SETTINGS
    _SETTINGS = Settings(_env_file=env_file)  # type: ignore[call-arg]
    set()


def get() -> Settings:
    """Get cacholote settings."""
    if _SETTINGS is None:
        reset()
        assert _SETTINGS is not None, "reset() did not work properly"
    return _SETTINGS.model_copy()
