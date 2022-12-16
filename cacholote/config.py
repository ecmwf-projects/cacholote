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

import contextvars
import datetime
import json
import pathlib
import tempfile
from types import TracebackType
from typing import Any, Dict, Literal, Optional, Tuple, Type, Union

import fsspec
import pydantic
import sqlalchemy
import sqlalchemy.orm

ENGINE: contextvars.ContextVar[sqlalchemy.engine.Engine] = contextvars.ContextVar(
    "cacholote_engine"
)
SETTINGS: contextvars.ContextVar[Settings] = contextvars.ContextVar(
    "cacholote_settings"
)
_DEFAULT_CACHE_DIR = pathlib.Path(tempfile.gettempdir()) / "cacholote"
_DEFAULT_CACHE_DIR.mkdir(exist_ok=True)

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
    target.expiration = target.expiration or datetime.datetime.max


class Settings(pydantic.BaseSettings):
    use_cache: bool = True
    cache_db_urlpath: str = f"sqlite:///{_DEFAULT_CACHE_DIR / 'cacholote.db'}"
    cache_files_urlpath: str = f"{_DEFAULT_CACHE_DIR / 'cache_files'}"
    cache_files_urlpath_readonly: Optional[str] = None
    cache_files_storage_options: Dict[str, Any] = {}
    xarray_cache_type: Literal[
        "application/netcdf", "application/x-grib", "application/vnd+zarr"
    ] = "application/netcdf"
    io_delete_original: bool = False
    raise_all_encoding_errors: bool = False
    expiration: Optional[str] = None
    tag: Optional[str] = None

    @pydantic.validator("expiration")
    def expiration_must_be_isoformat(
        cls: pydantic.BaseSettings, expiration: Optional[str]
    ) -> Optional[str]:
        """Validate expiration."""
        if isinstance(expiration, str):
            try:
                datetime.datetime.fromisoformat(expiration)
            except ValueError as ex:
                raise ValueError(
                    f"{expiration=} is NOT a valid ISO 8601 format"
                ) from ex
        return expiration

    def make_cache_dir(self) -> None:
        fs, _, (urlpath, *_) = fsspec.get_fs_token_paths(
            self.cache_files_urlpath,
            storage_options=self.cache_files_storage_options,
        )
        fs.mkdirs(urlpath, exist_ok=True)

    def set_engine(self) -> Optional[contextvars.Token]:  # type: ignore[type-arg] # py38 not subscriptable
        try:
            engine = ENGINE.get()
        except LookupError:
            pass
        else:
            if str(engine.url) == self.cache_db_urlpath:
                return None
        engine = sqlalchemy.create_engine(self.cache_db_urlpath, future=True)
        Base.metadata.create_all(engine)
        return ENGINE.set(engine)

    class Config:
        case_sensitive = False
        env_prefix = "cacholote_"


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
    """

    def __init__(self, **kwargs: Any):
        old_settings = SETTINGS.get()
        new_settings = Settings(**{**old_settings.dict(), **kwargs})
        new_settings.make_cache_dir()
        self._settings_token = SETTINGS.set(new_settings)
        self._engine_token = new_settings.set_engine()

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self._engine_token:
            ENGINE.reset(self._engine_token)
        SETTINGS.reset(self._settings_token)


def reset(env_file: Optional[Union[str, Tuple[str]]] = None) -> None:
    """Reset cacholote settings.

    Priority:
    1. Evironment variables with prefix `CACHOLOTE_`
    2. Dotenv file(s)
    3. Cacholote defaults

    Parameters
    ----------
    env_file: str, tuple[str], default=None
        Dot env file(s).
    """
    SETTINGS.set(Settings(_env_file=env_file))
    set()


reset()
