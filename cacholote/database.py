"""Database objects."""

# Copyright 2023, European Union.
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
import functools
import json
import os
import warnings
from typing import Any

import alembic.command
import alembic.config
import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy_utils

from . import utils

_DATETIME_MAX = datetime.datetime(
    datetime.MAXYEAR, 12, 31, tzinfo=datetime.timezone.utc
)

Base = sa.orm.declarative_base()


class CacheEntry(Base):
    __tablename__ = "cache_entries"

    id = sa.Column(sa.Integer(), primary_key=True)
    key = sa.Column(sa.String(32))
    expiration = sa.Column(sa.DateTime, default=_DATETIME_MAX)
    result = sa.Column(sa.JSON)
    created_at = sa.Column(sa.DateTime, default=utils.utcnow)
    updated_at = sa.Column(sa.DateTime, default=utils.utcnow, onupdate=utils.utcnow)
    counter = sa.Column(sa.Integer)
    tag = sa.Column(sa.String)

    __table_args__ = (sa.Index("ix_cache_entries_key_expiration", "key", "expiration"),)

    @property
    def _result_as_string(self) -> str:
        return json.dumps(self.result)

    def __repr__(self) -> str:
        public_attrs = (
            "id",
            "key",
            "expiration",
            "created_at",
            "updated_at",
            "counter",
            "tag",
        )
        public_attrs_repr = ", ".join(
            [f"{attr}={getattr(self, attr)!r}" for attr in public_attrs]
        )
        return f"CacheEntry({public_attrs_repr})"


@sa.event.listens_for(CacheEntry, "before_insert")
def set_expiration_to_max(
    mapper: sa.orm.Mapper[CacheEntry],
    connection: sa.Connection,
    target: CacheEntry,
) -> None:
    target.expiration = target.expiration or _DATETIME_MAX
    if target.expiration < utils.utcnow():
        warnings.warn(f"Expiration date has passed. {target.expiration=}", UserWarning)


def _commit_or_rollback(session: sa.orm.Session) -> None:
    try:
        session.commit()
    finally:
        session.rollback()


def _encode_kwargs(**kwargs: Any) -> dict[str, Any]:
    encoded_kwargs = {}
    for key, value in kwargs.items():
        if isinstance(value, dict):
            encoded_kwargs["_encoded_" + key] = json.dumps(value)
        else:
            encoded_kwargs[key] = value
    return encoded_kwargs


def _decode_kwargs(**kwargs: Any) -> dict[str, Any]:
    decoded_kwargs = {}
    for key, value in kwargs.items():
        if key.startswith("_encoded_"):
            decoded_kwargs[key.replace("_encoded_", "", 1)] = json.loads(value)
        else:
            decoded_kwargs[key] = value
    return decoded_kwargs


@functools.lru_cache
def _cached_sessionmaker(
    url: str, **kwargs: Any
) -> sa.orm.sessionmaker[sa.orm.Session]:
    engine = init_database(url, **_decode_kwargs(**kwargs))
    Base.metadata.create_all(engine)
    return sa.orm.sessionmaker(engine)


def cached_sessionmaker(url: str, **kwargs: Any) -> sa.orm.sessionmaker[sa.orm.Session]:
    return _cached_sessionmaker(url, **_encode_kwargs(**kwargs))


def init_database(
    connection_string: str, force: bool = False, **kwargs: Any
) -> sa.engine.Engine:
    """
    Make sure the db located at URI `connection_string` exists updated and return the engine object.

    Parameters
    ----------
    connection_string: str
        Something like 'postgresql://user:password@netloc:port/dbname'
    force: bool
        if True, drop the database structure and build again from scratch
    kwargs: Any
        Keyword arguments for create_engine

    Returns
    -------
    engine: Engine
    """
    engine = sa.create_engine(connection_string, **kwargs)
    migration_directory = os.path.abspath(os.path.join(__file__, ".."))
    with utils.change_working_dir(migration_directory):
        alembic_config_path = os.path.join(migration_directory, "alembic.ini")
        alembic_cfg = alembic.config.Config(alembic_config_path)
        for option in [
            "drivername",
            "username",
            "password",
            "host",
            "port",
            "database",
        ]:
            value = getattr(engine.url, option)
            if value is None:
                value = ""
            alembic_cfg.set_main_option(option, str(value))
        if not sqlalchemy_utils.database_exists(engine.url):
            sqlalchemy_utils.create_database(engine.url)
            # cleanup and create the schema
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)
            alembic.command.stamp(alembic_cfg, "head")
        elif "cache_entries" not in sa.inspect(engine).get_table_names():
            # db structure is empty or incomplete
            force = True
        if force:
            # cleanup and create the schema
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)
            alembic.command.stamp(alembic_cfg, "head")
        else:
            # update db structure
            alembic.command.upgrade(alembic_cfg, "head")
    return engine
