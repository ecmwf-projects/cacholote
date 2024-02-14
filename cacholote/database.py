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
import warnings
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm

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
    timestamp = sa.Column(
        sa.DateTime,
        default=utils.utcnow,
        onupdate=utils.utcnow,
    )
    counter = sa.Column(sa.Integer)
    tag = sa.Column(sa.String)

    @property
    def _result_as_string(self) -> str:
        return json.dumps(self.result)

    def __repr__(self) -> str:
        public_attrs = ("id", "key", "expiration", "timestamp", "counter", "tag")
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
    engine = sa.create_engine(url, **_decode_kwargs(**kwargs))
    Base.metadata.create_all(engine)
    return sa.orm.sessionmaker(engine)


def cached_sessionmaker(url: str, **kwargs: Any) -> sa.orm.sessionmaker[sa.orm.Session]:
    return _cached_sessionmaker(url, **_encode_kwargs(**kwargs))
