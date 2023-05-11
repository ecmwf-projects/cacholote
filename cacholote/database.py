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

import datetime
import json
from typing import Any, Dict, Optional

import sqlalchemy as sa
import sqlalchemy.orm

from . import utils

_DATETIME_MAX = datetime.datetime(
    datetime.MAXYEAR, 12, 31, tzinfo=datetime.timezone.utc
)

ENGINE: Optional[sa.engine.Engine] = None
SESSIONMAKER: Optional[sa.orm.sessionmaker] = None  # type: ignore[type-arg]

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
        return f"CacheEntry(id={self.id!r}, key={self.key!r}, expiration={self.expiration!r})"


@sa.event.listens_for(CacheEntry, "before_insert")
def set_expiration_to_max(
    mapper: sa.orm.Mapper[CacheEntry],
    connection: sa.Connection,
    target: CacheEntry,
) -> None:
    target.expiration = target.expiration or _DATETIME_MAX
    if target.expiration < utils.utcnow():
        raise ValueError(f"Expiration date has passed. {target.expiration=}")


def _commit_or_rollback(session: sa.orm.Session) -> None:
    try:
        session.commit()
    finally:
        session.rollback()


def _set_engine_and_session(
    cache_db_urlpath: str, create_engine_kwargs: Dict[str, Any]
) -> None:
    global ENGINE, SESSIONMAKER
    ENGINE = sa.create_engine(cache_db_urlpath, future=True, **create_engine_kwargs)
    Base.metadata.create_all(ENGINE)
    SESSIONMAKER = sa.orm.sessionmaker(ENGINE)
