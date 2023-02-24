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

import sqlalchemy
import sqlalchemy.orm

ENGINE: Optional[sqlalchemy.engine.Engine] = None
SESSIONMAKER: Optional[sqlalchemy.orm.sessionmaker] = None  # type: ignore[type-arg]

Base = sqlalchemy.orm.declarative_base()


class CacheEntry(Base):
    __tablename__ = "cache_entries"

    id = sqlalchemy.Column(sqlalchemy.Integer(), primary_key=True)
    key = sqlalchemy.Column(sqlalchemy.String(32))
    expiration = sqlalchemy.Column(sqlalchemy.DateTime, default=datetime.datetime.max)
    result = sqlalchemy.Column(sqlalchemy.JSON)
    timestamp = sqlalchemy.Column(
        sqlalchemy.DateTime,
        default=datetime.datetime.utcnow,
        onupdate=datetime.datetime.utcnow,
    )
    counter = sqlalchemy.Column(sqlalchemy.Integer)
    tag = sqlalchemy.Column(sqlalchemy.String)

    @property
    def _result_as_string(self) -> str:
        return json.dumps(self.result)

    def __repr__(self) -> str:
        return f"CacheEntry(id={self.id!r}, key={self.key!r}, expiration={self.expiration!r})"


@sqlalchemy.event.listens_for(CacheEntry, "before_insert")  # type: ignore[misc]
def set_expiration_to_max(
    mapper: sqlalchemy.orm.Mapper,
    connection: sqlalchemy.engine.Connection,
    target: CacheEntry,
) -> None:
    target.expiration = target.expiration or datetime.datetime.max
    if target.expiration < datetime.datetime.utcnow():
        raise ValueError("Expiration date has passed.")


def _commit_or_rollback(session: sqlalchemy.orm.Session) -> None:
    try:
        session.commit()
    finally:
        session.rollback()


def _set_engine_and_session(
    cache_db_urlpath: str, create_engine_kwargs: Dict[str, Any]
) -> None:
    global ENGINE, SESSIONMAKER
    ENGINE = sqlalchemy.create_engine(
        cache_db_urlpath, future=True, **create_engine_kwargs
    )
    Base.metadata.create_all(ENGINE)
    SESSIONMAKER = sqlalchemy.orm.sessionmaker(ENGINE)
