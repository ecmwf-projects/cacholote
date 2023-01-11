import contextvars
import datetime
import json
from typing import Any, Dict

import sqlalchemy
import sqlalchemy.orm

ENGINE: contextvars.ContextVar[sqlalchemy.engine.Engine] = contextvars.ContextVar(
    "cacholote_engine"
)

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
