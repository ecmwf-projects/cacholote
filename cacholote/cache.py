"""Public decorator."""

# Copyright 2019, B-Open Solutions srl.
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

import contextlib
import contextvars
import datetime
import functools
import json
import warnings
from typing import Any, Callable, Dict, Iterator, Optional, TypeVar, Union, cast

import sqlalchemy
import sqlalchemy.orm

from . import clean, config, database, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])

LAST_PRIMARY_KEYS: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "cacholote_last_primary_keys"
)

_LOCKER = "__locked__"


def _decode_and_update(
    session: sqlalchemy.orm.Session,
    cache_entry: Any,
    settings: config.Settings,
) -> Any:
    result = decode.loads(cache_entry._result_as_string)
    cache_entry.counter += 1
    if settings.tag is not None:
        cache_entry.tag = settings.tag
    database._commit_or_rollback(session)
    LAST_PRIMARY_KEYS.set(cache_entry._primary_keys)
    return result


def _delete_cache_entry(
    session: sqlalchemy.orm.Session, cache_entry: database.CacheEntry
) -> None:
    # First, delete database entry
    session.delete(cache_entry)
    database._commit_or_rollback(session)
    # Then, delete files
    json.loads(cache_entry._result_as_string, object_hook=clean._delete_cache_file)


def _hexdigestify_python_call(
    func_to_hex: Union[str, Callable[..., Any]],
    *args: Any,
    **kwargs: Any,
) -> str:
    return utils.hexdigestify(encode.dumps_python_call(func_to_hex, *args, **kwargs))


@contextlib.contextmanager
def _lock_cache_entry(
    session: sqlalchemy.orm.Session,
    hexdigest: str,
    expiration: Optional[datetime.datetime],
    settings: config.Settings,
) -> Iterator[database.CacheEntry]:
    cache_entry = database.CacheEntry(
        key=hexdigest,
        expiration=expiration,
        result=_LOCKER,
        tag=settings.tag,
    )
    session.add(cache_entry)
    database._commit_or_rollback(session)

    cache_entry = (
        session.query(database.CacheEntry)
        .filter(
            database.CacheEntry.key == cache_entry.key,
            database.CacheEntry.expiration == cache_entry.expiration,
        )
        .with_for_update()
        .one()
    )
    try:
        yield cache_entry
    finally:
        if cache_entry.result == _LOCKER:
            _delete_cache_entry(session, cache_entry)


def cacheable(func: F) -> F:
    """Make a function cacheable.

    The __context__ argument allows to set the `contextvars.Context`.
    __context__ is not passed to the wrapped function.
    """

    @functools.wraps(func)
    def wrapper(
        *args: Any, __context__: Optional[contextvars.Context] = None, **kwargs: Any
    ) -> Any:
        if __context__:
            for key, value in __context__.items():
                key.set(value)

        LAST_PRIMARY_KEYS.set({})

        settings = config.get()
        if not settings.use_cache:
            return func(*args, **kwargs)

        try:
            hexdigest = _hexdigestify_python_call(func, *args, **kwargs)
        except encode.EncodeError as ex:
            warnings.warn(f"can NOT encode python call: {ex!r}", UserWarning)
            return func(*args, **kwargs)

        filters = [
            database.CacheEntry.key == hexdigest,
            database.CacheEntry.expiration > datetime.datetime.utcnow(),
        ]

        expiration = (
            datetime.datetime.fromisoformat(settings.expiration)
            if settings.expiration is not None
            else settings.expiration
        )
        if expiration is not None:
            # When expiration is provided, only get entries with matching expiration
            filters.append(database.CacheEntry.expiration == expiration)

        with database.SESSION.get()() as session:
            for cache_entry in (
                session.query(database.CacheEntry)
                .filter(*filters)
                .order_by(database.CacheEntry.timestamp.desc())
                .with_for_update()
            ):
                try:
                    return _decode_and_update(session, cache_entry, settings)
                except decode.DecodeError as ex:
                    warnings.warn(str(ex), UserWarning)
                    _delete_cache_entry(session, cache_entry)

            with _lock_cache_entry(
                session, hexdigest, expiration, settings
            ) as cache_entry:
                try:
                    result = func(*args, **kwargs)
                    cache_entry.result = json.loads(encode.dumps(result))
                    return _decode_and_update(session, cache_entry, settings)
                except encode.EncodeError as ex:
                    warnings.warn(f"can NOT encode output: {ex!r}", UserWarning)
                    return result

    return cast(F, wrapper)
