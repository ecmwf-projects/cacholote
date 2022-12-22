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

import contextvars
import datetime
import functools
import json
import time
import warnings
from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast

import sqlalchemy
import sqlalchemy.orm

from . import clean, config, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])

LAST_PRIMARY_KEYS: contextvars.ContextVar[Dict[str, Any]] = contextvars.ContextVar(
    "cacholote_last_primary_keys"
)

_LOCKER = "__locked__"


def _update_last_primary_keys(
    session: sqlalchemy.orm.Session, cache_entry: Any, tag: Optional[str]
) -> Any:
    # Wait until unlocked
    warned = False
    while cache_entry.result == _LOCKER:
        session.refresh(cache_entry)
        if not warned:
            warnings.warn(
                f"can NOT proceed until the cache entry is unlocked: {cache_entry!r}."
            )
            warned = True
        time.sleep(1)
    # Get result
    result = decode.loads(cache_entry._result_as_string)
    cache_entry.counter += 1
    if tag is not None:
        cache_entry.tag = tag
    try:
        session.commit()
    finally:
        session.rollback()
    LAST_PRIMARY_KEYS.set(cache_entry._primary_keys)
    return result


def _clear_last_primary_keys(result: Any) -> Any:
    LAST_PRIMARY_KEYS.set({})
    return result


def _delete_cache_entry(
    session: sqlalchemy.orm.Session, cache_entry: config.CacheEntry
) -> None:
    session.delete(cache_entry)
    try:
        session.commit()
    finally:
        session.rollback()
    # Delete cache file
    json.loads(cache_entry._result_as_string, object_hook=clean._delete_cache_file)


def hexdigestify_python_call(
    func_to_hex: Union[str, Callable[..., Any]],
    *args: Any,
    **kwargs: Any,
) -> str:
    """Convert function to its hash made of hexadecimal digits.

    Parameters
    ----------
    func_to_hex: str, callable
        Function to hexdigestify
    *args: Any
        Arguments of ``func``
    **kwargs: Any
        Keyword arguments of ``func``

    Returns
    -------
    str
    """
    return utils.hexdigestify(encode.dumps_python_call(func_to_hex, *args, **kwargs))


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

        settings = config.SETTINGS.get()
        tag = settings.tag
        expiration = (
            datetime.datetime.fromisoformat(settings.expiration)
            if settings.expiration is not None
            else settings.expiration
        )

        # Cache opt-out
        if not settings.use_cache:
            return _clear_last_primary_keys(func(*args, **kwargs))

        try:
            # Get key
            hexdigest = hexdigestify_python_call(func, *args, **kwargs)
        except encode.EncodeError as ex:
            warnings.warn(f"can NOT encode python call: {ex!r}", UserWarning)
            return _clear_last_primary_keys(func(*args, **kwargs))

        # Filters for the database query
        filters = [
            config.CacheEntry.key == hexdigest,
            config.CacheEntry.expiration > datetime.datetime.utcnow(),
        ]
        if expiration is not None:
            # If expiration is provided, only get entries with matching expiration
            filters.append(config.CacheEntry.expiration == expiration)
        with sqlalchemy.orm.Session(config.ENGINE.get(), autoflush=False) as session:
            for cache_entry in (
                session.query(config.CacheEntry)
                .filter(*filters)
                .order_by(config.CacheEntry.timestamp.desc())
            ):
                # Attempt all valid cache entries
                try:
                    return _update_last_primary_keys(session, cache_entry, tag)
                except decode.DecodeError as ex:
                    # Something wrong, e.g. cached files are corrupted
                    warnings.warn(str(ex), UserWarning)
                    _delete_cache_entry(session, cache_entry)

            # Not in the cache
            cache_entry = None
            try:
                # Acquire lock
                cache_entry = config.CacheEntry(
                    key=hexdigest,
                    expiration=expiration,
                    result=_LOCKER,
                    tag=settings.tag,
                )
                session.add(cache_entry)
                try:
                    session.commit()
                finally:
                    session.rollback()
            except sqlalchemy.exc.IntegrityError:
                # Concurrent job: This cache entry already exists.
                filters = [
                    config.CacheEntry.key == cache_entry.key,
                    config.CacheEntry.expiration == cache_entry.expiration,
                ]
                cache_entry = session.query(config.CacheEntry).filter(*filters).one()
                return _update_last_primary_keys(session, cache_entry, tag)
            else:
                # Compute result from scratch
                result = func(*args, **kwargs)
                try:
                    # Update cache
                    cache_entry.result = json.loads(encode.dumps(result))
                    return _update_last_primary_keys(session, cache_entry, tag)
                except encode.EncodeError as ex:
                    # Enconding error, return result without caching
                    warnings.warn(f"can NOT encode output: {ex!r}", UserWarning)
                    return _clear_last_primary_keys(result)
            finally:
                # Release lock
                if cache_entry and cache_entry.result == _LOCKER:
                    _delete_cache_entry(session, cache_entry)

    return cast(F, wrapper)
