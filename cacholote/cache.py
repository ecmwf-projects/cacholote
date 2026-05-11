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
from __future__ import annotations

import functools
import json
import warnings
from typing import Any, Callable, TypeVar, cast

import sqlalchemy as sa
import sqlalchemy.orm

from . import clean, config, database, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])


class NoCacheEntry(Exception):
    pass


def _decode_and_update(
    session: sa.orm.Session,
    cache_entry: Any,
    return_cache_entry: bool,
    tag: str | None,
) -> Any:
    if not return_cache_entry:
        result = decode.loads(cache_entry._result_as_string)
    cache_entry.counter = (cache_entry.counter or 0) + 1
    if tag is not None:
        cache_entry.tag = tag
    database._commit_or_rollback(session)
    if return_cache_entry:
        session.refresh(cache_entry)
        return cache_entry
    return result


def cacheable(
    func: F,
    compute: bool = True,
    **cache_kwargs: Any,
) -> F:
    """Make a function cacheable.

    Parameters
    ----------
    func: callable
        Function to cache
    compute: bool
        Enables computation when a cached result cannot be found
    **cache_kwargs: Any
        Additional kwargs to use for hashing

    Returns
    -------
    callable
        Cached function
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        settings = config.get()

        return_cache_entry = True if compute is False else settings.return_cache_entry

        if not settings.use_cache and not compute:
            raise ValueError(
                "Invalid configuration: 'use_cache' and 'compute' cannot both be False."
            )

        if compute and not settings.use_cache and not return_cache_entry:
            return func(*args, **kwargs)

        try:
            hexdigest = encode._hexdigestify_python_call(
                func, *args, cache_kwargs=cache_kwargs, **kwargs
            )
        except encode.EncodeError as ex:
            if not compute or return_cache_entry:
                raise ex
            warnings.warn(f"can NOT encode python call: {ex!r}", UserWarning)
            return func(*args, **kwargs)

        if settings.use_cache:
            filters = [
                database.CacheEntry.key == hexdigest,
                database.CacheEntry.expiration > utils.utcnow(),
            ]
            if settings.expiration:
                # When expiration is provided, only get entries with matching expiration
                filters.append(database.CacheEntry.expiration == settings.expiration)

            with settings.instantiated_sessionmaker() as session:
                for cache_entry in session.scalars(
                    sa.select(database.CacheEntry)
                    .filter(*filters)
                    .order_by(database.CacheEntry.updated_at.desc())
                ):
                    try:
                        return _decode_and_update(
                            session,
                            cache_entry,
                            return_cache_entry,
                            settings.tag,
                        )
                    except decode.DecodeError as ex:
                        warnings.warn(str(ex), UserWarning)
                        clean._delete_cache_entries(session, cache_entry)

        if not compute:
            raise NoCacheEntry(f"No cache entry for key: {hexdigest}")

        result = func(*args, **kwargs)
        cache_entry = database.CacheEntry(
            key=hexdigest,
            expiration=settings.expiration,
            tag=settings.tag,
        )
        try:
            cache_entry.result = json.loads(encode.dumps(result))
        except encode.EncodeError as ex:
            if return_cache_entry:
                raise ex
            warnings.warn(f"can NOT encode output: {ex!r}", UserWarning)
            return result

        with settings.instantiated_sessionmaker() as session:
            session.add(cache_entry)
            return _decode_and_update(
                session, cache_entry, settings.return_cache_entry, settings.tag
            )

    return cast(F, wrapper)


def cacheable_no_compute(
    func: F, **cache_kwargs: Any
) -> Callable[..., database.CacheEntry]:
    result = cacheable(func, compute=False, **cache_kwargs)
    return result
