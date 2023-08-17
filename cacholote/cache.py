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

import functools
import json
import traceback
import warnings
from typing import Any, Callable, TypeVar, cast

import sqlalchemy as sa
import sqlalchemy.orm

from . import clean, config, database, decode, encode, utils
from .database import CacheEntry

F = TypeVar("F", bound=Callable[..., Any])


def _decode_and_update(
    session: sa.orm.Session,
    cache_entry: Any,
    settings: config.Settings,
) -> Any:
    result = decode.loads(cache_entry._result_as_string)
    cache_entry.counter = (cache_entry.counter or 0) + 1
    if settings.tag is not None:
        cache_entry.tag = settings.tag
    database._commit_or_rollback(session)
    if settings.return_cache_entry:
        session.refresh(cache_entry)
        return cache_entry
    return result


def _add_exception_log(
    session: sa.orm.Session,
    cache_entry: Any,
    settings: config.Settings,
) -> None:
    cache_entry.log = {"exception": traceback.format_exc()}
    with settings.sessionmaker() as session:
        session.add(cache_entry)
        database._commit_or_rollback(session)


def cacheable(func: F) -> F:
    """Make a function cacheable."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        settings = config.get()

        if not settings.use_cache:
            return func(*args, **kwargs)

        try:
            hexdigest = encode._hexdigestify_python_call(func, *args, **kwargs)
        except encode.EncodeError as exc:
            if settings.return_cache_entry:
                raise exc
            warnings.warn(f"can't encode python call: {exc!r}", UserWarning)
            return func(*args, **kwargs)

        filters = [
            CacheEntry.key == hexdigest,
            CacheEntry.result.isnot(None),
            CacheEntry.expiration > utils.utcnow(),
        ]
        if settings.expiration:
            # When expiration is provided, only get entries with matching expiration
            filters.append(CacheEntry.expiration == settings.expiration)

        with settings.sessionmaker() as session:
            for cache_entry in session.scalars(
                sa.select(CacheEntry)
                .filter(*filters)
                .order_by(CacheEntry.timestamp.desc())
            ):
                try:
                    return _decode_and_update(session, cache_entry, settings)
                except decode.DecodeError as exc:
                    warnings.warn(str(exc), UserWarning)
                    clean._delete_cache_entry(session, cache_entry)

        cache_entry = CacheEntry(
            key=hexdigest,
            expiration=settings.expiration,
            tag=settings.tag,
        )
        try:
            result = func(*args, **kwargs)
        except Exception:
            _add_exception_log(session, cache_entry, settings)
            raise
        try:
            cache_entry.result = json.loads(encode.dumps(result))
        except Exception as exc:
            _add_exception_log(session, cache_entry, settings)
            if settings.return_cache_entry or not isinstance(exc, encode.EncodeError):
                raise
            warnings.warn(f"can't encode output: {exc!r}", UserWarning)
            return result

        with settings.sessionmaker() as session:
            session.add(cache_entry)
            return _decode_and_update(session, cache_entry, settings)

    return cast(F, wrapper)
