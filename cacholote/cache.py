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

import datetime
import functools
import json
import time
import warnings
from typing import Any, Callable, Dict, TypeVar, Union, cast

import sqlalchemy
import sqlalchemy.orm

from . import clean, config, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])

LAST_PRIMARY_KEYS: Dict[str, Any] = {}

_LOCKER = "__locked__"


def _update_last_primary_keys_and_return(
    session: sqlalchemy.orm.Session, cache_entry: Any
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
    session.commit()
    LAST_PRIMARY_KEYS.update(cache_entry._primary_keys)
    return result


def _clear_last_primary_keys_and_return(result: Any) -> Any:
    LAST_PRIMARY_KEYS.clear()
    return result


def hexdigestify_python_call(
    func: Union[str, Callable[..., Any]],
    *args: Any,
    **kwargs: Any,
) -> str:
    """Convert function to its hash made of hexadecimal digits.

    Parameters
    ----------
    func: str, callable
        Function to hexdigestify
    *args: Any
        Arguments of ``func``
    **kwargs: Any
        Keyword arguments of ``func``

    Returns
    -------
    str
    """
    return utils.hexdigestify(encode.dumps_python_call(func, *args, **kwargs))


def cacheable(func: F) -> F:
    """Make a function cacheable."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # Cache opt-out
        if not config.SETTINGS["use_cache"]:
            return _clear_last_primary_keys_and_return(func(*args, **kwargs))

        # Key defining the function and its arguments
        try:
            hexdigest = hexdigestify_python_call(
                func,
                *args,
                **kwargs,
            )
        except encode.EncodeError as ex:
            warnings.warn(f"can NOT encode python call: {ex!r}", UserWarning)
            return _clear_last_primary_keys_and_return(func(*args, **kwargs))

        # Filters for the database query
        filters = [
            config.CacheEntry.key == hexdigest,
            config.CacheEntry.expiration > datetime.datetime.utcnow(),
        ]
        if config.SETTINGS["expiration"]:
            # If expiration is provided, only get entries with matching expiration
            filters.append(
                config.CacheEntry.expiration
                == datetime.datetime.fromisoformat(config.SETTINGS["expiration"])
            )
        with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
            for cache_entry in (
                session.query(config.CacheEntry)
                .filter(*filters)
                .order_by(config.CacheEntry.timestamp.desc())
            ):
                try:
                    return _update_last_primary_keys_and_return(session, cache_entry)
                except decode.DecodeError as ex:
                    # Something wrong, e.g. cached files are corrupted
                    warnings.warn(str(ex), UserWarning)
                    clean.delete_cache_entry(session, cache_entry)

            # Not in the cache
            try:
                # Lock cache entry
                cache_entry = config.CacheEntry(
                    key=hexdigest,
                    expiration=config.SETTINGS["expiration"],
                    result=_LOCKER,
                )
                session.add(cache_entry)
                session.commit()
            except sqlalchemy.exc.IntegrityError:
                # Concurrent job: This cache entry already exist.
                filters = [
                    config.CacheEntry.key == cache_entry.key,
                    config.CacheEntry.expiration == cache_entry.expiration,
                ]
                session.rollback()
                cache_entry = session.query(config.CacheEntry).filter(*filters).one()
                return _update_last_primary_keys_and_return(session, cache_entry)

            # Compute result from scratch and unlock
            result = func(*args, **kwargs)
            try:
                cache_entry.result = json.loads(encode.dumps(result))
                return _update_last_primary_keys_and_return(session, cache_entry)
            except Exception as ex:
                clean.delete_cache_entry(session, cache_entry)
                if not isinstance(ex, encode.EncodeError):
                    raise ex
                warnings.warn(f"can NOT encode output: {ex!r}", UserWarning)
                return _clear_last_primary_keys_and_return(result)

    return cast(F, wrapper)
