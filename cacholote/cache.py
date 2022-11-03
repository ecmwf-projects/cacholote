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

from . import clean, config, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])

LAST_PRIMARY_KEYS: Dict[str, Any] = {}


def _update_last_primary_keys_and_return(cache_entry_or_result: Any) -> Any:
    if not isinstance(cache_entry_or_result, config.CacheEntry):
        LAST_PRIMARY_KEYS.clear()
        return cache_entry_or_result

    LAST_PRIMARY_KEYS.update(cache_entry_or_result._primary_keys)
    return decode.loads(cache_entry_or_result._result_as_string)


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
        if not config.SETTINGS["use_cache"]:
            result = func(*args, **kwargs)
            return _update_last_primary_keys_and_return(result)

        try:
            hexdigest = hexdigestify_python_call(
                func,
                *args,
                **kwargs,
            )
        except encode.EncodeError as ex:
            warnings.warn(f"can NOT encode python call: {ex!r}", UserWarning)
            result = func(*args, **kwargs)
            return _update_last_primary_keys_and_return(result)

        # Filters
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
                    session.refresh(cache_entry)
                    while cache_entry.result == "__locked__":
                        session.refresh(cache_entry)
                        warnings.warn(
                            f"can NOT proceed until {cache_entry!r} is unlocked."
                        )
                        time.sleep(1)
                    result = _update_last_primary_keys_and_return(cache_entry)
                    cache_entry.counter += 1
                    session.commit()
                    return result
                except decode.DecodeError as ex:
                    # Something wrong, e.g. cached files are corrupted
                    warnings.warn(str(ex), UserWarning)
                    clean.delete_cache_entry(session, cache_entry)

            # Not in the cache: Lock
            cache_entry = config.CacheEntry(
                key=hexdigest,
                expiration=config.SETTINGS["expiration"],
                result="__locked__",
            )
            session.add(cache_entry)
            session.commit()

            # Compute result
            result = func(*args, **kwargs)
            try:
                cache_entry.result = json.loads(encode.dumps(result))
                session.commit()
                return _update_last_primary_keys_and_return(cache_entry)
            except Exception as ex:
                session.delete(cache_entry)
                session.commit()
                if not isinstance(ex, encode.EncodeError):
                    raise ex
                warnings.warn(f"can NOT encode output: {ex!r}", UserWarning)
                return _update_last_primary_keys_and_return(result)

    return cast(F, wrapper)
