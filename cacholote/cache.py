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
import logging
import warnings
from typing import Any, Callable, TypeVar, Union, cast

import sqlalchemy
import sqlalchemy.exc

from . import clean, config, decode, encode, utils

F = TypeVar("F", bound=Callable[..., Any])


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
            return func(*args, **kwargs)

        try:
            hexdigest = hexdigestify_python_call(
                func,
                *args,
                **kwargs,
            )
        except encode.EncodeError:
            warnings.warn("can NOT encode python call", UserWarning)
            return func(*args, **kwargs)

        # Database query settings
        queries = (config.CacheEntry.key, config.CacheEntry.expiration)
        sorters = (config.CacheEntry.timestamp.desc(),)
        filters = [
            config.CacheEntry.key == hexdigest,
            config.CacheEntry.expiration > datetime.datetime.now(),
        ]
        expiration = config.SETTINGS["expiration"]
        if expiration:
            # If expiration is provided, only get entries with matching expiration
            expiration = datetime.datetime.fromisoformat(expiration)
            filters.append(config.CacheEntry.expiration == expiration)
        else:
            expiration = datetime.datetime.max

        with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
            for (k, e) in session.query(*queries).filter(*filters).order_by(*sorters):
                try:
                    cache_entry = (
                        session.query(config.CacheEntry).filter(
                            config.CacheEntry.key == k,
                            config.CacheEntry.expiration == e,
                        )
                    ).one()
                    cache_entry.counter += 1
                    logging.info(cache_entry)
                    session.commit()
                    return cache_entry.result
                except decode.DecodeError as ex:
                    # Something wrong, e.g. cached files are corrupted
                    warnings.warn(str(ex), UserWarning)
                    clean.delete_entry(k, e)

            # Not in the cache: Compute result
            result = func(*args, **kwargs)
            try:
                cache_entry = config.CacheEntry(
                    key=hexdigest, expiration=expiration, result=result
                )
                session.add(cache_entry)
                logging.info(cache_entry)
                session.commit()
                return cache_entry.result
            except sqlalchemy.exc.StatementError:
                warnings.warn("can NOT encode output", UserWarning)
                return result

    return cast(F, wrapper)
