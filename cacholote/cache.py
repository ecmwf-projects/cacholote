"""Public decorator."""
# Copyright 2019, B-Open Solutions srl.
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
import warnings
from typing import Any, Callable, TypeVar, Union, cast

import sqlalchemy
import sqlalchemy.exc

from . import config, decode, encode, extra_encoders, utils

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
        try:
            hexdigest = hexdigestify_python_call(
                func,
                *args,
                **kwargs,
            )
        except encode.EncodeError:
            warnings.warn("can NOT encode python call", UserWarning)
            return func(*args, **kwargs)

        stmt = sqlalchemy.select(config.CacheEntry).where(
            config.CacheEntry.key == hexdigest
        )
        with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
            try:
                # Get result from cache
                cache_entry = session.scalars(stmt).one()
            except sqlalchemy.exc.NoResultFound:
                # Not in the cache
                pass
            else:
                # Attempt to decode from cache
                try:
                    result = decode.loads(cache_entry.value)
                except Exception as ex:
                    # Something wrong, e.g. cached files are corrupted
                    warnings.warn(str(ex), UserWarning)

                    # Delete cache file
                    cached_dict = json.loads(cache_entry.value)
                    if (
                        isinstance(cached_dict, dict)
                        and "file:local_path" in cached_dict
                    ):
                        fs, urlpath = extra_encoders._get_fs_and_urlpath_to_decode(
                            cached_dict, validate=False
                        )
                        if fs.exists(urlpath):
                            fs.rm(urlpath, recursive=True)

                    # Remove cache entry
                    session.delete(cache_entry)
                    session.commit()
                else:
                    # Update stats and return cached result
                    cache_entry.timestamp = datetime.datetime.now()
                    cache_entry.count += 1
                    session.commit()
                    return result

            # Compute result
            result = func(*args, **kwargs)
            try:
                value = encode.dumps(result)
            except encode.EncodeError:
                warnings.warn("can NOT encode output", UserWarning)
                return result

            # Add cache entry and return result computed
            cache_entry = config.CacheEntry(
                key=hexdigest, value=value, timestamp=datetime.datetime.now(), count=1
            )
            session.add(cache_entry)
            session.commit()
            return decode.loads(value)

    return cast(F, wrapper)
