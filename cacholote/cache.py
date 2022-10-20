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

        with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
            try:
                # Get result from cache
                cache_entry = (
                    session.query(config.CacheEntry)
                    .filter(config.CacheEntry.key == hexdigest)
                    .one()
                )

                # Update stats and return cached result
                cache_entry.counter += 1
                return cache_entry.result
            except sqlalchemy.exc.NoResultFound:
                # Not in the cache
                pass
            except decode.DecodeError as ex:
                # Something wrong, e.g. cached files are corrupted
                warnings.warn(str(ex), UserWarning)

                # Delete cache file
                (cached_args,) = (
                    session.query(config.CacheEntry.result["args"])
                    .filter(config.CacheEntry.key == hexdigest)
                    .one()
                )
                if extra_encoders._are_file_args(*cached_args):
                    fs, urlpath = extra_encoders._get_fs_and_urlpath(*cached_args)
                    if fs.exists(urlpath):
                        recursive = cached_args[0]["type"] == "application/vnd+zarr"
                        fs.rm(urlpath, recursive=recursive)

                # Remove cache entry
                session.query(config.CacheEntry).filter(
                    config.CacheEntry.key == hexdigest
                ).delete()
            finally:
                session.commit()

            # Not in the cache: Compute result
            result = func(*args, **kwargs)
            try:
                cache_entry = config.CacheEntry(key=hexdigest, result=result)
                session.add(cache_entry)
                session.commit()
                return cache_entry.result
            except sqlalchemy.exc.StatementError:
                warnings.warn("can NOT encode output", UserWarning)
                return result

    return cast(F, wrapper)
