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


def _append_info(cached: str) -> str:
    cached_dict = json.loads(cached)
    info = cached_dict.get("__info__", {})
    info["atime"] = str(datetime.datetime.now())
    info["count"] = info.get("hits", 0) + 1
    cached_dict["__info__"] = info
    return encode.dumps(cached_dict)


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

        cache_store = config.SETTINGS["cache_store"]
        try:
            # Use try/except to update stats correctly
            cached = cache_store[hexdigest]
        except KeyError:
            # +1 miss
            pass
        else:
            # +1 hit
            try:
                res = decode.loads(cached)
            except Exception as ex:
                # Something wrong, e.g. cached files are corrupted
                # Warn and recreate cache value
                warnings.warn(str(ex), UserWarning)

                # Clean-up
                cached_dict = json.loads(cached)
                if "file:local_path" in cached_dict:
                    fs, urlpath = extra_encoders._get_fs_and_urlpath_to_decode(
                        cached_dict
                    )
                    if fs.exists(urlpath):
                        try:
                            fs.rm(
                                urlpath,
                                recursive=cached_dict["type"] == "application/vnd+zarr",
                            )
                        except NotImplemented:
                            pass
                del cache_store[hexdigest]
            else:
                if config.SETTINGS["append_info"]:
                    cache_store[hexdigest] = _append_info(cached)
                return res

        result = func(*args, **kwargs)
        try:
            cached = encode.dumps(result)
        except encode.EncodeError:
            warnings.warn("can NOT encode output", UserWarning)
            return result

        if config.SETTINGS["append_info"]:
            cached = _append_info(cached)
        cache_store[hexdigest] = cached
        return decode.loads(cached)

    return cast(F, wrapper)
