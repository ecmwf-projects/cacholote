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


import functools
import hashlib
import warnings
from typing import Any, Callable, TypeVar, cast

from . import decode, encode
from .config import SETTINGS

F = TypeVar("F", bound=Callable[..., Any])


def hexdigestify(text: str) -> str:
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def cacheable(func: F) -> F:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            call_json = encode.dumps_python_call(
                func,
                *args,
                **kwargs,
            )
        except encode.EncodeError:
            warnings.warn("bad input", UserWarning)
            return func(*args, **kwargs)

        hexdigest = hexdigestify(call_json)
        cache_store = SETTINGS["cache_store"]
        try:
            return decode.loads(cache_store[hexdigest])
        except Exception as ex:
            if hexdigest in cache_store:
                warnings.warn(str(ex), UserWarning)

        result = func(*args, **kwargs)
        try:
            cached = encode.dumps(result)
        except encode.EncodeError:
            warnings.warn("bad output", UserWarning)
            return result

        cache_store[hexdigest] = cached
        return decode.loads(cached)

    return cast(F, wrapper)
