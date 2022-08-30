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


import binascii
import collections.abc
import datetime
import inspect
import json
import operator
import pathlib
import pickle
import warnings
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from . import config


def inspect_fully_qualified_name(obj: Callable[..., Any]) -> str:
    """Return the fully qualified name of a python object."""
    module = inspect.getmodule(obj)
    if module is None:
        raise ValueError(f"can't getmodule for {obj!r}")
    return f"{module.__name__}:{obj.__qualname__}"


def dictify_python_object(obj: Union[str, Callable[..., Any]]) -> Dict[str, str]:
    if isinstance(obj, str):
        # NOTE: a stricter test would be decode.import_object(obj)
        if ":" not in obj:
            raise ValueError(f"{obj} not in the form 'module:qualname'")
        fully_qualified_name = obj
    else:
        fully_qualified_name = inspect_fully_qualified_name(obj)
    object_simple = {
        "type": "python_object",
        "fully_qualified_name": fully_qualified_name,
    }
    return object_simple


def dictify_python_call(
    func: Union[str, Callable[..., Any]],
    *args: Any,
    **kwargs: Any,
) -> Dict[str, Any]:
    kwargs = dict(sorted(kwargs.items(), key=operator.itemgetter(0)))
    callable_fqn = dictify_python_object(func)["fully_qualified_name"]
    python_call_simple: Dict[str, Any] = {
        "type": "python_call",
        "callable": callable_fqn,
    }
    if args:
        python_call_simple["args"] = args
    if kwargs:
        python_call_simple["kwargs"] = kwargs
    return python_call_simple


def dictify_xarray_asset(
    checksum: str,
    size: int,
    open_kwargs: Dict[str, Any] = {},
    storage_options: Dict[str, Any] = {},
) -> Dict[str, Any]:

    filetype = config.SETTINGS["xarray_cache_type"]
    extension = config.EXTENSIONS[filetype]

    return {
        "type": filetype,
        "href": f"./{checksum}{extension}",
        "file:checksum": checksum,
        "file:size": size,
        "file:local_path": str(
            pathlib.Path(config.SETTINGS["directory"]).absolute()
            / f"{checksum}{extension}"
        ),
        "xarray:open_kwargs": open_kwargs,
        "xarray:storage_options": storage_options,
    }


def dictify_datetime(obj: datetime.datetime) -> Dict[str, Any]:
    # Work around "AttributeError: 'NoneType' object has no attribute '__name__'"
    return dictify_python_call("datetime:datetime.fromisoformat", obj.isoformat())


def dictify_date(obj: datetime.date) -> Dict[str, Any]:
    return dictify_python_call("datetime:date.fromisoformat", obj.isoformat())


def dictify_timedelta(obj: datetime.timedelta) -> Dict[str, Any]:
    return dictify_python_call(
        "datetime:timedelta", obj.days, obj.seconds, obj.microseconds
    )


def dictify_bytes(obj: bytes) -> Dict[str, Any]:
    ascii_decoded = binascii.b2a_base64(obj).decode("ascii")
    return dictify_python_call(binascii.a2b_base64, ascii_decoded)


def dictify_pickable(obj: Any) -> Dict[str, Any]:
    return dictify_python_call(pickle.loads, pickle.dumps(obj))


FILECACHE_ENCODERS: List[Tuple[Any, Callable[..., Any]]] = [
    (object, dictify_pickable),
    (collections.abc.Callable, dictify_python_object),
    (bytes, dictify_bytes),
    (datetime.date, dictify_date),
    (datetime.datetime, dictify_datetime),
    (datetime.timedelta, dictify_timedelta),
]


class EncodeError(Exception):
    pass


def filecache_default(
    obj: Any,
    encoders: List[Tuple[Any, Callable[..., Any]]] = FILECACHE_ENCODERS,
) -> Any:
    for type_, encoder in reversed(encoders):
        if isinstance(obj, type_):
            try:
                return encoder(obj)
            except Exception as ex:
                warnings.warn(f"{encoder!r} did not work: {ex!r}")
    raise EncodeError("can't encode object")


def dumps(
    obj: Any,
    separators: Optional[Tuple[str, str]] = (",", ":"),
    **kwargs: Any,
) -> str:
    default = filecache_default
    return json.dumps(obj, separators=separators, default=default, **kwargs)


def dumps_python_call(
    func: Union[str, Callable[..., Any]],
    *args: Any,
    **kwargs: Any,
) -> str:
    python_call = dictify_python_call(func, *args, **kwargs)
    return dumps(python_call)
