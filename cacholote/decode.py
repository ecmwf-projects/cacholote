"""Decode JSON data."""
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


import importlib
import json
from typing import Any, Dict, Union

from . import extra_encoders


def import_object(fully_qualified_name: str) -> Any:
    """Import python objects defined by fully qualified names (``'module:qualname'``)."""
    # FIXME: apply exclude/include-rules to ``fully_qualified_name``
    if ":" not in fully_qualified_name:
        raise ValueError(f"{fully_qualified_name!r} not in the form 'module:qualname'")
    module_name, _, object_name = fully_qualified_name.partition(":")
    obj = importlib.import_module(module_name)
    for attr_name in object_name.split("."):
        obj = getattr(obj, attr_name)
    return obj


def object_hook(obj: Dict[str, Any]) -> Any:
    """Decode deserialized JSON data (``dict``)."""
    if obj.get("type") == "python_object" and "fully_qualified_name" in obj:
        return import_object(obj["fully_qualified_name"])

    if obj.get("type") == "python_call" and "callable" in obj:
        if callable(obj["callable"]):
            func = obj["callable"]
        else:
            func = import_object(obj["callable"])
        args = obj.get("args", ())
        kwargs = obj.get("kwargs", {})
        return func(*args, **kwargs)

    if {"tmp:open_kwargs", "tmp:storage_options"} <= set(obj):
        fs, urlpath = extra_encoders._get_fs_and_urlpath_to_decode(obj)
        return fs.open(urlpath)

    if {"xarray:open_kwargs", "xarray:storage_options"} <= set(obj):
        return extra_encoders.decode_xr_dataset(obj)

    return obj


def loads(obj: Union[str, bytes, bytearray], **kwargs: Any) -> Any:
    """Decode serialized JSON data to a python object.

    Parameters
    ----------
    obj: str, bytes, bytearray
        Serialized JSON data.
    **kwargs: Any
        Keyword arguments for ``json.loads``

    Returns
    -------
    Any
    """
    return json.loads(obj, object_hook=object_hook, **kwargs)
