import datetime
import inspect
import json
import uuid

import xarray as xr


def inspect_fully_qualified_name(obj):
    """Return the fully qualified name of a python object."""
    module = inspect.getmodule(obj)
    return f"{module.__name__}:{obj.__qualname__}"


def dictify_python_object(obj):
    object_simple = {
        "type": "python_object",
        "fully_qualified_name": inspect_fully_qualified_name(obj),
    }
    return object_simple


def dictify_python_call(func, *args, **kwargs):
    callable_simple = dictify_python_object(func)
    python_call_simple = {"type": "python_call", "callable": callable_simple}
    if args:
        python_call_simple["args"] = args
    if kwargs:
        python_call_simple["kwargs"] = kwargs
    return python_call_simple


def filecache_default(o):
    if isinstance(o, datetime.datetime):
        return dictify_python_call(datetime.datetime.fromisoformat, o.isoformat())
    elif isinstance(o, xr.Dataset):
        try:
            path = o.encoding["source"]
            orig = xr.open_dataset(path)
            if not o.identical(orig):
                path = None
        except:
            path = None
        if path is None:
            path = f"./{uuid.uuid4()}.nc"
            o.to_netcdf(path)
        return dictify_python_call(xr.open_dataset, path)
    elif callable(o):
        return dictify_python_object(o)
    raise TypeError("can't encode object")


def dumps(obj, separators=(",", ":"), **kwargs):
    return json.dumps(obj, separators=separators, default=filecache_default, **kwargs)
