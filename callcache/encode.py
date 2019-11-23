import datetime
import collections.abc
import inspect
import json


def inspect_fully_qualified_name(obj):
    """Return the fully qualified name of a python object."""
    module = inspect.getmodule(obj)
    return f"{module.__name__}:{obj.__qualname__}"


def dictify_python_object(obj):
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


def dictify_python_call(func, *args, **kwargs):
    callable_simple = dictify_python_object(func)
    python_call_simple = {"type": "python_call", "callable": callable_simple}
    if args:
        python_call_simple["args"] = args
    if kwargs:
        python_call_simple["kwargs"] = kwargs
    return python_call_simple


def dictify_datetime(o):
    # Work around "AttributeError: 'NoneType' object has no attribute '__name__'"
    return dictify_python_call("datetime:datetime.fromisoformat", o.isoformat())


def dictify_date(o):
    return dictify_python_call("datetime:date.fromisoformat", o.isoformat())


def dictify_timedelta(o):
    return dictify_python_call("datetime:timedelta", o.days, o.seconds, o.microseconds)


def dictify_bytes(o):
    return dictify_python_call(bytes, list(o))


FILECACHE_ENCODERS = {
    datetime.datetime: dictify_datetime,
    datetime.date: dictify_date,
    datetime.timedelta: dictify_timedelta,
    bytes: dictify_bytes,
    collections.abc.Callable: dictify_python_object,
}


def filecache_default(o, encoders=FILECACHE_ENCODERS):
    for test, encoder in encoders.items():
        if isinstance(o, test):
            return encoder(o)
    raise TypeError("can't encode object")


def dumps(obj, separators=(",", ":"), **kwargs):
    return json.dumps(obj, separators=separators, default=filecache_default, **kwargs)
