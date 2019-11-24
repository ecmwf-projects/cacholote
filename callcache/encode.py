import binascii
import collections.abc
import datetime
import functools
import inspect
import json
import logging
import operator
import pickle


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
    kwargs = dict(sorted(kwargs.items(), key=operator.itemgetter(0)))
    callable_fqn = dictify_python_object(func)["fully_qualified_name"]
    python_call_simple = {"type": "python_call", "callable": callable_fqn}
    if args:
        python_call_simple["args"] = args
    if kwargs:
        python_call_simple["kwargs"] = kwargs
    return python_call_simple


def dictify_datetime(o, **kwargs):
    # Work around "AttributeError: 'NoneType' object has no attribute '__name__'"
    return dictify_python_call("datetime:datetime.fromisoformat", o.isoformat())


def dictify_date(o, **kwargs):
    return dictify_python_call("datetime:date.fromisoformat", o.isoformat())


def dictify_timedelta(o, **kwargs):
    return dictify_python_call("datetime:timedelta", o.days, o.seconds, o.microseconds)


def dictify_bytes(o, **kwargs):
    ascii_decoded = binascii.b2a_base64(o).decode("ascii")
    return dictify_python_call(binascii.a2b_base64, ascii_decoded)


def dictify_pickable(o, **kwargs):
    return dictify_python_call(pickle.loads, pickle.dumps(o))


FILECACHE_ENCODERS = [
    (object, dictify_pickable),
    (collections.abc.Callable, dictify_python_object),
    (bytes, dictify_bytes),
    (datetime.date, dictify_date),
    (datetime.datetime, dictify_datetime),
    (datetime.timedelta, dictify_timedelta),
]


def filecache_default(o, cache_root='.', errors="warn", encoders=FILECACHE_ENCODERS):
    for test, encoder in reversed(encoders):
        if isinstance(o, test):
            try:
                return encoder(o, cache_root=cache_root)
            except Exception:
                if errors == "warn":
                    logging.exception("can't pickle object")
    raise TypeError("can't encode object")


def dumps(obj, separators=(",", ":"), cache_root='.', **kwargs):
    default = functools.partial(filecache_default, cache_root=cache_root)
    return json.dumps(obj, separators=separators, default=default, **kwargs)


def dumps_python_call(func, *args, dumps_cache_root='.', **kwargs):
    python_call = dictify_python_call(func, *args, **kwargs)
    return dumps(python_call, cache_root=dumps_cache_root)
