import binascii
import collections.abc
import datetime
import functools
import inspect
import json
import logging
import operator
import pickle
import typing as T


def inspect_fully_qualified_name(o) -> str:
    """Return the fully qualified name of a python object."""
    module = inspect.getmodule(o)
    if module is None:
        raise ValueError(f"can't getmodule for {o!r}")
    return f"{module.__name__}:{o.__qualname__}"


def dictify_python_object(o) -> T.Dict[str, str]:
    if isinstance(o, str):
        # NOTE: a stricter test would be decode.import_object(obj)
        if ":" not in o:
            raise ValueError(f"{o} not in the form 'module:qualname'")
        fully_qualified_name = o
    else:
        fully_qualified_name = inspect_fully_qualified_name(o)
    object_simple = {
        "type": "python_object",
        "fully_qualified_name": fully_qualified_name,
    }
    return object_simple


def dictify_python_call(
    func: T.Union[collections.abc.Callable, str],
    *args,
    _callable_version: str = None,
    **kwargs,
) -> T.Dict[str, T.Any]:
    kwargs = dict(sorted(kwargs.items(), key=operator.itemgetter(0)))
    callable_fqn = dictify_python_object(func)["fully_qualified_name"]
    python_call_simple: T.Dict[str, T.Any] = {
        "type": "python_call",
        "callable": callable_fqn,
    }
    if _callable_version is not None:
        python_call_simple["version"] = _callable_version
    if args:
        python_call_simple["args"] = args
    if kwargs:
        python_call_simple["kwargs"] = kwargs
    return python_call_simple


def dictify_datetime(o: datetime.datetime, **kwargs) -> T.Dict[str, T.Any]:
    # Work around "AttributeError: 'NoneType' object has no attribute '__name__'"
    return dictify_python_call("datetime:datetime.fromisoformat", o.isoformat())


def dictify_date(o: datetime.date, **kwargs) -> T.Dict[str, T.Any]:
    return dictify_python_call("datetime:date.fromisoformat", o.isoformat())


def dictify_timedelta(o: datetime.timedelta, **kwargs) -> T.Dict[str, T.Any]:
    return dictify_python_call("datetime:timedelta", o.days, o.seconds, o.microseconds)


def dictify_bytes(o: bytes, **kwargs) -> T.Dict[str, T.Any]:
    ascii_decoded = binascii.b2a_base64(o).decode("ascii")
    return dictify_python_call(binascii.a2b_base64, ascii_decoded)


def dictify_pickable(o, **kwargs) -> T.Dict[str, T.Any]:
    return dictify_python_call(pickle.loads, pickle.dumps(o))


FILECACHE_ENCODERS = [
    (object, dictify_pickable),
    (collections.abc.Callable, dictify_python_object),
    (bytes, dictify_bytes),
    (datetime.date, dictify_date),
    (datetime.datetime, dictify_datetime),
    (datetime.timedelta, dictify_timedelta),
]


def filecache_default(o, filecache_root=".", encoders=FILECACHE_ENCODERS):
    for type_, encoder in reversed(encoders):
        if isinstance(o, type_):
            try:
                return encoder(o, filecache_root=filecache_root)
            except Exception:
                logging.exception("can't pickle object")
    raise TypeError("can't encode object")


def dumps(obj, separators=(",", ":"), filecache_root=".", **kwargs) -> str:
    default = functools.partial(filecache_default, filecache_root=filecache_root)
    return json.dumps(obj, separators=separators, default=default, **kwargs)


def dumps_python_call(
    func: T.Union[T.Callable, str], *args, _filecache_root=".", **kwargs
) -> str:
    python_call = dictify_python_call(func, *args, **kwargs)
    return dumps(python_call, filecache_root=_filecache_root)
