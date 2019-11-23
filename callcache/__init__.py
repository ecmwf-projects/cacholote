import datetime
import functools
import hashlib
import inspect
import json
import operator
import uuid

import xarray as xr

from .decode import object_hook, loads


def uniquify_arguments(callable_, *args, **kwargs):
    try:
        bound_arguments = inspect.signature(callable_).bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        args, kwargs = bound_arguments.args, bound_arguments.kwargs
    except:
        pass
    sorted_kwargs = sorted(kwargs.items(), key=operator.itemgetter(0))
    return args, dict(sorted_kwargs)


def inspect_fully_qualified_name(obj):
    """Return the fully qualified name of a python object."""
    module = inspect.getmodule(obj)
    return f"{module.__name__}:{obj.__qualname__}"


def uniquify_call_signature(callable_, *args, **kwargs):
    if isinstance(callable_, str):
        fully_qualified_name = callable_
    else:
        fully_qualified_name = inspect_fully_qualified_name(callable_)
    args, kwargs = uniquify_arguments(callable_, *args, **kwargs)
    call_signature = {"callable": fully_qualified_name}
    if args:
        call_signature["args"] = args
    if kwargs:
        call_signature["kwargs"] = kwargs
    return call_signature


def filecache_default(o):
    if isinstance(o, datetime.datetime):
        return uniquify_call_signature("datetime:datetime.fromisoformat", o.isoformat())
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
        call_signature = {"type": "python_call"}
        call_signature.update(uniquify_call_signature(xr.open_dataset, path))
        return call_signature
    elif callable(o):
        object_json = {
            "type": "python_object",
            "fully_qualified_name": inspect_fully_qualified_name(o),
        }
        return object_json
    raise TypeError("can't encode object")


def jsonify(obj):
    return json.dumps(obj, separators=(",", ":"), default=filecache_default)


def uniquify_call_signature_json(callable_, *args, **kwargs):
    unique_call_signature = uniquify_call_signature(callable_, *args, **kwargs)
    return jsonify(unique_call_signature)


def hexdigestify(text):
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def uniquify_call_signatures(callable_, *args, **kwargs):
    call_signature = uniquify_call_signature(callable_, *args, **kwargs)
    call_signature_json = jsonify(call_signature)
    return call_signature, call_signature_json, hexdigestify(call_signature_json)


def make_call_signature_json(func, args=(), kwargs={}):
    call_simple = {"type": "python_call", "callable": func}
    if args:
        call_simple["args"] = args
    if kwargs:
        call_simple["kwargs"] = kwargs
    return json.dumps(call_simple, default=filecache_default)


CACHE = {}


def invalidate_entry(hexdigest):
    return CACHE.pop(hexdigest, None)


def cacheable(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            call_siganture_json = make_call_signature_json(func, args, kwargs)
        except TypeError:
            print(f"UNCACHEABLE: {func} {args} {kwargs}")
            return func(*args, **kwargs)

        hexdigest = hexdigestify(call_siganture_json)
        if hexdigest not in CACHE:
            print(f"MISS: {hexdigest} {call_siganture_json}")
            result = func(*args, **kwargs)
            CACHE[hexdigest] = (call_siganture_json, result)
        else:
            print(f"HIT: {hexdigest}")
        return CACHE[hexdigest][1]

    return wrapper
