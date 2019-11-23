import functools
import hashlib
import inspect
import json
import operator


from .decode import object_hook, loads
from .encode import filecache_default, dumps

from . import encode

__all__ = ["dumps", "filecache_default", "loads", "object_hook"]


def uniquify_arguments(callable_, *args, **kwargs):
    try:
        bound_arguments = inspect.signature(callable_).bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        args, kwargs = bound_arguments.args, bound_arguments.kwargs
    except:
        pass
    sorted_kwargs = sorted(kwargs.items(), key=operator.itemgetter(0))
    return args, dict(sorted_kwargs)


def uniquify_call_signature(callable_, *args, **kwargs):
    if isinstance(callable_, str):
        fully_qualified_name = callable_
    else:
        fully_qualified_name = encode.inspect_fully_qualified_name(callable_)
    args, kwargs = uniquify_arguments(callable_, *args, **kwargs)
    call_signature = {"callable": fully_qualified_name}
    if args:
        call_signature["args"] = args
    if kwargs:
        call_signature["kwargs"] = kwargs
    return call_signature


def uniquify_call_signature_json(callable_, *args, **kwargs):
    unique_call_signature = uniquify_call_signature(callable_, *args, **kwargs)
    return encode.dumps(unique_call_signature)


def hexdigestify(text):
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def uniquify_call_signatures(callable_, *args, **kwargs):
    call_signature = uniquify_call_signature(callable_, *args, **kwargs)
    call_signature_json = encode.dumps(call_signature)
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
