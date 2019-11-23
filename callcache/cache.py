import functools
import hashlib

from .decode import loads
from .encode import dumps_python_call, dumps


CACHE = {}
CACHE_STATS = {"hit": 0, "miss": 0, "uncacheable_input": 0, "uncacheable_output": 0}


def hexdigestify(text):
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def cacheable(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            call_siganture_json = dumps_python_call(func, *args, **kwargs)
        except TypeError:
            print(f"UNCACHEABLE INPUT: {func} {args} {kwargs}")
            CACHE_STATS["uncacheable_input"] += 1
            return func(*args, **kwargs)

        hexdigest = hexdigestify(call_siganture_json)
        if hexdigest not in CACHE:
            result = func(*args, **kwargs)
            try:
                cached = dumps(result)
                print(f"MISS: {hexdigest} {call_siganture_json}")
                CACHE_STATS["miss"] += 1
                CACHE[hexdigest] = cached
            except Exception:
                print(f"UNCACHEABLE OUTPUT: {func} {args} {kwargs}")
                CACHE_STATS["uncacheable_output"] += 1
                return result
        else:
            print(f"HIT: {hexdigest}")
            CACHE_STATS["hit"] += 1
        return loads(CACHE[hexdigest])

    return wrapper
