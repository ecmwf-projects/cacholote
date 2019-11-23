import inspect
import operator

from .cache import cacheable
from .decode import object_hook, loads
from .encode import filecache_default, dumps


__all__ = ["cacheable", "dumps", "filecache_default", "loads", "object_hook"]


def uniquify_arguments(callable_, *args, **kwargs):
    try:
        bound_arguments = inspect.signature(callable_).bind(*args, **kwargs)
        bound_arguments.apply_defaults()
        args, kwargs = bound_arguments.args, bound_arguments.kwargs
    except:
        pass
    sorted_kwargs = sorted(kwargs.items(), key=operator.itemgetter(0))
    return args, dict(sorted_kwargs)
