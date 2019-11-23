import importlib
import json


def import_object(fully_qualified_name):
    # FIXME: apply exclude/include-rules to `fully_qualified_name`
    if ":" not in fully_qualified_name:
        raise ValueError(f"{fully_qualified_name} not in the form 'module:qualname'")
    module_name, _, object_name = fully_qualified_name.partition(":")
    obj = importlib.import_module(module_name)
    for attr_name in object_name.split("."):
        obj = getattr(obj, attr_name)
    return obj


def call_object_hook(o):
    if o.get("type") == "python_object" and "fully_qualified_name" in o:
        o = import_object(o["fully_qualified_name"])
    elif o.get("type") == "python_call" and "callable" in o:
        o = o["callable"](*o.get("args", ()), **o.get("kwargs", {}))
    return o


def loads(obj, **kwargs):
    return json.loads(obj, object_hook=call_object_hook, **kwargs)
