import importlib
import json


def import_object(fully_qualified_name: str):
    # FIXME: apply exclude/include-rules to `fully_qualified_name`
    if ":" not in fully_qualified_name:
        raise ValueError(f"{fully_qualified_name} not in the form 'module:qualname'")
    module_name, _, object_name = fully_qualified_name.partition(":")
    obj = importlib.import_module(module_name)
    for attr_name in object_name.split("."):
        obj = getattr(obj, attr_name)
    return obj


def object_hook(obj: dict):
    if obj.get("type") == "python_object" and "fully_qualified_name" in obj:
        obj = import_object(obj["fully_qualified_name"])
    elif obj.get("type") == "python_call" and "callable" in obj:
        if callable(obj["callable"]):
            func = obj["callable"]
        else:
            func = import_object(obj["callable"])
        args = obj.get("args", ())
        kwargs = obj.get("kwargs", {})
        obj = func(*args, **kwargs)
    return obj


def loads(obj, **kwargs):
    return json.loads(obj, object_hook=object_hook, **kwargs)
