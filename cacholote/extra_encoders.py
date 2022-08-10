from typing import Any, Dict

from . import encode

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False


def dictify_xr_dataarray(obj: "xr.DataArray") -> Dict[str, Any]:
    return encode.dictify_python_call(
        "xarray:DataArray.from_dict", obj.to_dict(data=True, encoding=True)
    )


def dictify_xr_dataset(obj: "xr.Dataset") -> Dict[str, Any]:
    return encode.dictify_python_call(
        "xarray:Dataset.from_dict", obj.to_dict(data=True, encoding=True)
    )


def register_all() -> None:
    if HAS_XARRAY:
        encode.FILECACHE_ENCODERS.append((xr.DataArray, dictify_xr_dataarray))
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_dataset))
