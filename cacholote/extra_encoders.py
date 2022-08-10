import pickle
from typing import Any, Dict, Union

from . import encode

try:
    import xarray as xr

    HAS_XARRAY = True
except ImportError:
    HAS_XARRAY = False


def dictify_xr_object(obj: Union["xr.DataArray", "xr.Dataset"]) -> Dict[str, Any]:
    # .compute() beacause when pickling an object opened from a NetCDF file,
    # the pickle file will contain a reference to the file on disk.
    return encode.dictify_python_call(pickle.loads, pickle.dumps(obj.compute()))


def register_all() -> None:
    if HAS_XARRAY:
        encode.FILECACHE_ENCODERS.append((xr.DataArray, dictify_xr_object))
        encode.FILECACHE_ENCODERS.append((xr.Dataset, dictify_xr_object))
