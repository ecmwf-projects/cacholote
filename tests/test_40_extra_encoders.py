from typing import Any, TypeVar

import pytest

from cacholote import cache, config, decode, encode

xr = pytest.importorskip("xarray")
T = TypeVar("T")


def func(a: T) -> T:
    return a


DATAARRAY = xr.DataArray([0], name="foo")
XARRAY_OBJECTS = [DATAARRAY, DATAARRAY.to_dataset()]


@pytest.mark.parametrize("obj", XARRAY_OBJECTS)
def test_roundtrip(obj: Any) -> None:
    date_json = encode.dumps(obj)
    assert decode.loads(date_json).identical(obj)


@pytest.mark.parametrize("obj", XARRAY_OBJECTS)
def test_cacheable(obj: Any) -> None:
    cfunc = cache.cacheable(func)

    res = cfunc(obj)
    assert res.identical(obj)
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    res = cfunc(obj)
    assert res.identical(obj)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
