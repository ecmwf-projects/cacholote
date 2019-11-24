import os.path
import pytest

xr = pytest.importorskip("xarray")  # noqa

from callcache import cache
from callcache import decode
from callcache import encode
from callcache import extra_encoders


def func(a):
    return a


def test_dictify_xr_dataset(tmpdir):
    cache_root = str(tmpdir)
    data_name = "a6c9d74e563abf0d5527a1c3bad999bde7d10ab0e66cfe33c2969098.nc"
    data_path = os.path.join(cache_root, data_name)
    data = xr.Dataset(data_vars={"data": [0]})
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path,),
    }
    res = extra_encoders.dictify_xr_dataset(data, cache_root)
    assert res == expected

    data_name1 = "58249f0d51d51f386c8180e2b6ca2cb2907cc15c26852efc9ecf2be0.nc"
    data_path1 = os.path.join(cache_root, data_name1)
    expected = {
        "type": "python_call",
        "callable": "xarray.backends.api:open_dataset",
        "args": (data_path1,),
    }
    data = xr.open_dataset(data_path)
    res = extra_encoders.dictify_xr_dataset(data, cache_root)
    assert res == expected

    # skip saving a file already present on disk
    data = xr.open_dataset(data_path1)
    res = extra_encoders.dictify_xr_dataset(data, cache_root)
    assert res == expected

    data["data"].values[0] = 1
    with pytest.raises(RuntimeError):
        extra_encoders.dictify_xr_dataset(data, cache_root, data_name1)


def test_roundtrip(tmpdir):
    cache_root = str(tmpdir)
    data = xr.Dataset(data_vars={"data": [0]})
    assert decode.loads(encode.dumps(data, cache_root=cache_root)).identical(data)


def test_caceable(tmpdir):
    cfunc = cache.cacheable(cache_root=str(tmpdir))(func)
    cache.CACHE.clear()

    data = xr.Dataset(data_vars={"data": [0]})
    res = cfunc(data)
    assert res.identical(data)
    assert cache.CACHE.stats["miss"] == 1

    # FIXME: why do we get two misses?
    res = cfunc(data)
    assert res.identical(data)
    assert cache.CACHE.stats["miss"] == 2

    res = cfunc(data)
    assert res.identical(data)
    assert cache.CACHE.stats["hit"] == 1
