import os.path
import pytest

pd = pytest.importorskip("pandas")  # noqa
xr = pytest.importorskip("xarray")  # noqa

from callcache import decode
from callcache import encode
from callcache import extra_encoders


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


def test_dictify_pd_dataframe(tmpdir):
    cache_root = str(tmpdir)
    data_name = "72415954bda87fdd2bb934dd93aa137b7a78bcf580194b3fa90b81f7.csv"
    data_path = os.path.join(cache_root, data_name)
    data = pd.DataFrame({"data": [0]})
    expected = {
        "type": "python_call",
        "callable": "pandas.io.parsers:_make_parser_function.<locals>.parser_f",
        "args": (data_path,),
    }
    res = extra_encoders.dictify_pd_dataframe(data, cache_root)
    assert res == expected

    data_name1 = '91aae6e1ff77e0cde8413be9226a3453162d06616419106b200ee94d.csv'
    data_path1 = os.path.join(cache_root, data_name1)
    expected = {
        "type": "python_call",
        "callable": "pandas.io.parsers:_make_parser_function.<locals>.parser_f",
        "args": (data_path1,),
    }
    data = pd.read_csv(data_path)
    res = extra_encoders.dictify_pd_dataframe(data, cache_root)
    assert res == expected


def test_roundtrip():
    data = xr.Dataset(data_vars={"data": [0]})
    assert decode.loads(encode.dumps(data)).identical(data)
