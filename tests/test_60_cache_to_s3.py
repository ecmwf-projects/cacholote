import os
import shlex
import subprocess
import time
import typing
from typing import Any, Dict

import fsspec
import fsspec.implementations.local
import pytest

from cacholote import cache, config

try:
    import xarray as xr
except ImportError:
    pass

requests = pytest.importorskip("requests")
pytest.importorskip("s3fs")


# TODO: See https://gist.github.com/michcio1234/7d72edc97bd751931aaf1952e4cb479c
# This is a workaround because moto.mock_s3 does not work.

PORT = 5555
ENDPOINT_URI = f"http://127.0.0.1:{PORT}/"


@typing.no_type_check
@pytest.fixture(scope="session")
def s3_base():
    """Run moto in server mode
    This starts a local S3 server which we'll test against.
    We must do this because if we try to use moto's s3_mock, problems with aiobotocore
    arise. See https://github.com/aio-libs/aiobotocore/issues/755.
    This and some other fixtures are taken from
    https://github.com/fsspec/s3fs/blob/main/s3fs/tests/test_s3fs.py
    """

    try:
        # should fail since we didn't start server yet
        r = requests.get(ENDPOINT_URI)
    except:  # noqa: E722
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(shlex.split(f"moto_server s3 -p {PORT}"))

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(ENDPOINT_URI)
            if r.ok:
                break
        except:  # noqa: E722
            pass
        timeout -= 0.1
        time.sleep(0.1)
    yield
    proc.terminate()
    proc.wait()


@typing.no_type_check
def get_boto3_client():
    from botocore.session import Session

    # NB: we use the sync botocore client for setup
    session = Session()
    return session.create_client("s3", endpoint_url=ENDPOINT_URI)


@typing.no_type_check
@pytest.fixture()
def s3_config(s3_base):
    """Yields properly configured S3FileSystem instance + test bucket name"""
    test_bucket_name = "test-bucket"
    client = get_boto3_client()
    client.create_bucket(
        Bucket=test_bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )

    s3 = fsspec.filesystem("s3", client_kwargs={"endpoint_url": ENDPOINT_URI})
    s3.invalidate_cache()
    yield {
        "cache_files_urlpath": f"s3://{test_bucket_name}/",
        "cache_files_storage_options": {
            "client_kwargs": {"endpoint_url": ENDPOINT_URI}
        },
    }
    s3.rm(f"s3://{test_bucket_name}/", recursive=True)  # removes the bucket as well


@cache.cacheable
def io_cached_func(path: str) -> fsspec.implementations.local.LocalFileOpener:
    return fsspec.open(path, "rb").open()


@cache.cacheable
def xr_cached_func(data_vars: Dict[str, Any]) -> "xr.Dataset":
    return xr.Dataset(data_vars, attrs={})


@pytest.mark.parametrize("io_delete_original", [True, False])
def test_io_to_s3(
    tmpdir: str, s3_config: fsspec.AbstractFileSystem, io_delete_original: bool
) -> None:
    tmpfile = os.path.join(tmpdir, "test.txt")
    with open(tmpfile, "wb") as f:
        f.write(b"test")
    checksum = fsspec.filesystem("file").checksum(tmpfile)

    # Create cache
    with config.set(**s3_config, io_delete_original=io_delete_original):
        res = io_cached_func(tmpfile)
    assert res.read() == b"test"
    assert res.path == f"test-bucket/{checksum}.txt"
    assert config.SETTINGS["cache_store"].stats() == (0, 1)
    assert os.path.exists(tmpfile) is not io_delete_original

    # Use cache
    with config.set(**s3_config, io_delete_original=io_delete_original):
        res = io_cached_func(tmpfile)
    assert res.read() == b"test"
    assert res.path == f"test-bucket/{checksum}.txt"
    assert config.SETTINGS["cache_store"].stats() == (1, 1)


@pytest.mark.parametrize(
    "xarray_cache_type,extension",
    [("application/x-netcdf", ".nc"), ("application/vnd+zarr", ".zarr")],
)
def test_xr_to_s3(
    tmpdir: str,
    s3_config: fsspec.AbstractFileSystem,
    xarray_cache_type: str,
    extension: str,
) -> None:
    pytest.importorskip("xarray")

    # Create cache
    with config.set(**s3_config, xarray_cache_type=xarray_cache_type):
        res = xr_cached_func({"foo": [0]})
        fs = config.get_cache_files_dirfs()
    xr.testing.assert_identical(res, xr.Dataset({"foo": [0]}))
    assert config.SETTINGS["cache_store"].stats() == (0, 1)
    assert fs.exists(f"247fd17e087ae491996519c097e70e48{extension}")
    checksum = fs.checksum(f"247fd17e087ae491996519c097e70e48{extension}")

    # Use cache
    with config.set(**s3_config, xarray_cache_type=xarray_cache_type):
        res = xr_cached_func({"foo": [0]})
        fs = config.get_cache_files_dirfs()
    xr.testing.assert_identical(res, xr.Dataset({"foo": [0]}))
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
    assert fs.checksum(f"247fd17e087ae491996519c097e70e48{extension}") == checksum
