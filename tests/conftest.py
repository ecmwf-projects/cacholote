import contextlib
import os
import shlex
import subprocess
import sys
import tempfile
import time
from typing import Generator

import pytest

from cacholote import config

PORT = 5555
ENDPOINT_URI = f"http://127.0.0.1:{PORT}/"


@contextlib.contextmanager
def s3_base() -> Generator[None, None, None]:
    """Run moto in server mode
    This starts a local S3 server which we'll test against.
    We must do this because if we try to use moto's s3_mock, problems with aiobotocore
    arise. See https://github.com/aio-libs/aiobotocore/issues/755.
    This and some other fixtures are taken from
    https://github.com/fsspec/s3fs/blob/main/s3fs/tests/test_s3fs.py
    """
    pytest.importorskip("boto3")
    requests = pytest.importorskip("requests")
    try:
        # should fail since we didn't start server yet
        r = requests.get(ENDPOINT_URI)
    except requests.exceptions.ConnectionError:
        pass
    else:
        if r.ok:
            raise RuntimeError("moto server already up")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(shlex.split(f"moto_server s3 -p {PORT}"))

    try:
        timeout = 5.0
        while timeout > 0:
            try:
                r = requests.get(ENDPOINT_URI)
                if r.ok:
                    break
            except requests.exceptions.ConnectionError:
                pass
            timeout -= 0.1
            time.sleep(0.1)
            yield
    finally:
        proc.terminate()
        proc.wait()


@pytest.fixture(autouse=True)
def set_cache(
    tmpdir: str,
    request: pytest.FixtureRequest,
) -> Generator[str, None, None]:
    """
    Fixture providing a writable FTP filesystem.
    """
    if not hasattr(request, "param") or request.param == "file":
        with config.set(cache_store_directory=tmpdir):
            yield "file"
    elif request.param == "ftp":
        pytest.importorskip("pyftpdlib")
        with tempfile.TemporaryDirectory() as ftp_dir:
            popen = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "pyftpdlib",
                    "-d",
                    ftp_dir,
                    "-u",
                    "user",
                    "-P",
                    "pass",
                    "-w",
                    "-V",
                ]
            )
            try:
                time.sleep(1)
                with config.set(
                    cache_store_directory=tmpdir,
                    cache_files_urlpath="ftp:///",
                    cache_files_storage_options={
                        "host": "localhost",
                        "port": 2121,
                        "username": "user",
                        "password": "pass",
                    },
                ):
                    yield request.param
            finally:
                popen.terminate()
                popen.wait()
    elif request.param == "s3":
        botocore_session = pytest.importorskip("botocore.session")
        s3fs = pytest.importorskip("s3fs")
        """Yields properly configured S3FileSystem instance + test bucket name"""
        with s3_base():
            test_bucket_name = "test-bucket"
            session = botocore_session.Session()
            client = session.create_client("s3", endpoint_url=ENDPOINT_URI)
            client.create_bucket(
                Bucket=test_bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
            )

            s3fs.S3FileSystem.clear_instance_cache()
            s3 = s3fs.S3FileSystem(client_kwargs={"endpoint_url": ENDPOINT_URI})
            s3.invalidate_cache()
            try:
                with config.set(
                    cache_store_directory=tmpdir,
                    cache_files_urlpath=f"s3://{test_bucket_name}",
                    cache_files_storage_options=dict(
                        client_kwargs={"endpoint_url": ENDPOINT_URI}
                    ),
                ):
                    yield request.param
            finally:
                s3.rm(
                    f"s3://{test_bucket_name}/", recursive=True
                )  # removes the bucket as well

    else:
        raise ValueError
