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


@contextlib.contextmanager
def initialize_s3() -> Generator[None, None, None]:
    pytest.importorskip("boto3")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    proc = subprocess.Popen(shlex.split("moto_server s3 -p 5555"))
    time.sleep(0.5)
    try:
        yield
    finally:
        proc.terminate()
        proc.wait()


@contextlib.contextmanager
def initialize_ftp() -> Generator[None, None, None]:
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
        time.sleep(0.5)
        try:
            yield
        finally:
            popen.terminate()
            popen.wait()


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
        with initialize_ftp():
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
    elif request.param == "s3":
        pytest.importorskip("s3fs")
        botocore_session = pytest.importorskip("botocore.session")
        client_kwargs = {"endpoint_url": "http://127.0.0.1:5555/"}
        test_bucket_name = "test-bucket"
        with initialize_s3():
            session = botocore_session.Session()
            client = session.create_client("s3", **client_kwargs)
            client.create_bucket(Bucket=test_bucket_name)
            with config.set(
                cache_store_directory=tmpdir,
                cache_files_urlpath=f"s3://{test_bucket_name}",
                cache_files_storage_options=dict(client_kwargs=client_kwargs),
            ):
                yield request.param
    else:
        raise ValueError
