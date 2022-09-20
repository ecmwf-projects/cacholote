import contextlib
import os
import pathlib
import shlex
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Generator

import fsspec
import pytest

from cacholote import config


def wait_s3_up(
    endpoint_url: str, max_sleep: float = 1.0, min_sleep: float = 0.1
) -> None:
    requests = pytest.importorskip("requests")
    while max_sleep > 0:
        time.sleep(min_sleep)
        max_sleep -= min_sleep
        try:
            r = requests.get(endpoint_url)
            if r.ok:
                break
        except requests.exceptions.ConnectionError:
            pass


def wait_ftp_up(
    storage_options: Dict[str, Any], max_sleep: float = 1.0, min_sleep: float = 0.1
) -> None:
    while max_sleep > 0:
        time.sleep(min_sleep)
        max_sleep -= min_sleep
        try:
            fsspec.filesystem("ftp", **storage_options)
            break
        except ConnectionRefusedError:
            pass


@contextlib.contextmanager
def initialize_s3() -> Generator[Dict[str, str], None, None]:
    pytest.importorskip("boto3")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    port = 5555
    proc = subprocess.Popen(shlex.split(f"moto_server s3 -p {port}"))
    endpoint_url = f"http://127.0.0.1:{port}/"
    wait_s3_up(endpoint_url)
    try:
        yield {"endpoint_url": endpoint_url}
    finally:
        proc.terminate()
        proc.wait()


@contextlib.contextmanager
def initialize_ftp() -> Generator[Dict[str, Any], None, None]:
    pytest.importorskip("pyftpdlib")
    storage_options = {
        "host": "localhost",
        "port": 2121,
        "username": "user",
        "password": "pass",
    }
    with tempfile.TemporaryDirectory() as ftp_dir:

        popen = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "pyftpdlib",
                "-d",
                ftp_dir,
                "-u",
                str(storage_options["username"]),
                "-P",
                str(storage_options["password"]),
                "-w",
                "-V",
            ]
        )
        wait_ftp_up(storage_options)
        try:
            yield storage_options
        finally:
            popen.terminate()
            popen.wait()


@pytest.fixture(autouse=True)
def set_cache(
    tmpdir: pathlib.Path,
    request: pytest.FixtureRequest,
) -> Generator[str, None, None]:
    """
    Fixture providing a writable FTP filesystem.
    """
    if not hasattr(request, "param") or request.param == "file":
        with config.set(cache_store_directory=tmpdir):
            yield "file"
    elif request.param == "ftp":
        with initialize_ftp() as storage_options:
            with config.set(
                cache_store_directory=tmpdir,
                cache_files_urlpath="ftp:///",
                cache_files_storage_options=storage_options,
            ):
                yield request.param
    elif request.param == "s3":
        pytest.importorskip("s3fs")
        botocore_session = pytest.importorskip("botocore.session")

        test_bucket_name = "test-bucket"
        with initialize_s3() as client_kwargs:
            session = botocore_session.Session()
            client = session.create_client("s3", **client_kwargs)
            client.create_bucket(Bucket=test_bucket_name)
            with config.set(
                cache_store_directory=tmpdir,
                cache_files_urlpath=f"s3://{test_bucket_name}",
                cache_files_storage_options=dict(client_kwargs=client_kwargs),
            ):
                yield request.param
    elif request.param == "redis":
        redislite = pytest.importorskip("redislite")
        with config.set(cache_store=redislite.Redis()):
            yield request.param
    else:
        raise ValueError
