import contextlib
import os
import pathlib
import shlex
import subprocess
import time
from typing import Any, Dict, Generator

import psycopg
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


@contextlib.contextmanager
def initialize_s3() -> Generator[Dict[str, str], None, None]:
    pytest.importorskip("boto3")
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    port = 5555
    proc = subprocess.Popen(
        shlex.split(f"moto_server s3 -p {port}"),
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
    )
    endpoint_url = f"http://127.0.0.1:{port}/"
    wait_s3_up(endpoint_url)
    try:
        yield {"endpoint_url": endpoint_url}
    finally:
        proc.terminate()
        proc.wait()


@pytest.fixture(autouse=True)
def set_cache(
    tmpdir: pathlib.Path,
    postgresql: psycopg.Connection[Any],
    request: pytest.FixtureRequest,
) -> Generator[str, None, None]:
    if not hasattr(request, "param") or request.param == "file":
        with config.set(
            cache_db_urlpath="sqlite:///" + str(tmpdir / "cacholote.db"),
            cache_files_urlpath=str(tmpdir / "cache_files"),
        ):
            yield "file"
    elif request.param == "cads":
        pytest.importorskip("s3fs")
        botocore_session = pytest.importorskip("botocore.session")

        test_bucket_name = "test-bucket"
        with initialize_s3() as client_kwargs:
            session = botocore_session.Session()
            client = session.create_client("s3", **client_kwargs)
            client.create_bucket(Bucket=test_bucket_name)
            with config.set(
                cache_db_urlpath=(
                    f"postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:"
                    f"{postgresql.info.port}/{postgresql.info.dbname}"
                ),
                cache_files_urlpath=f"s3://{test_bucket_name}",
                cache_files_storage_options=dict(client_kwargs=client_kwargs),
            ):
                yield request.param
    else:
        raise ValueError
