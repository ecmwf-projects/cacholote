from __future__ import annotations

import os
import pathlib
from collections.abc import Iterator
from typing import Any

import botocore.session
import psycopg
import pytest
import requests
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from cacholote import config, database


@pytest.fixture(scope="session")
def s3_server() -> Iterator[ThreadedMotoServer]:
    if "AWS_SECRET_ACCESS_KEY" not in os.environ:
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
    if "AWS_ACCESS_KEY_ID" not in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=5555)
    server.start()
    yield server
    server.stop()


def create_test_bucket(
    server: ThreadedMotoServer, test_bucket_name: str
) -> dict[str, Any]:
    endpoint_url = f"http://{server._ip_address}:{server._port}/"
    client_kwargs = {"endpoint_url": endpoint_url}
    requests.post(f"{endpoint_url}moto-api/reset")
    session = botocore.session.Session()
    client = session.create_client("s3", **client_kwargs)
    client.create_bucket(Bucket=test_bucket_name)
    return client_kwargs


@pytest.fixture(autouse=True)
def set_cache(
    tmp_path: pathlib.Path,
    postgresql: psycopg.Connection[Any],
    request: pytest.FixtureRequest,
    s3_server: ThreadedMotoServer,
) -> Iterator[str]:
    param = getattr(request, "param", "file")
    if param.lower() == "cads":
        database._cached_sessionmaker.cache_clear()
        test_bucket_name = "test-bucket"
        client_kwargs = create_test_bucket(s3_server, test_bucket_name)
        with config.set(
            cache_db_urlpath=(
                f"postgresql+psycopg2://{postgresql.info.user}:@{postgresql.info.host}:"
                f"{postgresql.info.port}/{postgresql.info.dbname}"
            ),
            cache_files_urlpath=f"s3://{test_bucket_name}",
            cache_files_storage_options={"client_kwargs": client_kwargs},
            cache_files_protocol="s3",
        ):
            yield "cads"
    elif param.lower() in ("file", "local"):
        with config.set(
            cache_db_urlpath="sqlite:///" + str(tmp_path / "cacholote.db"),
            cache_files_urlpath=str(tmp_path / "cache_files"),
        ):
            yield "file"
    elif param.lower() == "off":
        yield "off"
    else:
        raise ValueError(f"{param=}")
