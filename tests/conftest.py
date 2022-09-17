import subprocess
import sys
import tempfile
import time
from typing import Generator, Optional

import pytest

from cacholote import config


@pytest.fixture(autouse=True)
def set_cache(
    tmpdir: str,
    request: Optional[pytest.FixtureRequest] = None,
) -> Generator[None, None, None]:
    """
    Fixture providing a writable FTP filesystem.
    """
    if request is None or request.param == "file":
        with config.set(cache_store_directory=tmpdir):
            yield
    elif request.param == "s3":
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
                    cache_files_urlpath="ftp:///",
                    cache_files_storage_options={
                        "host": "localhost",
                        "port": 2121,
                        "username": "user",
                        "password": "pass",
                    },
                ):
                    yield
            finally:
                popen.terminate()
                popen.wait()
    else:
        raise ValueError
