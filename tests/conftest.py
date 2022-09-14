import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Generator

import pytest

from cacholote import config


@pytest.fixture(autouse=True)
def set_tmpdir(tmpdir: str) -> Generator[None, None, None]:
    with config.set(cache_store_directory=tmpdir):
        yield


@pytest.fixture
def ftp_config_settings(
    request: pytest.FixtureRequest,
) -> Generator[Dict[str, Any], None, None]:
    """
    Fixture providing a writable FTP filesystem.
    """
    if request.param is False:
        yield {}
    else:
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
                yield {
                    "cache_files_urlpath": "ftp:///",
                    "cache_files_storage_options": {
                        "host": "localhost",
                        "port": 2121,
                        "username": "user",
                        "password": "pass",
                    },
                }
            finally:
                popen.terminate()
                popen.wait()
