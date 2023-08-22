"""Utilities."""

# Copyright 2019, B-Open Solutions srl.
# Copyright 2022, European Union.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.import hashlib

import dataclasses
import datetime
import functools
import hashlib
import io
import time
import warnings
from types import TracebackType
from typing import Any, Optional, Tuple, Type

import fsspec

from . import config


def hexdigestify(text: str) -> str:
    """Convert text to its hash made of hexadecimal digits."""
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()[:32]


def get_cache_files_fs_dirname() -> Tuple[fsspec.AbstractFileSystem, str]:
    """Return the ``fsspec`` filesystem and directory name where cache files are stored."""
    fs, _, (path,) = fsspec.get_fs_token_paths(
        config.get().cache_files_urlpath,
        storage_options=config.get().cache_files_storage_options,
    )
    fs.invalidate_cache()
    return (fs, path)


def copy_buffered_file(
    f_in: Any,
    f_out: fsspec.spec.AbstractBufferedFile,
    buffer_size: Optional[int] = None,
) -> None:
    """Copy file in chunks.

    Parameters
    ----------
    f_in, f_out: fsspec.spec.AbstractBufferedFile
        Source and destination buffered files.
    buffer_size: int, optional, default=None
        Maximum size for chunks in bytes.
        None: use ``io.DEFAULT_BUFFER_SIZE``
    """
    if buffer_size is None:
        buffer_size = io.DEFAULT_BUFFER_SIZE
    while True:
        data = f_in.read(buffer_size)
        if not data:
            break
        f_out.write(data if isinstance(data, bytes) else data.encode())


@dataclasses.dataclass
class FileLock:
    fs: fsspec.AbstractFileSystem  # fsspec file system
    urlpath: str  # file to lock
    timeout: Optional[float]  # lock timeout in seconds

    @functools.cached_property
    def lockfile(self) -> str:
        return self.urlpath + ".lock"

    def acquire(self) -> None:
        self.fs.touch(self.lockfile)

    def release(self) -> None:
        if self.fs.exists(self.lockfile):
            self.fs.rm(self.lockfile)

    @property
    def is_locked(self) -> bool:
        return bool(self.fs.exists(self.lockfile))

    def wait_until_released(self) -> None:
        warned = False
        message = f"{self.urlpath!r} is locked: {self.lockfile!r}"
        start = time.perf_counter()
        while self.is_locked:
            if self.timeout is not None and time.perf_counter() - start > self.timeout:
                raise TimeoutError(message)
            if not warned:
                warnings.warn(message, UserWarning)
                warned = True
            time.sleep(min(1, self.timeout or 1))

    def __enter__(self) -> bool:
        self.wait_until_released()
        self.acquire()
        return bool(self.fs.exists(self.urlpath))

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.release()


def utcnow() -> datetime.datetime:
    """See https://discuss.python.org/t/deprecating-utcnow-and-utcfromtimestamp/26221."""
    return datetime.datetime.now(tz=datetime.timezone.utc)
