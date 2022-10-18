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

import hashlib
import io
from typing import Optional

import fsspec
import fsspec.implementations.dirfs

from . import config


def hexdigestify(text: str) -> str:
    """Convert text to its hash made of hexadecimal digits."""
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def get_cache_files_directory() -> str:
    """Return the directory where cache files are stored."""
    if (
        config.SETTINGS["cache_files_urlpath"]
        is config.SETTINGS["cache_store_directory"]
        is None
    ):
        raise ValueError(
            "please set 'cache_files_urlpath' and 'cache_files_storage_options'"
        )
    if config.SETTINGS["cache_files_urlpath"] is None:
        return str(config.SETTINGS["cache_store_directory"])
    return str(config.SETTINGS["cache_files_urlpath"])


def get_cache_files_directory_readonly() -> str:
    """Return the directory where cache files are stored."""
    if config.SETTINGS["cache_files_urlpath_readonly"] is None:
        return get_cache_files_directory()
    return str(config.SETTINGS["cache_files_urlpath_readonly"])


def get_cache_files_fs() -> fsspec.implementations.dirfs.DirFileSystem:
    """Return the ``fsspec`` filesystem where cache files are stored."""
    fs, _, _ = fsspec.get_fs_token_paths(
        get_cache_files_directory(),
        storage_options=config.SETTINGS["cache_files_storage_options"],
    )
    return fs


def get_cache_files_dirfs() -> fsspec.implementations.dirfs.DirFileSystem:
    """Return the ``fsspec`` directory filesystem where cache files are stored."""
    return fsspec.implementations.dirfs.DirFileSystem(
        get_cache_files_directory(), get_cache_files_fs()
    )


def copy_buffered_file(
    f_in: fsspec.spec.AbstractBufferedFile,
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
        f_out.write(data)
