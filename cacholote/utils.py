"""Utilities."""
# Copyright 2019, B-Open Solutions srl.
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
import os
from typing import Any, Dict, Optional, Union

import fsspec
import fsspec.implementations.dirfs

from . import config


def hexdigestify(text: str) -> str:
    """Convert text to its hash made of hexadecimal digits."""
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def get_cache_files_directory() -> Union[str, os.PathLike[Any]]:
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


def get_cache_files_dirfs() -> fsspec.implementations.dirfs.DirFileSystem:
    """Return the ``fsspec`` directory filesystem where cache files are stored."""
    cache_files_directory = get_cache_files_directory()
    protocol = fsspec.utils.get_protocol(cache_files_directory)
    fs = fsspec.filesystem(protocol, **config.SETTINGS["cache_files_storage_options"])
    return fsspec.implementations.dirfs.DirFileSystem(cache_files_directory, fs)


def get_filesystem_from_urlpath(
    urlpath: str, storage_options: Dict[str, Any]
) -> fsspec.AbstractFileSystem:
    """Return the ``fsspec`` filesystem inferred from the URL protocol.

    Parameters
    ----------
    urlpath: str
        URL of the form protocol://location
    storage_options: dict
        Storage options for fsspec.filesystem

    Returns
    -------
    AbstractFileSystem
    """
    protocol = fsspec.utils.get_protocol(urlpath)
    return fsspec.filesystem(protocol, **storage_options)


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
