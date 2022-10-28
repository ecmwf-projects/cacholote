"""Functions to clean cache database and files."""

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
# limitations under the License.

import functools
import json
import posixpath
from typing import Any, Dict, Literal, Optional, Set

import sqlalchemy.orm

from . import config, extra_encoders, utils


def delete_cache_file(
    obj: Dict[str, Any],
    session: Optional[sqlalchemy.orm.Session] = None,
    cache_entry: Optional[config.CacheEntry] = None,
    sizes: Dict[str, int] = {},
    dry_run: bool = False,
) -> Any:
    if {"type", "callable", "args", "kwargs"} == set(obj) and obj["callable"] in (
        "cacholote.extra_encoders:decode_xr_dataset",
        "cacholote.extra_encoders:decode_io_object",
    ):
        cache_fs, cache_dirname = utils.get_cache_files_fs_dirname()
        cache_dirname = cache_fs.unstrip_protocol(cache_dirname)

        fs, urlpath = extra_encoders._get_fs_and_urlpath(*obj["args"][:2])
        urlpath = fs.unstrip_protocol(urlpath)

        if cache_fs == fs and posixpath.dirname(urlpath) == cache_dirname:
            sizes.pop(urlpath, None)
            if session and cache_entry and not dry_run:
                # Clean database
                session.delete(cache_entry)
                session.commit()
            if fs.exists(urlpath) and not dry_run:
                # Remove file
                fs.rm(urlpath, recursive=True)

    return obj


def _get_unknown_files(sizes: Dict[str, Any]) -> Set[str]:
    unknown_sizes = dict(sizes)
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for cache_entry in session.query(config.CacheEntry):
            json.loads(
                cache_entry.result,
                object_hook=functools.partial(
                    delete_cache_file,
                    sizes=unknown_sizes,
                    dry_run=True,
                ),
            )
    return set(unknown_sizes)


def delete_cache_entry(
    session: sqlalchemy.orm.Session, cache_entry: config.CacheEntry
) -> None:
    # Delete cache entry
    session.delete(cache_entry)
    session.commit()

    # Delete cache file
    json.loads(cache_entry.result, object_hook=delete_cache_file)


def clean_cache_files(
    maxsize: int,
    method: Literal["LRU", "LFU"] = "LRU",
    delete_unknown_files: bool = False,
) -> None:
    """Clean cache files.

    Parameters
    ----------
    maxsize: int
        Maximum total size of cache files (bytes).
    method: str, default="LRU"
        * LRU: Last Recently Used
        * LFU: Least Frequently Used
    delete_unknown_files: bool, default=False
        Delete files that are not present in the database before
        deleting files in the database.
    """
    if method == "LRU":
        sorters = [config.CacheEntry.timestamp, config.CacheEntry.counter]
    elif method == "LFU":
        sorters = [config.CacheEntry.counter, config.CacheEntry.timestamp]
    else:
        raise ValueError("`method` must be 'LRU' or 'LFU'.")
    sorters.append(config.CacheEntry.expiration)

    # Freeze directory content
    fs, dirname = utils.get_cache_files_fs_dirname()
    sizes = {fs.unstrip_protocol(path): fs.du(path) for path in fs.ls(dirname)}
    if sum(sizes.values()) <= maxsize:
        return

    if delete_unknown_files:
        for urlpath in _get_unknown_files(sizes):
            if fs.exists(urlpath):
                fs.rm(urlpath)
            sizes.pop(urlpath)
            if sum(sizes.values()) <= maxsize:
                return

    # Clean files in database
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for cache_entry in session.query(config.CacheEntry).order_by(*sorters):
            json.loads(
                cache_entry.result,
                object_hook=functools.partial(
                    delete_cache_file,
                    session=session,
                    cache_entry=cache_entry,
                    sizes=sizes,
                ),
            )
            if sum(sizes.values()) <= maxsize:
                return

    raise ValueError(
        f"Unable to clean {dirname!r}. Final size: {sum(sizes.values())!r}."
    )
