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
import logging
import posixpath
from typing import Any, Dict, Literal, Optional, Set

import sqlalchemy.orm

from . import config, extra_encoders, utils


def _delete_cache_file(
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

        if posixpath.dirname(urlpath) == cache_dirname:
            sizes.pop(urlpath, None)
            if session and cache_entry and not dry_run:
                logging.info(f"Deleting cache entry: {cache_entry!r}")
                session.delete(cache_entry)
                session.commit()
            if fs.exists(urlpath) and not dry_run:
                logging.info(f"Deleting {urlpath!r}")
                fs.rm(urlpath, recursive=True)

    return obj


def _get_unknown_files(sizes: Dict[str, Any]) -> Set[str]:
    files_to_skip = []
    for urlpath in sizes:
        if urlpath.endswith(".lock"):
            files_to_skip.append(urlpath)
            files_to_skip.append(urlpath.rsplit(".lock", 1)[0])

    unknown_sizes = {k: v for k, v in sizes.items() if k not in files_to_skip}
    if unknown_sizes:
        with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
            for cache_entry in session.query(config.CacheEntry):
                json.loads(
                    cache_entry._result_as_string,
                    object_hook=functools.partial(
                        _delete_cache_file,
                        sizes=unknown_sizes,
                        dry_run=True,
                    ),
                )
    return set(unknown_sizes)


def _stop_cleaning(maxsize: int, sizes: Dict[str, int], dirname: str) -> bool:
    size = sum(sizes.values())
    logging.info(f"Size of {dirname!r}: {size!r}")
    return size <= maxsize


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
    if _stop_cleaning(maxsize, sizes, dirname):
        return

    if delete_unknown_files:
        for urlpath in _get_unknown_files(sizes):
            sizes.pop(urlpath)
            if fs.exists(urlpath):
                logging.info(f"Deleting {urlpath!r}")
                fs.rm(urlpath)
            if _stop_cleaning(maxsize, sizes, dirname):
                return

    # Clean files in database
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for cache_entry in session.query(config.CacheEntry).order_by(*sorters):
            json.loads(
                cache_entry._result_as_string,
                object_hook=functools.partial(
                    _delete_cache_file,
                    session=session,
                    cache_entry=cache_entry,
                    sizes=sizes,
                ),
            )
            if _stop_cleaning(maxsize, sizes, dirname):
                return

    raise ValueError(
        f"Unable to clean {dirname!r}. Final size: {sum(sizes.values())!r}."
    )
