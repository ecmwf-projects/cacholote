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

import datetime
from typing import Literal

import sqlalchemy.orm

from . import config, extra_encoders, utils


def delete_entry(key: str, expiration: datetime.datetime) -> None:
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        filters = [
            config.CacheEntry.key == key,
            config.CacheEntry.expiration == expiration,
        ]
        (cached_args,) = (
            session.query(config.CacheEntry.result["args"]).filter(*filters).one()
        )

        # Remove cache entry
        session.query(config.CacheEntry).filter(*filters).delete()
        session.commit()

        # Delete cache file
        if extra_encoders._are_file_args(*cached_args):
            fs, urlpath = extra_encoders._get_fs_and_urlpath(*cached_args)
            if fs.exists(urlpath):
                recursive = cached_args[0]["type"] == "application/vnd+zarr"
                fs.rm(urlpath, recursive=recursive)


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
        Delete files that are not present in the cache database.
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

    # Clean files in database
    queries = (
        config.CacheEntry.key,
        config.CacheEntry.expiration,
        config.CacheEntry.result["args"],
    )
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for key, expiration, args in session.query(*queries).order_by(*sorters):
            if extra_encoders._are_file_args(*args):
                fs_entry, urlpath = extra_encoders._get_fs_and_urlpath(*args)
                urlpath = fs_entry.unstrip_protocol(urlpath)
                if fs == fs_entry and urlpath in sizes:
                    delete_entry(key, expiration)

                    # Delete file
                    sizes.pop(urlpath)
                    if sum(sizes.values()) <= maxsize:
                        return

    if delete_unknown_files:
        # Sort by modification time
        times = [
            fs.modified(k) if fs.exists(k) else datetime.datetime.min for k in sizes
        ]
        for _, urlpath in sorted(zip(times, sizes)):
            sizes.pop(urlpath)
            if fs.exists(urlpath):
                fs.rm(urlpath, recursive=True)
            if sum(sizes.values()) <= maxsize:
                return

    raise ValueError(
        f"Unable to clean {dirname!r}. Final size: {sum(sizes.values())!r}."
    )
