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

from typing import Literal

import sqlalchemy.orm

from . import config, extra_encoders, utils


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
        sorters = (config.CacheEntry.timestamp, config.CacheEntry.counter)
    elif method == "LFU":
        sorters = (config.CacheEntry.counter, config.CacheEntry.timestamp)
    else:
        raise ValueError("`method` must be 'LRU' or 'LFU'.")

    # Freeze directory content
    fs, dirname = utils.get_cache_files_fs_dirname()
    sizes = {fs.unstrip_protocol(path): fs.du(path) for path in fs.ls(dirname)}
    if sum(sizes.values()) <= maxsize:
        return

    # Clean files in database
    delete_stmt = sqlalchemy.delete(config.CacheEntry)
    query_tuple = (config.CacheEntry.key, config.CacheEntry.result["args"])
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for key, cached_args in session.query(*query_tuple).order_by(*sorters):
            if extra_encoders._are_file_args(*cached_args):
                fs_entry, urlpath = extra_encoders._get_fs_and_urlpath(*cached_args)
                urlpath = fs_entry.unstrip_protocol(urlpath)
                if fs == fs_entry and urlpath in sizes:
                    # Delete file
                    sizes.pop(urlpath)
                    recursive = cached_args[0]["type"] == "application/vnd+zarr"
                    fs.rm(urlpath, recursive=recursive)
                    # Delete database entry
                    session.execute(delete_stmt.where(config.CacheEntry.key == key))
                    session.commit()
                    if sum(sizes.values()) <= maxsize:
                        return

    if delete_unknown_files:
        # Sort by modification time
        times = [fs.modified(k) for k in sizes]
        for _, urlpath in sorted(zip(times, sizes)):
            sizes.pop(urlpath)
            fs.rm(urlpath, recursive=True)
            if sum(sizes.values()) <= maxsize:
                return

    raise ValueError(
        f"Unable to clean {dirname!r}. Final size: {sum(sizes.values())!r}."
    )
