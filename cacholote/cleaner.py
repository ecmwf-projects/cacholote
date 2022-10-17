import json
from typing import Literal

import sqlalchemy.orm

from . import config, extra_encoders, utils


def clean_cache_files(
    maxsize: int,
    method: Literal["LRU", "LFU"] = "LRU",
) -> None:
    """Clean cache files.

    Parameters
    ----------
    maxsize: int
        Maximum total size of cache files (bytes).
    method: str, default="LRU"
        * LRU: Last Recently Used
        * LFU: Least Frequently Used
    """
    if method == "LRU":
        sorters = (config.CacheEntry.timestamp, config.CacheEntry.count)
    elif method == "LFU":
        sorters = (config.CacheEntry.count, config.CacheEntry.timestamp)
    else:
        raise ValueError("`method` must be 'LRU' or 'LFU'.")

    fs = utils.get_cache_files_fs()
    if fs.du(config.SETTINGS["cache_files_urlpath"]) <= maxsize:
        return

    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for cache_entry in session.query(config.CacheEntry).order_by(*sorters):
            cached_json = json.loads(cache_entry.value)
            if extra_encoders._is_file_json(cached_json):
                fs_entry, urlpath = extra_encoders._get_fs_and_urlpath_to_decode(
                    *cached_json["args"]
                )
                if fs == fs_entry and fs.exists(urlpath):
                    recursive = cached_json["args"][0]["type"] == "application/vnd+zarr"
                    fs.rm(urlpath, recursive=recursive)
                    session.delete(cache_entry)
                    if fs.du(config.SETTINGS["cache_files_urlpath"]) <= maxsize:
                        break
        session.commit()
