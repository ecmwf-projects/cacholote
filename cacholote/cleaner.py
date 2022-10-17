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
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for cache_entry in session.query(config.CacheEntry).order_by(*sorters):
            if fs.du(config.SETTINGS["cache_files_urlpath"]) <= maxsize:
                break

            cached_dict = json.loads(cache_entry.value)
            if isinstance(cached_dict, dict) and "file:local_path" in cached_dict:
                fs_entry, urlpath = extra_encoders._get_fs_and_urlpath_to_decode(
                    cached_dict, validate=False
                )
                if fs == fs_entry and fs.exists(urlpath):
                    fs.rm(urlpath, recursive=True)
                    session.delete(cache_entry)
        session.commit()
