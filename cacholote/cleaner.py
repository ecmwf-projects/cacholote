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
        sorters = (config.CacheEntry.timestamp, config.CacheEntry.counter)
    elif method == "LFU":
        sorters = (config.CacheEntry.counter, config.CacheEntry.timestamp)
    else:
        raise ValueError("`method` must be 'LRU' or 'LFU'.")

    fs = utils.get_cache_files_fs()
    if fs.du(config.SETTINGS["cache_files_urlpath"]) <= maxsize:
        return

    delete_stmt = sqlalchemy.delete(config.CacheEntry)
    query_tuple = (config.CacheEntry.key, config.CacheEntry.result["args"].as_json())
    with sqlalchemy.orm.Session(config.SETTINGS["engine"]) as session:
        for key, cached_args in session.query(*query_tuple).order_by(*sorters):
            if extra_encoders._are_file_args(*cached_args):
                fs_entry, urlpath = extra_encoders._get_fs_and_urlpath(*cached_args)
                if fs == fs_entry and fs.exists(urlpath):
                    recursive = cached_args[0]["type"] == "application/vnd+zarr"
                    fs.rm(urlpath, recursive=recursive)
                    session.execute(delete_stmt.where(config.CacheEntry.key == key))
                    if fs.du(config.SETTINGS["cache_files_urlpath"]) <= maxsize:
                        break
        session.commit()
