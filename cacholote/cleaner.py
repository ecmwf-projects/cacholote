import datetime
import json
from typing import Literal

from . import config, utils


def clean_cache_files(
    maxsize: int,
    method: Literal["LRU", "LFU"] = "LRU",
    delete_unknown_files: bool = False,
) -> None:
    """Clean cache files.

    Parameters
    ----------
    maxsize: int
        Maximum total size of cache files
    method: str, default="LRU"
        * LRU: Last Recently Used
        * LFU: Least Frequently Used
    delete_unknown_files: bool, default=False
        Delete files in cache dir that are not fount in the cache store
    """
    methods = ["LRU", "LFU"]
    if method not in methods:
        raise ValueError(f"`method` must be one of {methods!r}")

    fs = utils.get_cache_files_fs()
    cache_dir = utils.get_cache_files_directory()
    if fs.du(cache_dir) <= maxsize:
        return

    # Get info from JSON
    paths = []
    keys = []
    atimes = []
    counts = []
    for key in utils.cache_store_keys_iter():
        obj_dict = json.loads(config.SETTINGS["cache_store"][key])
        path = obj_dict.get("file:local_path")
        if path and fs.exists(path):
            path = fs.unstrip_protocol(path)
            paths.append(path)
            keys.append(key)

            try:
                atime = obj_dict["info"]["atime"]
            except KeyError:
                # get time from file metadata
                atime = fs.modified(path)
            else:
                # atime stored by cacholote is a string
                atime = datetime.datetime.fromisoformat(atime)
            atimes.append(atime)

            try:
                counts.append(obj_dict["info"]["count"])
            except KeyError:
                counts.append(1)

    # Add unknown files
    if delete_unknown_files:
        for path in fs.ls(cache_dir):
            path = fs.unstrip_protocol(path)
            if path not in paths:
                paths.append(path)
                keys.append("")
                atimes.append(fs.modified(path))
                counts.append(0)

    # Sort and clean
    if method == "LRU":
        a = atimes
        b = counts
    elif method == "LFU":
        a = counts
        b = atimes
    else:
        NotImplementedError
    for _, _, path, key in sorted(zip(a, b, paths, keys)):
        if fs.du(cache_dir, total=True) <= maxsize:
            break
        fs.rm(path, recursive=True)
        if key:
            utils.delete_cache_store_key(key)
