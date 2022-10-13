import datetime
import json
import sqlite3
from typing import Any, Literal

from . import config, utils


def clean_cache_files(
    maxsize: int,
    database: str = ":memory:",
    method: Literal["LRU", "LFU"] = "LRU",
    delete_unknown_files: bool = False,
    **kwargs: Any,
) -> None:
    """Clean cache files.

    Parameters
    ----------
    maxsize: int
        Maximum total size of cache files.
    database: str, default=":memory:"
        Path to the cleaner database file.
    method: str, default="LRU"
        * LRU: Last Recently Used
        * LFU: Least Frequently Used
    delete_unknown_files: bool, default=False
        Delete unknown files in cache dir.
    **kwargs:
        Keyword arguments for `sqlite3.connect`.
    """
    if method == "LRU":
        sorters = ("atime", "count")
    elif method == "LFU":
        sorters = ("count", "atime")
    else:
        raise ValueError("`method` must be 'LRU' or 'LFU'.")

    fs = utils.get_cache_files_fs()
    cache_dir = utils.get_cache_files_directory()
    if fs.du(cache_dir) <= maxsize:
        return

    # Create db
    con = sqlite3.connect(database, **kwargs)
    cur = con.cursor()
    cur.execute("CREATE TABLE cleaner(path, key, atime, count)")
    for key in utils.cache_store_keys_iter():
        obj_dict = json.loads(config.SETTINGS["cache_store"][key])
        path = obj_dict.get("file:local_path")
        if path and fs.exists(path):
            path = fs.unstrip_protocol(path)

            try:
                atime = obj_dict["info"]["atime"]
            except KeyError:
                # get time from file metadata
                atime = fs.modified(path)
            else:
                # atime stored by cacholote is a string
                atime = datetime.datetime.fromisoformat(atime)

            try:
                count = obj_dict["info"]["count"]
            except KeyError:
                count = 1

            cur.execute(
                "INSERT INTO cleaner VALUES(?, ?, ?, ?)", (path, key, atime, count)
            )
            con.commit()

    # Add unknown files
    if delete_unknown_files:
        for path in fs.ls(cache_dir):
            path = fs.unstrip_protocol(path)
            if cur.execute("SELECT path FROM cleaner WHERE path=?", (path,)):
                cur.execute(
                    "INSERT INTO cleaner VALUES(?, ?, ?, ?)",
                    (path, "", fs.modified(path), 0),
                )
                con.commit()

    # Sort and clean
    for path, key in cur.execute(
        "SELECT path, key FROM cleaner ORDER BY " + ",".join(sorters)
    ):
        if fs.du(cache_dir, total=True) <= maxsize:
            break
        fs.rm(path, recursive=True)
        if key:
            utils.delete_cache_store_key(key)
    con.close()
