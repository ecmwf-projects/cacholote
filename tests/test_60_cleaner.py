import pathlib
import sqlite3
from typing import Literal

import fsspec
import pytest

from cacholote import cache, cleaner, config, utils


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("method", ["LRU", "LFU"])
def test_cleaner(
    tmpdir: pathlib.Path,
    method: Literal["LRU", "LFU"],
) -> None:

    con = sqlite3.connect(str(tmpdir / "cacholote.db"))
    cur = con.cursor()

    fs = utils.get_cache_files_fs()
    dirname = config.SETTINGS["cache_files_urlpath"]

    # Create files and copy to cache dir
    checksums = []
    for i in range(3):
        filename = tmpdir / f"test{i}.txt"
        with open(filename, "w") as f:
            f.write("0")
        checksums.append(fsspec.filesystem("file").checksum(filename))
        open_url(filename)

    # Re-use cache file
    open_url(tmpdir / "test1.txt")  # Most frequently used
    open_url(tmpdir / "test1.txt")  # Most frequently used
    open_url(tmpdir / "test0.txt")  # Most recently used

    assert fs.du(dirname) == 3

    cleaner.clean_cache_files(3, method=method)
    cur.execute("SELECT key FROM cache_entries")
    keys = cur.fetchall()
    assert len(keys) == fs.du(dirname) == 3

    cleaner.clean_cache_files(2, method=method)
    cur.execute("SELECT key FROM cache_entries")
    keys = cur.fetchall()
    assert len(keys) == fs.du(dirname) == 2

    cleaner.clean_cache_files(1, method=method)
    cur.execute("SELECT key FROM cache_entries")
    keys = cur.fetchall()
    assert len(keys) == fs.du(dirname) == 1

    if method == "LFU":
        # Most frequently used
        checksum = checksums[1]
    else:
        # Most recently used
        checksum = checksums[0]
    assert fs.exists(f"{dirname}/{checksum}.txt")
