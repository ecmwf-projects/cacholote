import pathlib
import sqlite3
from typing import Literal

import fsspec
import pytest

from cacholote import cache, clean, config, utils


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("method", ["LRU", "LFU"])
@pytest.mark.parametrize("set_cache", ["file", "s3"], indirect=True)
def test_clean_cache_files(
    tmpdir: pathlib.Path,
    set_cache: str,
    method: Literal["LRU", "LFU"],
) -> None:

    con = sqlite3.connect(str(tmpdir / "cacholote.db"))
    cur = con.cursor()
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create files
    checksums = {}
    for algorithm in ("LRU", "LFU"):
        filename = tmpdir / f"{algorithm}.txt"
        fsspec.filesystem("file").pipe_file(filename, b"1")
        cachedname = f"{dirname}/{fsspec.filesystem('file').checksum(filename)}.txt"
        checksums[algorithm] = cachedname

    # Copy to cache
    open_url(tmpdir / "LRU.txt")
    open_url(tmpdir / "LRU.txt")
    open_url(tmpdir / "LFU.txt")
    assert set(fs.ls(dirname)) == set(checksums.values())

    # Do not clean
    clean.clean_cache_files(2, method=method)
    nrows = len(cur.execute("SELECT * FROM cache_entries").fetchall())
    assert nrows == fs.du(dirname) == 2

    # Delete one file
    clean.clean_cache_files(1, method=method)
    nrows = len(cur.execute("SELECT * FROM cache_entries").fetchall())
    assert nrows == fs.du(dirname) == 1
    assert not fs.exists(f"{dirname}/{checksums[method]}.txt")


@pytest.mark.parametrize("delete_unknown_files", [True, False])
@pytest.mark.parametrize("add_lock", [True, False])
def test_delete_unknown_files(
    tmpdir: pathlib.Path, delete_unknown_files: bool, add_lock: bool
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1")

    # Copy to cache
    open_url(tmpfile)

    # Add unknown
    fs.put(str(tmpfile), f"{dirname}/unknown.txt")
    if add_lock:
        fs.touch(f"{dirname}/unknown.txt.lock")

    # Clean one file
    clean.clean_cache_files(1, delete_unknown_files=delete_unknown_files)
    if delete_unknown_files and not add_lock:
        assert fs.ls(dirname) == [f"{dirname}/{fs.checksum(tmpfile)}.txt"]
    else:
        if add_lock:
            assert fs.ls(dirname) == [
                f"{dirname}/unknown.txt",
                f"{dirname}/unknown.txt.lock",
            ]
        else:
            assert fs.ls(dirname) == [f"{dirname}/unknown.txt"]


def test_clean_tagged_files(tmpdir: pathlib.Path) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    for tag in [None, "1", "2"]:
        tmpfile = tmpdir / f"test_{tag}.txt"
        fsspec.filesystem("file").pipe_file(tmpfile, b"1")
        with config.set(tag=tag):
            open_url(tmpfile)

    clean.clean_cache_files(1, tags=[None, "2"])
    assert fs.ls(dirname) == [f"{dirname}/{fs.checksum(tmpdir / f'test_1.txt')}.txt"]
