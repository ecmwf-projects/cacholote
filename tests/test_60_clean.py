import pathlib
from typing import Any, Literal, Optional, Sequence

import fsspec
import pytest

from cacholote import cache, clean, config, utils


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("method", ["LRU", "LFU"])
@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
def test_clean_cache_files(
    tmpdir: pathlib.Path,
    set_cache: str,
    method: Literal["LRU", "LFU"],
) -> None:

    con = config.ENGINE.get().raw_connection()
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
    cur.execute("SELECT * FROM cache_entries", ())
    nrows = len(cur.fetchall())
    assert nrows == fs.du(dirname) == 2

    # Delete one file
    clean.clean_cache_files(1, method=method)
    cur.execute("SELECT * FROM cache_entries", ())
    nrows = len(cur.fetchall())
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
            assert set(fs.ls(dirname)) == {
                f"{dirname}/unknown.txt",
                f"{dirname}/unknown.txt.lock",
            }
        else:
            assert fs.ls(dirname) == [f"{dirname}/unknown.txt"]


@pytest.mark.parametrize(
    "tags_to_clean, tags_to_keep, expected",
    [
        ({None, "1"}, None, "2"),
        ({None, "2"}, None, "1"),
        ({"1", "2"}, None, None),
        (None, {None}, None),
        (None, {"1"}, "1"),
        (None, {"2"}, "2"),
    ],
)
def test_clean_tagged_files(
    tmpdir: pathlib.Path,
    tags_to_clean: Optional[Sequence[Optional[str]]],
    tags_to_keep: Optional[Sequence[Optional[str]]],
    expected: Optional[str],
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    for tag in [None, "1", "2"]:
        tmpfile = tmpdir / f"test_{tag}.txt"
        fsspec.filesystem("file").pipe_file(tmpfile, b"1")
        with config.set(tag=tag):
            open_url(tmpfile)

    clean.clean_cache_files(1, tags_to_clean=tags_to_clean, tags_to_keep=tags_to_keep)
    assert fs.ls(dirname) == [
        f"{dirname}/{fs.checksum(tmpdir / f'test_{expected}.txt')}.txt"
    ]


def test_clean_tagged_files_wrong_args() -> None:
    with pytest.raises(
        ValueError,
        match="tags_to_clean/keep are mutually exclusive.",
    ):
        clean.clean_cache_files(1, tags_to_keep=[], tags_to_clean=[])


@pytest.mark.parametrize("wrong_type", ["1", [1]])
def test_clean_tagged_files_wrong_types(wrong_type: Any) -> None:
    with pytest.raises(
        TypeError,
        match="tags_to_clean/keep must be None or a sequence of str/None.",
    ):
        clean.clean_cache_files(1, tags_to_keep=wrong_type)
    with pytest.raises(
        TypeError,
        match="tags_to_clean/keep must be None or a sequence of str/None.",
    ):
        clean.clean_cache_files(1, tags_to_clean=wrong_type)
