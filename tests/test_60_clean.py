import contextlib
import datetime
import pathlib
import time
from typing import Any, Literal, Optional, Sequence

import fsspec
import pytest
import structlog

from cacholote import cache, clean, config, utils

does_not_raise = contextlib.nullcontext


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
    con = config.get().engine.raw_connection()
    cur = con.cursor()
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create files
    for algorithm in ("LRU", "LFU"):
        filename = tmpdir / f"{algorithm}.txt"
        fsspec.filesystem("file").pipe_file(filename, b"1")

    # Copy to cache
    (lru_path,) = {open_url(tmpdir / "LRU.txt").path for _ in range(2)}
    lfu_path = open_url(tmpdir / "LFU.txt").path
    assert set(fs.ls(dirname)) == {lru_path, lfu_path}

    # Do not clean
    clean.clean_cache_files(2, method=method)
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (fs.du(dirname),) == (2,)

    # Delete one file
    clean.clean_cache_files(1, method=method)
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (fs.du(dirname),) == (1,)
    assert not fs.exists(lru_path if method == "LRU" else lfu_path)


@pytest.mark.parametrize("delete_unknown_files", [True, False])
def test_delete_unknown_files(tmpdir: pathlib.Path, delete_unknown_files: bool) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1")

    # Copy to cache
    cached_file = open_url(tmpfile).path

    # Add unknown
    fs.put(str(tmpfile), f"{dirname}/unknown.txt")

    # Clean one file
    clean.clean_cache_files(1, delete_unknown_files=delete_unknown_files)
    if delete_unknown_files:
        assert fs.ls(dirname) == [cached_file]
    else:
        assert fs.ls(dirname) == [f"{dirname}/unknown.txt"]


@pytest.mark.parametrize(
    "recursive,raises,final_size",
    [
        (True, does_not_raise(), 0),
        (False, pytest.raises((PermissionError, IsADirectoryError, ValueError)), 1),
    ],
)
def test_delete_unknown_dirs(
    recursive: bool, raises: contextlib.nullcontext, final_size: int  # type: ignore[type-arg]
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()
    fs.mkdir(f"{dirname}/unknown")
    fs.touch(f"{dirname}/unknown/unknown.txt")
    with raises:
        clean.clean_cache_files(0, delete_unknown_files=True, recursive=recursive)
    assert len(fs.ls(dirname)) == final_size


@pytest.mark.parametrize("lock_validity_period", [None, 0])
def test_clean_locked_files(
    tmpdir: pathlib.Path, lock_validity_period: Optional[float]
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1")

    # Copy to cache
    cached_file = open_url(tmpfile).path

    # Add unknown and lock
    fs.put(str(tmpfile), f"{dirname}/unknown.txt")
    fs.touch(f"{dirname}/unknown.txt.lock")

    # Clean one file
    clean.clean_cache_files(
        1, delete_unknown_files=True, lock_validity_period=lock_validity_period
    )
    if lock_validity_period == 0:
        assert fs.ls(dirname) == [cached_file]
    else:
        assert set(fs.ls(dirname)) == {
            f"{dirname}/unknown.txt",
            f"{dirname}/unknown.txt.lock",
        }


@pytest.mark.parametrize(
    "tags_to_clean, tags_to_keep, cleaned",
    [
        ({None, "1"}, None, {None, "1"}),
        ({None, "2"}, None, {None, "2"}),
        ({"1", "2"}, None, {"1", "2"}),
        (None, {None}, {"1", "2"}),
        (None, {"1"}, {None, "2"}),
        (None, {"2"}, {None, "1"}),
    ],
)
def test_clean_tagged_files(
    tmpdir: pathlib.Path,
    tags_to_clean: Optional[Sequence[Optional[str]]],
    tags_to_keep: Optional[Sequence[Optional[str]]],
    cleaned: Sequence[Optional[str]],
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    expected_ls = []
    for tag in [None, "1", "2"]:
        tmpfile = tmpdir / f"test_{tag}.txt"
        fsspec.filesystem("file").pipe_file(tmpfile, b"1")
        with config.set(tag=tag):
            cached_file = open_url(tmpfile).path
        if tag not in cleaned:
            expected_ls.append(cached_file)

    clean.clean_cache_files(1, tags_to_clean=tags_to_clean, tags_to_keep=tags_to_keep)
    assert fs.ls(dirname) == expected_ls


def test_clean_tagged_files_wrong_args() -> None:
    with pytest.raises(
        ValueError,
        match="tags_to_clean/keep are mutually exclusive.",
    ):
        clean.clean_cache_files(1, tags_to_keep=[], tags_to_clean=[])


@pytest.mark.parametrize("wrong_type", ["1", [1]])
def test_clean_tagged_files_wrong_types(wrong_type: Any) -> None:
    raises = pytest.raises(
        TypeError,
        match="tags_to_clean/keep must be None or a sequence of str/None.",
    )
    with raises:
        clean.clean_cache_files(1, tags_to_keep=wrong_type)
    with raises:
        clean.clean_cache_files(1, tags_to_clean=wrong_type)


def test_delete_cache_entry_and_files(tmpdir: pathlib.Path) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"old")

    # Copy to cache
    open_url(tmpfile)

    # Change tmp file
    fsspec.filesystem("file").pipe_file(tmpfile, b"new")
    assert open_url(tmpfile).read() == b"old"

    # Delete cache entry
    clean.delete(open_url, tmpfile)
    assert fs.ls(dirname) == []

    # Cache again
    assert open_url(tmpfile).read() == b"new"
    assert len(fs.ls(dirname)) == 1


@pytest.mark.parametrize("check_expiration", [True, False])
@pytest.mark.parametrize("try_decode", [True, False])
def test_clean_invalid_cache_entries(
    tmpdir: pathlib.Path, check_expiration: bool, try_decode: bool
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Valid cache file
    fsspec.filesystem("file").pipe_file(tmpdir / "valid.txt", b"1")
    valid = open_url(tmpdir / "valid.txt").path

    # Corrupted cache file
    fsspec.filesystem("file").pipe_file(tmpdir / "corrupted.txt", b"1")
    corrupted = open_url(tmpdir / "corrupted.txt").path
    fs.touch(corrupted)

    # Expired cache file
    fsspec.filesystem("file").pipe_file(tmpdir / "expired.txt", b"1")
    with config.set(expiration=utils.utcnow() + datetime.timedelta(seconds=0.2)):
        expired = open_url(tmpdir / "expired.txt").path
    time.sleep(0.2)

    # Clean
    clean.clean_invalid_cache_entries(
        check_expiration=check_expiration, try_decode=try_decode
    )

    # Check files
    fs, dirname = utils.get_cache_files_fs_dirname()
    assert valid in fs.ls(dirname)
    assert (
        corrupted not in fs.ls(dirname) if try_decode else corrupted in fs.ls(dirname)
    )
    assert (
        expired not in fs.ls(dirname) if check_expiration else expired in fs.ls(dirname)
    )

    # Check database
    con = config.get().engine.raw_connection()
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (3 - check_expiration - try_decode,)


def test_cleaner_logging(
    capsys: pytest.CaptureFixture[str], tmpdir: pathlib.Path
) -> None:
    # Cache file and create unknown
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1")
    cached_file = open_url(tmpfile)
    fs, dirname = utils.get_cache_files_fs_dirname()
    fs.pipe_file(f"{dirname}/unknown.txt", b"1")

    # Clean
    config.set(logger=structlog.get_logger())
    clean.clean_cache_files(0, delete_unknown_files=True)
    captured = iter(capsys.readouterr().out.splitlines())

    line = next(captured)
    assert "get disk usage of cache files" in line

    line = next(captured)
    assert "get unknown files" in line

    line = next(captured)
    assert "delete unknown" in line
    assert "recursive=False" in line
    assert f"urlpath=file://{dirname}/unknown.txt" in line

    line = next(captured)
    assert "check cache files total size" in line
    assert "size=1" in line

    line = next(captured)
    assert "delete cache entry" in line
    assert "cache_entry=" in line

    line = next(captured)
    assert "delete cache file" in line
    assert f"urlpath=file://{cached_file.path}" in line

    line = next(captured)
    assert "check cache files total size" in line
    assert "size=0" in line
