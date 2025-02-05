from __future__ import annotations

import contextlib
import datetime
import os
import pathlib
import time
from typing import Any, BinaryIO, Literal

import fsspec
import pydantic
import pytest
import pytest_structlog
import structlog

from cacholote import cache, clean, config, utils

ONE_BYTE = os.urandom(1)
TODAY = datetime.datetime.now(tz=datetime.timezone.utc)
TOMORROW = TODAY + datetime.timedelta(days=1)
YESTERDAY = TODAY - datetime.timedelta(days=1)
does_not_raise = contextlib.nullcontext


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@cache.cacheable
def open_urls(*urls: pathlib.Path) -> list[fsspec.spec.AbstractBufferedFile]:
    return [fsspec.open(url).open() for url in urls]


@cache.cacheable
def cached_now(*args: Any, **kwargs: Any) -> datetime.datetime:
    return datetime.datetime.now()


@pytest.mark.parametrize("method", ["LRU", "LFU"])
@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
@pytest.mark.parametrize("folder,depth", [("", 1), ("", 2), ("foo", 2)])
@pytest.mark.parametrize("use_database", [True, False])
def test_clean_cache_files(
    tmp_path: pathlib.Path,
    set_cache: str,
    method: Literal["LRU", "LFU"],
    folder: str,
    depth: int,
    use_database: bool,
) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    cache_files_urlpath = os.path.join(config.get().cache_files_urlpath, folder)
    with config.set(cache_files_urlpath=cache_files_urlpath):
        fs, dirname = utils.get_cache_files_fs_dirname()

        # Create files
        for algorithm in ("LRU", "LFU"):
            filename = tmp_path / f"{algorithm}.txt"
            fsspec.filesystem("file").pipe_file(filename, ONE_BYTE)

        # Copy to cache
        (lru_path,) = {open_url(tmp_path / "LRU.txt").path for _ in range(2)}
        lfu_path = open_url(tmp_path / "LFU.txt").path
        assert set(fs.ls(dirname)) == {lru_path, lfu_path}

    # Do not clean
    clean.clean_cache_files(2, method=method, depth=depth, use_database=use_database)
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (fs.du(dirname),) == (2,)

    # Delete one file
    clean.clean_cache_files(1, method=method, depth=depth, use_database=use_database)
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (fs.du(dirname),) == (1,)
    assert not fs.exists(lru_path if method == "LRU" else lfu_path)


@pytest.mark.parametrize("delete_unknown_files", [True, False])
def test_delete_unknown_files(
    tmp_path: pathlib.Path, delete_unknown_files: bool
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, ONE_BYTE)

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
    recursive: bool,
    raises: contextlib.nullcontext,  # type: ignore[type-arg]
    final_size: int,
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()
    fs.mkdir(f"{dirname}/unknown")
    fs.touch(f"{dirname}/unknown/unknown.txt")
    with raises:
        clean.clean_cache_files(0, delete_unknown_files=True, recursive=recursive)
    assert len(fs.ls(dirname)) == final_size


@pytest.mark.parametrize("lock_validity_period", [None, 0])
@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
def test_clean_locked_files(
    tmp_path: pathlib.Path, set_cache: str, lock_validity_period: float | None
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, ONE_BYTE)

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
        ([None, "1"], None, [None, "1"]),
        ([None, "2"], None, [None, "2"]),
        (["1", "2"], None, ["1", "2"]),
        (None, [None], ["1", "2"]),
        (None, ["1"], [None, "2"]),
        (None, ["2"], [None, "1"]),
    ],
)
def test_clean_tagged_files(
    tmp_path: pathlib.Path,
    tags_to_clean: list[str | None] | None,
    tags_to_keep: list[str | None] | None,
    cleaned: list[str | None],
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    expected_ls = []
    for tag in [None, "1", "2"]:
        tmpfile = tmp_path / f"test_{tag}.txt"
        fsspec.filesystem("file").pipe_file(tmpfile, ONE_BYTE)
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
        clean.clean_cache_files(0, tags_to_keep=[], tags_to_clean=[])


@pytest.mark.parametrize("wrong_type", ["1", [1]])
def test_clean_tagged_files_wrong_types(wrong_type: Any) -> None:
    with pytest.raises(pydantic.ValidationError):
        clean.clean_cache_files(0, tags_to_keep=wrong_type)
    with pytest.raises(pydantic.ValidationError):
        clean.clean_cache_files(0, tags_to_clean=wrong_type)


def test_delete_cache_entry_and_files(tmp_path: pathlib.Path) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Create file
    tmpfile = tmp_path / "test.txt"
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
    tmp_path: pathlib.Path, check_expiration: bool, try_decode: bool
) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    # Valid cache file
    fsspec.filesystem("file").pipe_file(tmp_path / "valid.txt", ONE_BYTE)
    valid = open_url(tmp_path / "valid.txt").path

    # Corrupted cache file
    fsspec.filesystem("file").pipe_file(tmp_path / "corrupted.txt", ONE_BYTE)
    corrupted = open_url(tmp_path / "corrupted.txt").path
    fs.touch(corrupted)

    # Expired cache file
    fsspec.filesystem("file").pipe_file(tmp_path / "expired.txt", ONE_BYTE)
    with config.set(expiration=utils.utcnow() + datetime.timedelta(seconds=0.2)):
        expired = open_url(tmp_path / "expired.txt").path
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
    log: pytest_structlog.StructuredLogCapture, tmp_path: pathlib.Path
) -> None:
    # Cache file and create unknown
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, ONE_BYTE)
    open_url(tmpfile)
    fs, dirname = utils.get_cache_files_fs_dirname()
    fs.pipe_file(f"{dirname}/unknown.txt", ONE_BYTE)

    # Clean
    config.set(logger=structlog.get_logger())
    clean.clean_cache_files(0, delete_unknown_files=True)

    assert log.events == [
        {"event": "getting disk usage", "level": "info"},
        {"disk_usage": 2, "event": "check disk usage", "level": "info"},
        {"event": "getting unknown files", "level": "info"},
        {
            "n_files_to_delete": 1,
            "recursive": False,
            "event": "deleting files",
            "level": "info",
        },
        {"disk_usage": 1, "event": "check disk usage", "level": "info"},
        {"event": "getting cache entries to delete", "level": "info"},
        {"n_entries_to_delete": 1, "event": "deleting cache entries", "level": "info"},
        {
            "n_files_to_delete": 1,
            "recursive": False,
            "event": "deleting files",
            "level": "info",
        },
        {"disk_usage": 0, "event": "check disk usage", "level": "info"},
    ]


def test_clean_multiple_files(tmp_path: pathlib.Path) -> None:
    fs, dirname = utils.get_cache_files_fs_dirname()

    fsspec.filesystem("file").pipe_file(tmp_path / "test1.txt", ONE_BYTE)
    fsspec.filesystem("file").pipe_file(tmp_path / "test2.txt", ONE_BYTE)

    open_urls(tmp_path / "test1.txt", tmp_path / "test2.txt")
    assert len(fs.ls(dirname)) == 2

    clean.clean_cache_files(0)
    assert len(fs.ls(dirname)) == 0


@pytest.mark.parametrize(
    "tags,before,after",
    [
        (["foo"], None, None),
        (None, TOMORROW, None),
        (None, None, YESTERDAY),
        (["foo"], TOMORROW, YESTERDAY),
    ],
)
@pytest.mark.parametrize("delete,n_entries", [(True, 0), (False, 1)])
def test_expire_cache_entries(
    tags: None | list[str],
    before: None | datetime.datetime,
    after: None | datetime.datetime,
    delete: bool,
    n_entries: int,
) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    with config.set(tag="foo"):
        now = cached_now()

    # Do not expire
    count = clean.expire_cache_entries(
        tags=["bar"], before=YESTERDAY, after=TOMORROW, delete=delete
    )
    assert count == 0
    assert now == cached_now()

    # Expire
    count = clean.expire_cache_entries(
        tags=tags, before=before, after=after, delete=delete
    )
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (n_entries,)
    assert count == 1
    assert now != cached_now()


def test_expire_cache_entries_created_at() -> None:
    tic = utils.utcnow()
    _ = cached_now()
    toc = utils.utcnow()
    _ = cached_now()

    assert clean.expire_cache_entries(before=tic) == 0
    assert clean.expire_cache_entries(after=toc) == 0
    assert clean.expire_cache_entries(before=toc) == 1
    assert clean.expire_cache_entries(after=tic) == 1


def test_multiple(tmp_path: pathlib.Path) -> None:
    oldpath = tmp_path / "old.txt"
    oldpath.write_bytes(ONE_BYTE)
    with config.set(cache_files_urlpath=str(tmp_path / "old")):
        cached_oldpath = pathlib.Path(open_url(oldpath).path)
    assert cached_oldpath.exists()

    newpath = tmp_path / "new.txt"
    newpath.write_bytes(ONE_BYTE)
    with config.set(cache_files_urlpath=str(tmp_path / "new")):
        cached_newpath = pathlib.Path(open_url(newpath).path)
    assert cached_newpath.exists()

    with config.set(cache_files_urlpath=str(tmp_path / "new")):
        clean.clean_cache_files(0)
    assert not cached_newpath.exists()
    assert cached_oldpath.exists()


@pytest.mark.parametrize("use_database", [True, False])
def test_clean_multiple_urlpaths(tmp_path: pathlib.Path, use_database: bool) -> None:
    # Create files
    tmpfile1 = tmp_path / "file1.txt"
    fsspec.filesystem("file").pipe_file(tmpfile1, ONE_BYTE)
    tmpfile2 = tmp_path / "file2.txt"
    fsspec.filesystem("file").pipe_file(tmpfile2, ONE_BYTE)

    # Copy to cache
    path1 = tmp_path / "cache_files" / "folder1"
    with config.set(cache_files_urlpath=str(path1 / "today")):
        cached_file1 = pathlib.Path(open_url(tmpfile1).path)
    path2 = tmp_path / "cache_files" / "folder2"
    with config.set(cache_files_urlpath=str(path2 / "today")):
        cached_file2 = pathlib.Path(open_url(tmpfile2).path)

    # Clean
    with config.set(cache_files_urlpath=str(path1)):
        clean.clean_cache_files(maxsize=0, use_database=use_database, depth=2)
    assert not cached_file1.exists()
    assert cached_file2.exists()


def test_clean_duplicates(tmp_path: pathlib.Path) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    # Create file
    tmpfile = tmp_path / "file.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, ONE_BYTE)

    @cache.cacheable
    def func1(path: pathlib.Path) -> BinaryIO:
        return path.open("rb")

    @cache.cacheable
    def func2(path: pathlib.Path) -> BinaryIO:
        return path.open("rb")

    fp1 = func1(tmpfile)
    fp2 = func2(tmpfile)
    assert fp1.name == fp2.name

    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (2,)

    clean.clean_cache_files(maxsize=0)
    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (0,)


@pytest.mark.parametrize(
    "batch_size,batch_delay,expected_time",
    [
        (1, 0, 1),
        (2, 0, 1),
        (1, 1, 2),
        (2, 1, 1),
    ],
)
@pytest.mark.parametrize("delete", [True, False])
def test_expire_cache_entries_batch(
    batch_size: int,
    batch_delay: float,
    expected_time: float,
    delete: bool,
) -> None:
    for i in range(2):
        cached_now(i)

    tic = time.perf_counter()
    count = clean.expire_cache_entries(
        before=TOMORROW,
        batch_size=batch_size,
        batch_delay=batch_delay,
        delete=delete,
    )
    toc = time.perf_counter()
    assert count == 2
    assert expected_time - 1 < toc - tic < expected_time


@pytest.mark.parametrize(
    "batch_size,batch_delay,expected_time",
    [
        (1, 0, 1),
        (2, 0, 1),
        (1, 1, 2),
        (2, 1, 1),
    ],
)
def test_expire_clean_cache_files_batch(
    batch_size: int,
    batch_delay: float,
    expected_time: float,
    tmp_path: pathlib.Path,
) -> None:
    for i in range(2):
        tmpfile1 = tmp_path / f"file{i}.txt"
        fsspec.filesystem("file").pipe_file(tmpfile1, ONE_BYTE)
        open_url(tmpfile1)
    fs, dirname = utils.get_cache_files_fs_dirname()
    assert len(fs.ls(dirname)) == 2

    tic = time.perf_counter()
    clean.clean_cache_files(
        maxsize=0,
        batch_size=batch_size,
        batch_delay=batch_delay,
    )
    toc = time.perf_counter()
    assert expected_time - 1 < toc - tic < expected_time

    fs, dirname = utils.get_cache_files_fs_dirname()
    assert fs.ls(dirname) == []


@pytest.mark.parametrize("dry_run,cache_entries", [(True, 1), (False, 0)])
def test_expire_cache_entries_dry_run(dry_run: bool, cache_entries: int) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    cached_now()
    count = clean.expire_cache_entries(dry_run=dry_run, delete=True, before=TOMORROW)
    assert count == 1

    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (cache_entries,)


@pytest.mark.parametrize(
    "batch_size,batch_delay,expected_time",
    [
        (1, 0, 1),
        (2, 0, 1),
        (1, 1, 2),
        (2, 1, 1),
    ],
)
def test_clean_invalid_cache_entries_batch(
    batch_size: int,
    batch_delay: float,
    expected_time: float,
) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    for i in range(2):
        cached_now(i)
    clean.expire_cache_entries(before=TOMORROW, delete=False)

    tic = time.perf_counter()
    clean.clean_invalid_cache_entries(
        check_expiration=True,
        try_decode=False,
        batch_size=batch_size,
        batch_delay=batch_delay,
    )
    toc = time.perf_counter()
    assert expected_time - 1 < toc - tic < expected_time

    cur.execute("SELECT COUNT(*) FROM cache_entries", ())
    assert cur.fetchone() == (0,)
