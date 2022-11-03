import pathlib
import sqlite3
import threading
import time

import fsspec
import pytest
import pytest_httpserver

from cacholote import cache, config, decode, encode, extra_encoders, utils


def open_url(url: str) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("io_delete_original", [True, False])
def test_dictify_io_object(tmpdir: pathlib.Path, io_delete_original: bool) -> None:
    # Define readonly dir
    readonly_dir = tmpdir / "readonly"
    fsspec.filesystem("file").mkdir(readonly_dir)
    config.set(
        io_delete_original=io_delete_original, cache_files_urlpath_readonly=readonly_dir
    )

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"test")
    tmp_checksum = fsspec.filesystem("file").checksum(tmpfile)

    # Check dict and cached file
    actual = extra_encoders.dictify_io_object(open(tmpfile, "rb"))
    href = f"{readonly_dir}/{tmp_checksum}.txt"
    local_path = f"{tmpdir}/cache_files/{tmp_checksum}.txt"
    checksum = fsspec.filesystem("file").checksum(local_path)
    expected = {
        "type": "python_call",
        "callable": "cacholote.extra_encoders:decode_io_object",
        "args": (
            {
                "type": "text/plain",
                "href": href,
                "file:checksum": checksum,
                "file:size": 4,
                "file:local_path": local_path,
            },
            {},
        ),
        "kwargs": {"mode": "rb"},
    }
    assert actual == expected
    assert fsspec.filesystem("file").exists(tmpfile) is not io_delete_original

    # Use href when local_path is missing or corrupted
    fsspec.filesystem("file").mv(local_path, href)
    assert decode.loads(encode.dumps(actual)).read() == b"test"


@pytest.mark.parametrize("set_cache", ["file", "s3"], indirect=True)
def test_copy_from_http_to_cache(
    tmpdir: pathlib.Path,
    httpserver: pytest_httpserver.HTTPServer,
    set_cache: str,
) -> None:

    # cache-db to check
    con = sqlite3.connect(tmpdir / "cacholote.db")
    cur = con.cursor()

    # http server
    httpserver.expect_request("/test").respond_with_data(b"test")
    url = httpserver.url_for("/test")
    cached_basename = str(fsspec.filesystem("http").checksum(url))

    # cache http file
    fs, dirname = utils.get_cache_files_fs_dirname()
    cfunc = cache.cacheable(open_url)
    for expected_counter in (1, 2):
        result = cfunc(url)

        # Check hits
        cur.execute("SELECT counter FROM cache_entries")
        assert cur.fetchall() == [(expected_counter,)]

        # Check result
        assert result.read() == b"test"

        # Check file in cache
        assert result.path == f"{dirname}/{cached_basename}"
        assert fs.exists(result.path)


def test_io_corrupted_files(
    tmpdir: pathlib.Path, httpserver: pytest_httpserver.HTTPServer
) -> None:

    # http server
    httpserver.expect_request("/test").respond_with_data(b"test")
    url = httpserver.url_for("/test")
    cached_basename = str(fsspec.filesystem("http").checksum(url))

    # cache file
    fs, dirname = utils.get_cache_files_fs_dirname()
    cfunc = cache.cacheable(open_url)
    cfunc(url)

    # Warn if file is corrupted
    fs.touch(f"{dirname}/{cached_basename}")
    touched_info = fs.info(f"{dirname}/{cached_basename}")
    with pytest.warns(UserWarning, match="checksum mismatch"):
        result = cfunc(url)
    assert result.read() == b"test"
    assert fs.info(f"{dirname}/{cached_basename}") != touched_info

    # Warn if file is deleted
    fs.rm(f"{dirname}/{cached_basename}")
    with pytest.warns(UserWarning, match="No such file or directory"):
        result = cfunc(url)
    assert result.read() == b"test"
    assert fs.exists(f"{dirname}/{cached_basename}")


@pytest.mark.parametrize("set_cache", ["file", "s3"], indirect=True)
def test_io_concurrent_jobs(tmpdir: pathlib.Path, set_cache: bool) -> None:
    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1" * 1_000_000)

    # Cached open
    cfunc = cache.cacheable(open)

    # Threading
    t1 = threading.Thread(target=cfunc, args=(tmpfile,))
    t2 = threading.Thread(target=cfunc, args=(tmpfile,))
    with pytest.warns(
        UserWarning, match="can NOT proceed until the cache entry is unlocked"
    ):
        t1.start()
        time.sleep(0.001)
        t2.start()
        t1.join()
        t2.join()

    # Check hits
    con = sqlite3.connect(tmpdir / "cacholote.db")
    cur = con.cursor()
    cur.execute("SELECT counter FROM cache_entries")
    assert cur.fetchall() == [(2,)]
