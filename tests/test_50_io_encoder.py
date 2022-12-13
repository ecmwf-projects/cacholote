import contextvars
import importlib
import io
import pathlib
import threading
import time
from typing import Any, Dict, List, Tuple, Union

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
    readonly_dir = str(tmpdir / "readonly")
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


@pytest.mark.parametrize("obj", [io.BytesIO(b"test"), io.StringIO("test")])
def test_dictify_bytes_io_object(
    tmpdir: pathlib.Path, obj: Union[io.BytesIO, io.StringIO]
) -> None:
    actual = extra_encoders.dictify_io_object(obj)["args"]
    local_path = f"{tmpdir}/cache_files/{hash(obj)}"
    checksum = fsspec.filesystem("file").checksum(local_path)
    expected: Tuple[Dict[str, Any], ...] = (
        {
            "type": "text/plain" if importlib.util.find_spec("magic") else "unknown",
            "href": local_path,
            "file:checksum": checksum,
            "file:size": 4,
            "file:local_path": local_path,
        },
        {},
    )
    assert actual == expected
    assert open(local_path).read() == "test"


@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
def test_copy_from_http_to_cache(
    tmpdir: pathlib.Path,
    httpserver: pytest_httpserver.HTTPServer,
    set_cache: str,
) -> None:

    # cache-db to check
    con = config.ENGINE.get().raw_connection()
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
        cur.execute("SELECT counter FROM cache_entries", ())
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


@pytest.mark.parametrize(
    "wait,lag,size,mode1,mode2,warning,expected,set_cache",
    [
        (0.2, 0, 0, "r", "r", "cache entry", [(2,)], "file"),
        (0.2, 0, 0, "r", "r", "cache entry", [(2,)], "cads"),
        (0, 1.0e-5, 10_000_000, "r", "rb", "file", [(1,), (1,)], "file"),
    ],
    indirect=["set_cache"],
)
def test_io_concurrent_calls(
    tmpdir: pathlib.Path,
    wait: float,
    lag: float,
    size: int,
    mode1: str,
    mode2: str,
    warning: str,
    expected: List[Any],
    set_cache: str,
) -> None:
    @cache.cacheable
    def wait_and_open(urlpath: str, mode: str) -> fsspec.spec.AbstractBufferedFile:
        time.sleep(wait)
        with fsspec.open(urlpath, mode) as f:
            return f

    # Create file
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"1" * size)

    ctx = contextvars.copy_context()
    try:
        # Threading
        t1 = threading.Timer(
            0, wait_and_open, args=(tmpfile, mode1), kwargs={"__context__": ctx}
        )
        t2 = threading.Timer(
            (wait / 2) + lag,
            wait_and_open,
            args=(tmpfile, mode2),
            kwargs={"__context__": ctx},
        )
        with pytest.warns(UserWarning, match=warning):
            t1.start()
            t2.start()
            t1.join()
            t2.join()

        # Check hits
        con = config.ENGINE.get().raw_connection()
        cur = con.cursor()
        cur.execute("SELECT counter FROM cache_entries", ())
        assert cur.fetchall() == expected
    finally:
        # Cleanup
        fsspec.filesystem("file").rm(tmpfile)
        fs, dirname = utils.get_cache_files_fs_dirname()
        fs.rm(dirname, recursive=True)
