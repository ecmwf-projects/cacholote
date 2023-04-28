import hashlib
import importlib
import io
import pathlib
import threading
from typing import Any, Dict, Tuple, Union

import fsspec
import pytest
import pytest_httpserver

from cacholote import cache, config, decode, encode, extra_encoders, utils


@cache.cacheable
def cached_open(*args: Any, **kwargs: Any) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(*args, **kwargs) as f:
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
    tmp_hash = f"{fsspec.filesystem('file').checksum(tmpfile):x}"

    # Check dict and cached file
    actual = extra_encoders.dictify_io_object(open(tmpfile, "rb"))
    href = f"{readonly_dir}/{tmp_hash}.txt"
    local_path = f"{tmpdir}/cache_files/{tmp_hash}.txt"
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
    obj_hash = hashlib.md5(f"{hash(obj)}".encode()).hexdigest()
    local_path = f"{tmpdir}/cache_files/{obj_hash}"
    checksum = fsspec.filesystem("file").checksum(local_path)
    type = (
        "text/plain"
        if importlib.util.find_spec("magic")
        else "application/octet-stream"
    )
    expected: Tuple[Dict[str, Any], ...] = (
        {
            "type": type,
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
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    # http server
    httpserver.expect_request("/test").respond_with_data(b"test")
    url = httpserver.url_for("/test")
    cached_basename = f"{fsspec.filesystem('http').checksum(url):x}"

    # cache http file
    fs, dirname = utils.get_cache_files_fs_dirname()
    for expected_counter in (1, 2):
        result = cached_open(url)

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
    cached_basename = f"{fsspec.filesystem('http').checksum(url):x}"

    # cache file
    fs, dirname = utils.get_cache_files_fs_dirname()
    cached_open(url)

    # Warn if file is corrupted
    fs.touch(f"{dirname}/{cached_basename}")
    touched_info = fs.info(f"{dirname}/{cached_basename}")
    with pytest.warns(UserWarning, match="checksum mismatch"):
        result = cached_open(url)
    assert result.read() == b"test"
    assert fs.info(f"{dirname}/{cached_basename}") != touched_info

    # Warn if file is deleted
    fs.rm(f"{dirname}/{cached_basename}")
    with pytest.warns(UserWarning, match="No such file or directory"):
        result = cached_open(url)
    assert result.read() == b"test"
    assert fs.exists(f"{dirname}/{cached_basename}")


def test_io_locker_warning(tmpdir: pathlib.Path) -> None:
    cached_open = cache.cacheable(open)

    # Create tmpfile
    tmpfile = tmpdir / "test.txt"
    fsspec.filesystem("file").touch(tmpfile)

    # Acquire lock
    fs, dirname = utils.get_cache_files_fs_dirname()
    file_hash = f"{fsspec.filesystem('file').checksum(tmpfile):x}"
    lock = f"{dirname}/{file_hash}.txt.lock"
    fs.touch(lock)

    def release_lock(fs: fsspec.AbstractFileSystem, lock: str) -> None:
        fs.rm(lock)

    # Threading
    t1 = threading.Timer(0, cached_open, args=(tmpfile,))
    t2 = threading.Timer(0.1, release_lock, args=(fs, lock))
    with pytest.warns(
        UserWarning, match=f"can NOT proceed until file is released: {lock!r}."
    ):
        t1.start()
        t2.start()
        t1.join()
        t2.join()


@pytest.mark.parametrize("set_cache", ["cads"], indirect=True)
def test_content_type(tmpdir: pathlib.Path, set_cache: str) -> None:
    tmpfile = str(tmpdir / "test.grib")
    fsspec.filesystem("file").touch(tmpfile)
    fs, _ = utils.get_cache_files_fs_dirname()
    cached_grib = cached_open(tmpfile)
    assert fs.info(cached_grib)["ContentType"] == "application/x-grib"
