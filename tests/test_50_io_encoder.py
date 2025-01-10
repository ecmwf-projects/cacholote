from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import pathlib
import subprocess
from typing import Any

import fsspec
import fsspec.implementations.local
import pytest
import pytest_httpserver
import pytest_structlog
import structlog

from cacholote import cache, config, decode, encode, extra_encoders, utils


@cache.cacheable
def cached_open(*args: Any, **kwargs: Any) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(*args, **kwargs) as f:
        return f


@pytest.mark.parametrize("io_delete_original", [True, False])
def test_dictify_io_object(tmp_path: pathlib.Path, io_delete_original: bool) -> None:
    # Define readonly dir
    readonly_dir = str(tmp_path / "readonly")
    fsspec.filesystem("file").mkdir(readonly_dir)
    config.set(
        io_delete_original=io_delete_original, cache_files_urlpath_readonly=readonly_dir
    )

    # Create file
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").pipe_file(tmpfile, b"test")
    tmp_hash = f"{fsspec.filesystem('file').checksum(tmpfile):x}"

    # Check dict and cached file
    actual = extra_encoders.dictify_io_object(open(tmpfile, "rb"))
    href = f"{readonly_dir}/{tmp_hash}.txt"
    local_path = f"{tmp_path}/cache_files/{tmp_hash}.txt"
    expected = {
        "type": "python_call",
        "callable": "cacholote.extra_encoders:decode_io_object",
        "args": (
            {
                "type": "text/plain",
                "href": href,
                "file:checksum": f"{fsspec.filesystem('file').checksum(local_path):x}",
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
    tmp_path: pathlib.Path, obj: io.BytesIO | io.StringIO
) -> None:
    actual = extra_encoders.dictify_io_object(obj)["args"]
    obj_hash = hashlib.md5(f"{hash(obj)}".encode()).hexdigest()
    local_path = f"{tmp_path}/cache_files/{obj_hash}"
    type = (
        "text/plain"
        if importlib.util.find_spec("magic")
        else "application/octet-stream"
    )
    expected: tuple[dict[str, Any], ...] = (
        {
            "type": type,
            "href": local_path,
            "file:checksum": f"{fsspec.filesystem('file').checksum(local_path):x}",
            "file:size": 4,
            "file:local_path": local_path,
        },
        {},
    )
    assert actual == expected
    assert open(local_path).read() == "test"


@pytest.mark.parametrize("set_cache", ["file", "cads"], indirect=True)
def test_copy_from_http_to_cache(
    tmp_path: pathlib.Path,
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
    tmp_path: pathlib.Path, httpserver: pytest_httpserver.HTTPServer
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


@pytest.mark.parametrize(
    "lock_timeout, raises_or_warns",
    (
        [None, pytest.warns(UserWarning, match="is locked")],
        [0, pytest.raises(TimeoutError, match="is locked")],
    ),
)
def test_io_locker(
    tmp_path: pathlib.Path,
    lock_timeout: float | None,
    raises_or_warns: contextlib.nullcontext,  # type: ignore[type-arg]
) -> None:
    config.set(lock_timeout=lock_timeout, raise_all_encoding_errors=True)
    # Create tmpfile
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").touch(tmpfile)

    # Acquire lock
    fs, dirname = utils.get_cache_files_fs_dirname()
    file_path = f"{dirname}/{fsspec.filesystem('file').checksum(tmpfile):x}.txt"
    fs.touch(file_path)
    fs.touch(f"{file_path}.lock")

    process = subprocess.Popen(f"sleep 0.2; rm {file_path}.lock", shell=True)
    with raises_or_warns:
        cached_open(tmpfile)
    assert process.wait() == 0


@pytest.mark.parametrize("set_cache", ["cads"], indirect=True)
def test_content_type(tmp_path: pathlib.Path, set_cache: str) -> None:
    tmpfile = str(tmp_path / "test.grib")
    fsspec.filesystem("file").touch(tmpfile)
    fs, _ = utils.get_cache_files_fs_dirname()
    cached_grib = cached_open(tmpfile)
    assert fs.info(cached_grib)["ContentType"] == "application/x-grib"


@pytest.mark.parametrize("set_cache", ["cads"], indirect=True)
def test_io_logging(
    log: pytest_structlog.StructuredLogCapture, tmp_path: pathlib.Path
) -> None:
    config.set(logger=structlog.get_logger(), io_delete_original=True)

    # Cache file
    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").touch(tmpfile)
    cached_file = cached_open(tmpfile)

    tmp_urlpath = f"file://{tmpfile!s}"
    cached_urlpath = f"s3://{cached_file.path}"
    expected = [
        {
            "urlpath": cached_urlpath,
            "size": 0,
            "event": "start upload",
            "level": "info",
        },
        {
            "urlpath": cached_urlpath,
            "size": 0,
            "upload_time": log.events[1]["upload_time"],
            "event": "end upload",
            "level": "info",
        },
        {
            "urlpath": tmp_urlpath,
            "size": 0,
            "event": "start remove",
            "level": "info",
        },
        {
            "urlpath": tmp_urlpath,
            "size": 0,
            "remove_time": log.events[3]["remove_time"],
            "event": "end remove",
            "level": "info",
        },
        {
            "urlpath": cached_urlpath,
            "event": "retrieve cache file",
            "level": "info",
        },
    ]
    assert log.events == expected


def test_io_in_place_file(tmp_path: pathlib.Path) -> None:
    @cache.cacheable
    def cached_in_place_open(path: str) -> io.FileIO:
        return extra_encoders.InPlaceFile(path, "rb")

    tmpfile = tmp_path / "test.txt"
    fsspec.filesystem("file").touch(tmpfile)

    output = cached_in_place_open(str(tmpfile))
    assert isinstance(output, fsspec.implementations.local.LocalFileOpener)
    assert output.name == str(tmpfile)
