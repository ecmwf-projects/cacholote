import fsspec
import pytest
import pytest_httpserver

from cacholote import cache, config, extra_encoders, utils


def open_file(urlpath: str) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(urlpath) as f:
        return f


@pytest.mark.parametrize("set_cache", ["file", "ftp", "s3"], indirect=True)
def test_dictify_io_object(tmpdir: str, set_cache: str) -> None:
    tmpfile = f"{tmpdir}/test.txt"
    with open(tmpfile, "wb") as f:
        f.write(b"test")
    checksum = fsspec.filesystem("file").checksum(tmpfile)

    actual = extra_encoders.dictify_io_object(open(tmpfile, "rb"))
    if set_cache == "s3":
        href = actual["href"]
        assert href.startswith(f"http://127.0.0.1:5555/test-bucket/{checksum}.txt")
        storage_options = {}
        fs = fsspec.filesystem("http")
        local_prefix = "s3://test-bucket"
    elif set_cache == "ftp":
        href = f"ftp:///{checksum}.txt"
        storage_options = {
            "host": "localhost",
            "port": 2121,
            "username": "user",
            "password": "pass",
        }
        fs = fsspec.filesystem(set_cache, **storage_options)
        local_prefix = "ftp://"
    else:
        href = f"{set_cache}://{tmpdir}/{checksum}.txt"
        storage_options = {}
        fs = fsspec.filesystem(set_cache)
        local_prefix = tmpdir

    expected = {
        "type": "text/plain",
        "href": href,
        "file:checksum": fs.checksum(href),
        "file:size": 4,
        "file:local_path": f"{local_prefix}/{checksum}.txt",
        "tmp:open_kwargs": {"mode": "rb"},
        "tmp:storage_options": storage_options,
    }
    assert actual == expected


@pytest.mark.parametrize("set_cache", ["file", "ftp", "s3"], indirect=True)
@pytest.mark.parametrize("io_delete_original", [True, False])
def test_copy_from_http_to_cache(
    tmpdir: str,
    set_cache: str,
    io_delete_original: bool,
) -> None:

    tmpfile = f"{tmpdir}/test"
    with open(tmpfile, "wb") as f:
        f.write(b"test")
    cached_basename = str(fsspec.filesystem("file").checksum(tmpfile))

    cfunc = cache.cacheable(open_file)
    infos = []
    for expected_stats in ((0, 1), (1, 1)):
        with config.set(io_delete_original=io_delete_original):
            dirfs = utils.get_cache_files_dirfs()
            result = cfunc(tmpfile)

        # Check hit & miss
        assert config.SETTINGS["cache_store"].stats() == expected_stats

        infos.append(dirfs.info(cached_basename))

        # Check result
        assert result.read() == b"test"

        # Check cache path
        if set_cache == "ftp":
            assert result.path == f"/{cached_basename}"
        elif set_cache == "s3":
            assert result.path.startswith(
                f"http://127.0.0.1:5555/test-bucket/{cached_basename}"
            )
        else:
            assert result.path == f"{tmpdir}/{cached_basename}"

        # Delete original
        assert fsspec.filesystem("file").exists(tmpfile) is not io_delete_original

    # Check cached file is not modified
    assert infos[0] == infos[1]


def test_io_corrupted_files(httpserver: pytest_httpserver.HTTPServer) -> None:
    httpserver.expect_request("/test").respond_with_data(b"test")
    url = httpserver.url_for("/test")
    cached_basename = str(fsspec.filesystem("http").checksum(url))

    cfunc = cache.cacheable(open_file)
    dirfs = utils.get_cache_files_dirfs()
    cfunc(url)

    # Warn if file is corrupted
    dirfs.touch(cached_basename)
    touched_info = dirfs.info(cached_basename)
    with pytest.warns(UserWarning, match="checksum mismatch"):
        result = cfunc(url)
    assert result.read() == b"test"
    assert dirfs.info(cached_basename) != touched_info

    # Warn if file is deleted
    dirfs.rm(cached_basename)
    with pytest.warns(UserWarning, match="No such file or directory"):
        result = cfunc(url)
    assert result.read() == b"test"
    assert dirfs.exists(cached_basename)
