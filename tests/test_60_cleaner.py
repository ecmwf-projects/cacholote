import pathlib
import time
from typing import Literal

import fsspec
import pytest

from cacholote import cache, cleaner, config, utils


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("append_info", [True, False])
@pytest.mark.parametrize("method", ["LRU", "LFU"])
@pytest.mark.parametrize("set_cache", ["s3", "redis"], indirect=True)
def test_cleaner(
    tmpdir: pathlib.Path,
    set_cache: str,
    append_info: bool,
    method: Literal["LRU", "LFU"],
) -> None:

    with config.set(append_info=append_info):
        fs = utils.get_cache_files_fs()
        dirname = utils.get_cache_files_directory()

        # Create files and copy to cache dir
        checksums = []
        for i in range(3):
            time.sleep(1)
            filename = tmpdir / f"test{i}.txt"
            with open(filename, "w") as f:
                f.write("0")
            checksums.append(fsspec.filesystem("file").checksum(filename))
            open_url(filename)

        # Re-use cache file
        open_url(tmpdir / "test1.txt")  # Most frequently used
        open_url(tmpdir / "test1.txt")  # Most frequently used
        open_url(tmpdir / "test0.txt")  # Most recently used

        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 3

        cleaner.clean_cache_files(3, method=method)
        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 3

        cleaner.clean_cache_files(2, method=method)
        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 2

        cleaner.clean_cache_files(1, method=method)
        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 1

        if not append_info:
            # Last file copied to cache
            checksum = checksums[2]
        elif method == "LFU":
            # Most frequently used
            checksum = checksums[1]
        else:
            # Most recently used
            checksum = checksums[0]
        assert fs.exists(f"{dirname}/{checksum}.txt")


@pytest.mark.parametrize("delete_unknown_files", [True, False])
@pytest.mark.parametrize("set_cache", ["redis"], indirect=True)
def test_clean_unknown(
    tmpdir: pathlib.Path, set_cache: str, delete_unknown_files: bool
) -> None:

    fs = utils.get_cache_files_fs()
    dirname = utils.get_cache_files_directory()

    with fs.open(f"{dirname}/unknown.txt", "wt") as f:
        f.write("0")

    with open(tmpdir / "test.txt", "w") as f:
        f.write("0")
    open_url(tmpdir / "test.txt")

    assert fs.du(dirname) == 2

    cleaner.clean_cache_files(1, delete_unknown_files=delete_unknown_files)
    assert fs.du(dirname) == 1
    assert fs.exists(f"{dirname}/unknown.txt") is not delete_unknown_files
