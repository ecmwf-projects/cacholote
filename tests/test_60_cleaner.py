import pathlib
import time

import fsspec
import pytest

from cacholote import cache, cleaner, config, utils


@cache.cacheable
def open_url(url: pathlib.Path) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("append_info", [True, False])
@pytest.mark.parametrize("set_cache", ["s3", "redis"], indirect=True)
def test_cleaner(tmpdir: pathlib.Path, set_cache: str, append_info: bool) -> None:

    with config.set(append_info=append_info):
        fs = utils.get_cache_files_fs()
        dirname = utils.get_cache_files_directory()

        # Create files and copy to cache dir
        for i in range(3):
            time.sleep(1)
            filename = tmpdir / f"test{i}.txt"
            with open(filename, "w") as f:
                f.write(str(i))
            open_url(filename)

        # Re-use cache file
        open_url(tmpdir / "test0.txt")

        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 3

        cleaner.cache_files_cleaner(2)
        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 2

        cleaner.cache_files_cleaner(1)
        assert len(list(utils.cache_store_keys_iter())) == fs.du(dirname) == 1

        filename, *_ = fs.ls(dirname)
        with fs.open(filename, "rt") as f:
            assert f.read() == "0" if append_info else "2"
