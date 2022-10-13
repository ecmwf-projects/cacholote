import pathlib
import time

import fsspec
import pytest

from cacholote import cache, cleaner, config, utils


@cache.cacheable
def open_url(url: str) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url) as f:
        return f


@pytest.mark.parametrize("append_info,lastfile", [(True, "0"), (False, "2")])
@pytest.mark.parametrize("set_cache", ["s3"], indirect=True)
def test_cleaner(
    tmpdir: pathlib.Path, set_cache: str, append_info: bool, lastfile: str
) -> None:

    with config.set(append_info=append_info):
        fs = utils.get_cache_files_fs()
        dirname = utils.get_cache_files_directory()

        for i in range(3):
            time.sleep(1)
            filename = str(tmpdir / f"test{i}.txt")
            with open(filename, "w") as f:
                f.write(str(i))
            open_url(filename)
        open_url(str(tmpdir / "test0.txt"))

        assert len(config.SETTINGS["cache_store"]) == fs.du(dirname) == 3

        cleaner.cache_files_cleaner(2)
        assert len(config.SETTINGS["cache_store"]) == fs.du(dirname) == 2

        cleaner.cache_files_cleaner(1)
        assert len(config.SETTINGS["cache_store"]) == fs.du(dirname) == 1

        filename, *_ = fs.ls(dirname)
        with fs.open(filename, "rt") as f:
            assert f.read() == lastfile
