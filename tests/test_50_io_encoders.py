import io
import os
from typing import Union

import fsspec
import pytest

from cacholote import cache, config, extra_encoders


def func(
    use_fsspec: bool,
) -> Union[io.BufferedReader, fsspec.spec.AbstractBufferedFile]:
    url = "https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib"
    if use_fsspec:
        fs = fsspec.filesystem("https")
        f: fsspec.spec.AbstractBufferedFile = fs.open(url, "rb")
        return f

    with fsspec.open(f"simplecache::{url}", simplecache={"same_names": True}) as of:
        return open(of.name, "rb")


@pytest.mark.parametrize("delete_original", [True, False])
def test_dictify_io_object(tmpdir: str, delete_original: bool) -> None:
    tmpfile = os.path.join(tmpdir, "dummy.txt")
    with open(tmpfile, "w") as f:
        f.write("dummy")

    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "f6e6e2cc3b79d2ff7163fe28e6324870bfe8cf16a912dfc2ebceee7a.txt",
    )
    expected = {
        "type": "text/plain",
        "href": local_path,
        "file:checksum": "f6e6e2cc3b79d2ff7163fe28e6324870bfe8cf16a912dfc2ebceee7a",
        "file:size": 5,
        "file:local_path": local_path,
        "tmp:open_kwargs": {"encoding": "UTF-8", "errors": "strict", "mode": "r"},
        "tmp:storage_options": {},
    }
    res = extra_encoders.dictify_io_object(
        open(tmpfile), delete_original=delete_original
    )
    assert res == expected
    assert os.path.exists(local_path)
    assert os.path.exists(tmpfile) is not delete_original


@pytest.mark.parametrize("use_fsspec", [True, False])
def test_copy_file_to_cache_directory(tmpdir: str, use_fsspec: bool) -> None:
    checksum = "b8712c8338bf14a51f18b48294e9675289501d282c1f24d37d1a8995"
    cached_file = os.path.join(
        config.SETTINGS["cache_store"].directory, f"{checksum}.grib"
    )
    cfunc = cache.cacheable(func)

    res = cfunc(use_fsspec)
    assert res.name == cached_file
    assert extra_encoders.hexdigestify_file(res) == checksum
    assert config.SETTINGS["cache_store"].stats() == (0, 1)
    assert len(config.SETTINGS["cache_store"]) == 1

    # skip copying a file already in cache directory
    mtime = os.path.getmtime(cached_file)
    res = cfunc(use_fsspec)
    assert res.name == cached_file
    assert extra_encoders.hexdigestify_file(res) == checksum
    assert mtime == os.path.getmtime(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
    assert len(config.SETTINGS["cache_store"]) == 1

    # do not crash if cached file is removed
    os.remove(cached_file)
    with pytest.warns(UserWarning, match=f"No such file or directory: {cached_file!r}"):
        res = cfunc(use_fsspec)
    assert res.name == cached_file
    assert extra_encoders.hexdigestify_file(res) == checksum
    assert config.SETTINGS["cache_store"].stats() == (2, 1)
    assert len(config.SETTINGS["cache_store"]) == 1
