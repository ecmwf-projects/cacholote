import os

import fsspec
import pytest

from cacholote import cache, config, extra_encoders


def func(url: str) -> fsspec.spec.AbstractBufferedFile:
    with fsspec.open(url, "rb") as f:
        return f


@pytest.mark.parametrize("delete_original", [True, False])
def test_dictify_io_object(tmpdir: str, delete_original: bool) -> None:
    tmpfile = os.path.join(tmpdir, "dummy.txt")
    with open(tmpfile, "w") as f:
        f.write("dummy")
    checksum = str(fsspec.filesystem("file").checksum(tmpfile))

    local_path = os.path.join(
        config.SETTINGS["cache_store"].directory,
        f"{checksum}.txt",
    )
    expected = {
        "type": "text/plain",
        "href": local_path,
        "file:checksum": checksum,
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


def test_copy_file_to_cache_directory(tmpdir: str) -> None:
    url = "https://github.com/ecmwf/cfgrib/raw/master/tests/sample-data/era5-levels-members.grib"
    checksum = "142252380851833733959491685103259827168"
    cached_file = os.path.join(
        config.SETTINGS["cache_store"].directory, f"{checksum}.grib"
    )
    cfunc = cache.cacheable(func)

    res = cfunc(url)
    assert res.name == cached_file
    assert config.SETTINGS["cache_store"].stats() == (0, 1)
    assert len(config.SETTINGS["cache_store"]) == 1

    # skip copying a file already in cache directory
    mtime = os.path.getmtime(cached_file)
    res = cfunc(url)
    assert res.name == cached_file
    assert mtime == os.path.getmtime(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)
    assert len(config.SETTINGS["cache_store"]) == 1

    # do not crash if cached file is removed
    os.remove(cached_file)
    with pytest.warns(UserWarning, match=f"No such file or directory: {cached_file!r}"):
        res = cfunc(url)
    assert res.name == cached_file
    assert config.SETTINGS["cache_store"].stats() == (2, 1)
    assert len(config.SETTINGS["cache_store"]) == 1
