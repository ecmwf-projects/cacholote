import os
from typing import TypeVar

import pytest

from cacholote import cache, config, extra_encoders

pytest.importorskip("magic")

T = TypeVar("T")


def func(a: T) -> T:
    return a


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
        "href": "./f6e6e2cc3b79d2ff7163fe28e6324870bfe8cf16a912dfc2ebceee7a.txt",
        "file:checksum": "f6e6e2cc3b79d2ff7163fe28e6324870bfe8cf16a912dfc2ebceee7a",
        "file:size": 5,
        "file:local_path": local_path,
        "tmp:open_kwargs": {"encoding": "UTF-8", "errors": "strict", "mode": "r"},
    }
    res = extra_encoders.dictify_io_object(
        open(tmpfile), delete_original=delete_original
    )
    assert res == expected
    assert os.path.exists(local_path)
    assert os.path.exists(tmpfile) is not delete_original


def test_copy_file_to_cache_directory(tmpdir: str) -> None:
    tmpfile = os.path.join(tmpdir, "dummy.txt")
    cached_file = os.path.join(
        config.SETTINGS["cache_store"].directory,
        "6b4e03423667dbb73b6e15454f0eb1abd4597f9a1b078e3f5b5a6bc7.txt",
    )

    with open(tmpfile, "w") as f:
        f.write("dummy")
    cfunc = cache.cacheable(func)

    res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    with open(cached_file, "r") as f:
        assert f.read() == "dummy"
    assert config.SETTINGS["cache_store"].stats() == (0, 1)

    # skip copying a file already in cache directory
    mtime = os.path.getmtime(cached_file)
    res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    assert mtime == os.path.getmtime(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (1, 1)

    # do not crash if cached file is removed
    os.remove(cached_file)
    with pytest.warns(UserWarning):
        res = cfunc(open(tmpfile))
    assert res.read() == "dummy"
    assert os.path.exists(cached_file)
    assert config.SETTINGS["cache_store"].stats() == (2, 1)
