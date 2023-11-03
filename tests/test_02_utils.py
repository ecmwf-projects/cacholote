import pathlib

import fsspec

from cacholote import utils


def test_hexdigestify() -> None:
    text = "some random Unicode text \U0001f4a9"
    expected = "278a2cefeef9a3269f4ba1c41ad733a4"
    res = utils.hexdigestify(text)
    assert res == expected


def test_get_cache_files(tmp_path: pathlib.Path) -> None:
    assert utils.get_cache_files_fs_dirname() == (
        fsspec.filesystem("file"),
        str(tmp_path / "cache_files"),
    )


def test_copy_buffered_file(tmp_path: pathlib.Path) -> None:
    src = tmp_path / "test0"
    dst = tmp_path / "test1"
    with open(src, "wb") as f:
        f.write(b"test")
    with open(src, "rb") as f_src, open(dst, "wb") as f_dst:
        utils.copy_buffered_file(f_src, f_dst)
    assert open(src, "rb").read() == open(dst, "rb").read() == b"test"
