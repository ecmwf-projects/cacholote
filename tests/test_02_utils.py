import fsspec

from cacholote import utils


def test_hexdigestify() -> None:
    text = "some random Unicode text \U0001f4a9"
    expected = "278a2cefeef9a3269f4ba1c41ad733a4c07101ae6859f607c8a42cf2"
    res = utils.hexdigestify(text)
    assert res == expected


def test_get_cache_files(tmpdir: str) -> None:
    assert utils.get_cache_files_directory() == tmpdir

    assert utils.get_cache_files_dirfs().path == tmpdir
    assert utils.get_cache_files_dirfs().fs == fsspec.filesystem("file")
