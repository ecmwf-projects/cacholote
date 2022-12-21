import pathlib
from typing import Any

import fsspec
import pytest

from cacholote import cache, config, extra_encoders

pd = pytest.importorskip("pandas")


def test_dictify_pd_dataframe(tmpdir: pathlib.Path) -> None:

    # Create sample dataframe
    df = pd.DataFrame({"foo": [0]})

    # Check dict
    actual = extra_encoders.dictify_pd_dataframe(df)
    local_path = f"{tmpdir}/cache_files/3713087409444908179.csv"
    expected = {
        "type": "python_call",
        "callable": "cacholote.extra_encoders:decode_pd_dataframe",
        "args": (
            {
                "type": "text/csv",
                "href": local_path,
                "file:checksum": fsspec.filesystem("file").checksum(local_path),
                "file:size": 6,
                "file:local_path": local_path,
            },
            {},
        ),
    }
    assert actual == expected


def test_pd_cacheable(tmpdir: pathlib.Path) -> None:
    @cache.cacheable
    def cfunc() -> Any:
        return pd.DataFrame({"foo": [0]})

    # cache-db to check
    con = config.ENGINE.get().raw_connection()
    cur = con.cursor()

    expected = pd.DataFrame({"foo": [0]})
    for expected_counter in (1, 2):
        actual = cfunc()

        # Check hits
        cur.execute("SELECT counter FROM cache_entries", ())
        assert cur.fetchall() == [(expected_counter,)]

        # Check result
        pd.testing.assert_frame_equal(actual, expected)
