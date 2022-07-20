import callcache


def test_version() -> None:
    assert callcache.__version__ != "999"
