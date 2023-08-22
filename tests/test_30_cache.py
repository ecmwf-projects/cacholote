import contextlib
import datetime
import json
import pathlib
import time
from typing import Any

import pytest

from cacholote import cache, config, database


def func(a: Any, *args: Any, b: Any = None, **kwargs: Any) -> Any:
    if b is None:
        return locals()
    else:

        class LocalClass:
            pass

        return LocalClass()


@cache.cacheable
def cached_now() -> datetime.datetime:
    return datetime.datetime.now()


@cache.cacheable
def cached_error() -> None:
    raise ValueError("test error")


def test_cacheable(tmpdir: pathlib.Path) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    cfunc = cache.cacheable(func)

    for counter in range(1, 3):
        before = datetime.datetime.now(tz=datetime.timezone.utc)
        res = cfunc("test")
        after = datetime.datetime.now(tz=datetime.timezone.utc)
        assert res == {"a": "test", "args": [], "b": None, "kwargs": {}}

        cur.execute(
            "SELECT id, key, expiration, result, counter FROM cache_entries", ()
        )
        assert cur.fetchall() == [
            (
                1,
                "a8260ac3cdc1404aa64a6fb71e853049",
                "9999-12-31 00:00:00.000000",
                '{"a": "test", "b": null, "args": [], "kwargs": {}}',
                counter,
            )
        ]

        cur.execute("SELECT timestamp FROM cache_entries", ())
        (timestamp,) = cur.fetchone() or []
        assert before < datetime.datetime.fromisoformat(timestamp + "+00:00") < after


@pytest.mark.parametrize("raise_all_encoding_errors", [True, False])
def test_encode_errors(tmpdir: pathlib.Path, raise_all_encoding_errors: bool) -> None:
    config.set(raise_all_encoding_errors=raise_all_encoding_errors)

    cfunc = cache.cacheable(func)

    class Dummy:
        pass

    inst = Dummy()

    if raise_all_encoding_errors:
        with pytest.raises(AttributeError):
            cfunc(inst)
    else:
        with pytest.warns(UserWarning, match="AttributeError"):
            res = cfunc(inst)
        assert res == {"a": inst, "args": (), "b": None, "kwargs": {}}

    if raise_all_encoding_errors:
        with pytest.raises(AttributeError):
            cfunc("test", b=1)
    else:
        with pytest.warns(UserWarning, match="AttributeError"):
            res = cfunc("test", b=1)
        assert res.__class__.__name__ == "LocalClass"

    # Check cache-db
    con = config.get().engine.raw_connection()
    cur = con.cursor()
    cur.execute("SELECT result, log FROM cache_entries", ())
    all = cur.fetchall()
    assert len(all) == 1
    result, log = all[0]
    assert result is None
    assert "AttributeError" in json.loads(log)["exception"]


def test_same_args_kwargs() -> None:
    ufunc = cache.cacheable(func)

    con = config.get().engine.raw_connection()
    cur = con.cursor()

    ufunc(1)
    cur.execute("SELECT id, key, counter FROM cache_entries", ())
    assert cur.fetchall() == [(1, "54f546036ae7dccdd0155893189154c0", 1)]

    ufunc(a=1)
    cur.execute("SELECT id, key, counter FROM cache_entries", ())
    assert cur.fetchall() == [(1, "54f546036ae7dccdd0155893189154c0", 2)]


@pytest.mark.parametrize("use_cache", [True, False])
def test_use_cache(use_cache: bool) -> None:
    config.set(use_cache=use_cache)

    if use_cache:
        assert cached_now() == cached_now()
    else:
        assert cached_now() < cached_now()


def test_expiration_and_return_cache_entry() -> None:
    config.set(return_cache_entry=True)
    first: database.CacheEntry = cached_now()  # type: ignore[assignment]
    assert first.id == 1
    assert first.key == "c3d9e414d0d32337c3672cb29b1b3cc9"
    assert first.expiration == datetime.datetime(9999, 12, 31)

    dt = datetime.timedelta(seconds=0.1)
    expiration = datetime.datetime.now(tz=datetime.timezone.utc) + dt
    with config.set(expiration=expiration):
        second: database.CacheEntry = cached_now()  # type: ignore[assignment]
        assert second.result != first.result
        assert second.id == 2
        assert second.key == "c3d9e414d0d32337c3672cb29b1b3cc9"
        assert (
            second.expiration is not None
            and second.expiration.isoformat() + "+00:00" == expiration.isoformat()
        )

    time.sleep(0.1)
    third: database.CacheEntry = cached_now()  # type: ignore[assignment]
    assert third.result == first.result
    assert third.id == 1
    assert third.key == "c3d9e414d0d32337c3672cb29b1b3cc9"
    assert third.expiration == datetime.datetime(9999, 12, 31)


def test_tag(tmpdir: pathlib.Path) -> None:
    con = config.get().engine.raw_connection()
    cur = con.cursor()

    cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [(None, 1)]

    with config.set(tag="1"):
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("1", 2)]

    with config.set(tag="2"):
        # Overwrite
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("2", 3)]

    with config.set(tag=None):
        # Do not overwrite if None
        cached_now()
    cur.execute("SELECT tag, counter FROM cache_entries", ())
    assert cur.fetchall() == [("2", 4)]


@pytest.mark.parametrize(
    "return_cache_entry,raises_or_warns",
    [
        (True, pytest.warns(UserWarning, match="ValueError.*test error")),
        (False, pytest.raises(ValueError, match="test error")),
    ],
)
def test_cached_error(
    return_cache_entry: bool, raises_or_warns: contextlib.nullcontext  # type: ignore[type-arg]
) -> None:
    config.set(return_cache_entry=return_cache_entry)

    con = config.get().engine.raw_connection()
    cur = con.cursor()

    with raises_or_warns:
        cache_entry = cached_error()
        assert isinstance(cache_entry, database.CacheEntry)
        assert cache_entry.result is None
        assert "ValueError" in cache_entry.log["exception"]

    cur.execute("SELECT result, log FROM cache_entries", ())
    all = cur.fetchall()
    assert len(all) == 1
    result, log = all[0]
    assert result is None
    assert "ValueError" in json.loads(log)["exception"]
