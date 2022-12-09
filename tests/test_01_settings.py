import contextvars
import datetime
import json
import os
import pathlib

import pytest
import sqlalchemy

from cacholote import config


def test_set_engine(tmpdir: pathlib.Path) -> None:
    old_db = config.SETTINGS["cache_db_urlpath"]
    new_db = "sqlite:///" + str(tmpdir / "dummy.db")
    old_engine = config.SETTINGS.get()["engine"]
    new_engine = sqlalchemy.create_engine(new_db, echo=True, future=True)
    assert old_engine is not new_engine

    with config.set(engine=new_engine):
        assert config.SETTINGS.get()["engine"] is new_engine
        assert config.SETTINGS.get()["cache_db_urlpath"] is None
    assert config.SETTINGS.get()["engine"] is old_engine
    assert config.SETTINGS.get()["cache_db_urlpath"] == old_db

    config.set(engine=new_engine)
    assert config.SETTINGS.get()["engine"] is new_engine
    assert config.SETTINGS.get()["cache_db_urlpath"] is None

    with pytest.raises(
        ValueError,
        match=r"'engine' and 'cache_db_urlpath' are mutually exclusive",
    ):
        config.set(engine=new_engine, cache_db_urlpath=new_db)

    with pytest.raises(
        ValueError,
        match=r"Can NOT dump to JSON when `engine` has been directly set.",
    ):
        config.json_dumps()


def test_change_settings(tmpdir: pathlib.Path) -> None:
    old_db = config.SETTINGS["cache_db_urlpath"]
    new_db = "sqlite:///" + str(tmpdir / "dummy.db")

    with config.set(cache_db_urlpath=new_db):
        assert str(config.SETTINGS.get()["engine"].url) == new_db
    assert str(config.SETTINGS.get()["engine"].url) == old_db

    config.set(cache_db_urlpath=new_db)
    assert str(config.SETTINGS.get()["engine"].url) == new_db

    with pytest.raises(
        ValueError, match="Wrong settings: {'dummy'}. Available settings: "
    ):
        config.set(dummy="dummy")


def test_json_dumps() -> None:
    old_settings = dict(config.SETTINGS.get())
    json_settings = json.loads(config.json_dumps())

    with config.set(**json_settings):
        new_settings = dict(config.SETTINGS.get())
        assert old_settings["engine"].url == config.SETTINGS.get()["engine"].url
        assert new_settings.pop("engine") != old_settings.pop("engine")
        assert old_settings == new_settings
        assert json.loads(config.json_dumps()) == json_settings


def test_expiration() -> None:
    with config.set(expiration=datetime.datetime(1492, 10, 12)):
        assert config.SETTINGS.get()["expiration"] == "1492-10-12T00:00:00"

    with config.set(expiration="1492-10-12T00:00:00"):
        assert config.SETTINGS.get()["expiration"] == "1492-10-12T00:00:00"


def test_env_variables() -> None:
    old_environ = dict(os.environ)
    os.environ.update(
        {
            "CACHOLOTE_CACHE_DB_URLPATH": "sqlite://",
            "CACHOLOTE_IO_DELETE_ORIGINAL": "TRUE",
        }
    )
    config._initialize_settings()
    try:
        assert config.SETTINGS.get()["cache_db_urlpath"] == "sqlite://"
        assert config.SETTINGS.get()["io_delete_original"] is True
        assert str(config.SETTINGS.get()["engine"].url) == "sqlite://"
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_contextvar() -> None:
    def set_tag() -> None:
        config.set(tag="foo")

    ctx = contextvars.copy_context()
    ctx.run(set_tag)

    assert config.SETTINGS.get()["tag"] is None
    assert ctx[config.SETTINGS] == {
        k: v if k != "tag" else "foo" for k, v in config.SETTINGS.get().items()
    }
