import json
import os

import pytest
import sqlalchemy

from cacholote import config


def test_set_engine(tmpdir: str) -> None:
    new_db = "sqlite:///" + os.path.join(tmpdir, "dummy.db")
    old_engine = config.SETTINGS["engine"]
    new_engine = sqlalchemy.create_engine(new_db, echo=True, future=True)
    assert old_engine is not new_engine

    with config.set(engine=new_engine):
        assert config.SETTINGS["engine"] is new_engine
        assert config.SETTINGS["cache_db_urlpath"] is None
    assert config.SETTINGS["engine"] is old_engine
    assert config.SETTINGS["cache_db_urlpath"] == "sqlite:///" + os.path.join(
        tmpdir, "cacholote.db"
    )

    config.set(engine=new_engine)
    assert config.SETTINGS["engine"] is new_engine
    assert config.SETTINGS["cache_db_urlpath"] is None

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


def test_change_settings(tmpdir: str) -> None:
    new_db = "sqlite:///" + os.path.join(tmpdir, "dummy.db")

    with config.set(cache_db_urlpath=new_db):
        assert str(config.SETTINGS["engine"].url) == new_db
    assert str(config.SETTINGS["engine"].url) == "sqlite:///" + os.path.join(
        tmpdir, "cacholote.db"
    )

    config.set(cache_db_urlpath=new_db)
    assert str(config.SETTINGS["engine"].url) == new_db

    with pytest.raises(ValueError, match="Wrong settings. Available settings: "):
        config.set(dummy="dummy")


def test_json_dumps() -> None:
    old_settings = dict(config._SETTINGS)
    json_settings = json.loads(config.json_dumps())

    with config.set(**json_settings):
        new_settings = dict(config._SETTINGS)
        assert old_settings["engine"].url == config.SETTINGS["engine"].url
        assert new_settings.pop("engine") != old_settings.pop("engine")
        assert old_settings == new_settings
        assert json.loads(config.json_dumps()) == json_settings
