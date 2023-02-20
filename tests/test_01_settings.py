import os
import pathlib

from cacholote import config


def test_change_engine(tmpdir: pathlib.Path) -> None:
    old_db = config.get().cache_db_urlpath
    new_db = "sqlite:///" + str(tmpdir / "dummy.db")
    old_engine = config.get().engine

    with config.set(cache_db_urlpath=new_db):
        assert config.get().engine is not old_engine
        assert str(config.get().engine.url) == config.get().cache_db_urlpath == new_db
    assert config.get().engine is old_engine
    assert str(config.get().engine.url) == config.get().cache_db_urlpath == old_db

    config.set(cache_db_urlpath=new_db)
    assert config.get().engine is not old_engine
    assert str(config.get().engine.url) == config.get().cache_db_urlpath == new_db


def test_expiration() -> None:
    with config.set(expiration="1492-10-12T00:00:00"):
        assert config.get().expiration == "1492-10-12T00:00:00"


def test_env_variables(tmpdir: pathlib.Path) -> None:
    # env variables
    old_environ = dict(os.environ)
    os.environ["CACHOLOTE_CACHE_DB_URLPATH"] = "sqlite://"

    # env file
    dotenv_path = tmpdir / ".env.cacholote"
    with dotenv_path.open("w") as f:
        f.write("CACHOLOTE_IO_DELETE_ORIGINAL=TRUE")

    config.reset(str(dotenv_path))
    try:
        assert config.get().cache_db_urlpath == "sqlite://"
        assert str(config.get().engine.url) == "sqlite://"
        assert config.get().io_delete_original is True
        assert str(config.get().engine.url) == "sqlite://"
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
