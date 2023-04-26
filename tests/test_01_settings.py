import os
import pathlib
from typing import Any, Dict, Union

import pytest
import sqlalchemy as sa

from cacholote import config


def test_change_cache_db_urlpath(tmpdir: pathlib.Path) -> None:
    old_db = config.get().cache_db_urlpath
    new_db = "sqlite:///" + str(tmpdir / "dummy.db")

    with config.set(cache_db_urlpath=new_db):
        assert str(config.get().engine.url) == config.get().cache_db_urlpath == new_db
    assert str(config.get().engine.url) == config.get().cache_db_urlpath == old_db

    config.set(cache_db_urlpath=new_db)
    assert str(config.get().engine.url) == config.get().cache_db_urlpath == new_db


@pytest.mark.parametrize(
    "key, reset",
    [
        ("cache_db_urlpath", True),
        ("create_engine_kwargs", True),
        ("cache_files_urlpath", False),
    ],
)
def test_set_engine_and_sessionmaker(
    tmpdir: pathlib.Path, key: str, reset: bool
) -> None:
    old_engine = config.get().engine
    old_sessionmaker = config.get().sessionmaker

    kwargs: Dict[str, Any] = {}
    if key == "cache_db_urlpath":
        kwargs[key] = "sqlite:///" + str(tmpdir / "dummy.db")
    elif key == "create_engine_kwargs":
        kwargs[key] = {"pool_recycle": 60}
    elif key == "cache_files_urlpath":
        kwargs[key] = str(tmpdir / "dummy_files")
    else:
        raise ValueError

    with config.set(**kwargs):
        if reset:
            assert config.get().engine is not old_engine
            assert config.get().sessionmaker is not old_sessionmaker
        else:
            assert config.get().engine is old_engine
            assert config.get().sessionmaker is old_sessionmaker
    assert config.get().engine is old_engine
    assert config.get().sessionmaker is old_sessionmaker

    config.set(**kwargs)
    if reset:
        assert config.get().engine is not old_engine
        assert config.get().sessionmaker is not old_sessionmaker
    else:
        assert config.get().engine is old_engine
        assert config.get().sessionmaker is old_sessionmaker


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


@pytest.mark.parametrize("poolclass", ("NullPool", sa.pool.NullPool))
def test_set_poolclass(poolclass: Union[str, sa.pool.Pool]) -> None:
    config.set(create_engine_kwargs={"poolclass": poolclass})
    settings = config.get()
    assert settings.create_engine_kwargs["poolclass"] == sa.pool.NullPool
    assert isinstance(settings.engine.pool, sa.pool.NullPool)
