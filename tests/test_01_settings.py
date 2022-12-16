import contextvars
import os
import pathlib

from cacholote import config


def test_change_engine(tmpdir: pathlib.Path) -> None:
    old_db = config.SETTINGS.get().cache_db_urlpath
    new_db = "sqlite:///" + str(tmpdir / "dummy.db")
    old_engine = config.ENGINE.get()

    with config.set(cache_db_urlpath=new_db):
        assert config.ENGINE.get() is not old_engine
        assert (
            str(config.ENGINE.get().url)
            == config.SETTINGS.get().cache_db_urlpath
            == new_db
        )
    assert config.ENGINE.get() is old_engine
    assert (
        str(config.ENGINE.get().url) == config.SETTINGS.get().cache_db_urlpath == old_db
    )

    config.set(cache_db_urlpath=new_db)
    assert config.ENGINE.get() is not old_engine
    assert (
        str(config.ENGINE.get().url) == config.SETTINGS.get().cache_db_urlpath == new_db
    )


def test_expiration() -> None:
    with config.set(expiration="1492-10-12T00:00:00"):
        assert config.SETTINGS.get().expiration == "1492-10-12T00:00:00"


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
        assert config.SETTINGS.get().cache_db_urlpath == "sqlite://"
        assert str(config.ENGINE.get().url) == "sqlite://"
        assert config.SETTINGS.get().io_delete_original is True
        assert str(config.ENGINE.get().url) == "sqlite://"
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def test_contextvar() -> None:
    def set_tag() -> None:
        config.set(tag="foo")

    ctx = contextvars.copy_context()
    ctx.run(set_tag)

    assert config.SETTINGS.get().tag is None
    assert ctx[config.SETTINGS].tag == "foo"
