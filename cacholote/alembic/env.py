"""module for entry points."""

import alembic.context
import sqlalchemy as sa

import cacholote


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = alembic.context.config.get_main_option("sqlalchemy.url")
    alembic.context.configure(
        url=url,
        target_metadata=cacholote.database.Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table="alembic_version_cacholote",
    )
    with alembic.context.begin_transaction():
        alembic.context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url = alembic.context.config.get_main_option("sqlalchemy.url")
    engine = sa.create_engine(url, poolclass=sa.pool.NullPool)  # type: ignore
    with engine.connect() as connection:
        alembic.context.configure(
            connection=connection,
            target_metadata=cacholote.database.Base.metadata,
            version_table="alembic_version_cacholote",
        )

        with alembic.context.begin_transaction():
            alembic.context.run_migrations()


if alembic.context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
