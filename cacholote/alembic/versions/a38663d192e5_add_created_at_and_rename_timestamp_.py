"""add created_at and rename timestamp column.

Revision ID: a38663d192e5
Revises:
Create Date: 2024-07-24 15:53:43.989464

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import cacholote

# revision identifiers, used by Alembic.
revision: str = "a38663d192e5"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "cache_entries",
        sa.Column("created_at", sa.DateTime, default=cacholote.utils.utcnow),
    )
    op.execute(f"UPDATE cache_entries SET created_at='{cacholote.utils.utcnow()!s}'")

    op.alter_column("cache_entries", "timestamp", new_column_name="updated_at")


def downgrade() -> None:
    op.drop_column("cache_entries", "created_at")
    op.alter_column("cache_entries", "updated_at", new_column_name="timestamp")
