"""adding index key-expiration.

Revision ID: fac14caeb10d
Revises: a38663d192e5
Create Date: 2025-03-06 11:43:44.497574

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fac14caeb10d"
down_revision: Union[str, None] = "a38663d192e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_cache_entries_key_expiration", "cache_entries", ["key", "expiration"]
    )


def downgrade() -> None:
    op.drop_index("ix_cache_entries_key_expiration", "cache_entries")
