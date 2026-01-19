"""add bio column to masters

Revision ID: 3b7f9a2c52ab
Revises: 9695f0514b8c
Create Date: 2026-01-19 03:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "3b7f9a2c52ab"
down_revision = "9695f0514b8c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("masters", sa.Column("bio", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("masters", "bio")
