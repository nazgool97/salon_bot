"""Merge heads 20251119061000_add_updated_at_columns and 20251122_add_master_user_names

Revision ID: 20251122_merge_updated_at_and_master_names
Revises: 20251119061000_add_updated_at_columns, 20251122_add_master_user_names
Create Date: 2025-11-22 00:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "20251122_merge_updated_at_and_master_names"
down_revision = (
    "20251119061000_add_updated_at_columns",
    "20251122_add_master_user_names",
)
branch_labels = None
depends_on = None


def upgrade():
    # merging two heads; no schema changes.
    pass


def downgrade():
    pass