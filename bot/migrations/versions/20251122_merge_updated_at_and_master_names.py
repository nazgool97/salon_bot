"""Merge migration placeholder for updated_at and master user name changes

Revision ID: 20251122_merge_updated_at_and_master_names
Revises: 0001_initial_schema
Create Date: 2025-11-22 00:00:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "20251122_merge_updated_at_and_master_names"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade():
    # merging two heads; no schema changes.
    pass


def downgrade():
    pass