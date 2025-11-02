"""Placeholder migration to satisfy legacy dependency.

Revision ID: 0006_add_price_tracking_columns
Revises: 0001_initial_schema
Create Date: 2025-10-01 12:00:00.000000

This file is a no-op placeholder because legacy runtime expects the
revision '0006_add_price_tracking_columns'. The actual schema changes
were consolidated into the new baseline migration 0001_initial_schema.
Once all references are updated, this file can be removed safely.
"""

import sqlalchemy as sa  # noqa
from alembic import op  # noqa

# revision identifiers, used by Alembic.
revision = "0006_add_price_tracking_columns"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:  # no-op
    pass


def downgrade() -> None:  # no-op
    pass
