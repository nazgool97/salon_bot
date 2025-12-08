"""merge heads

Revision ID: 6dde3cc7c8c7
Revises: 20251204_expand_exclusion_constraint_bookings_statuses, 8494de10cc35
Create Date: 2025-12-04 23:42:23.274455

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6dde3cc7c8c7'
down_revision = ('20251204_expand_exclusion_constraint_bookings_statuses', '8494de10cc35')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
