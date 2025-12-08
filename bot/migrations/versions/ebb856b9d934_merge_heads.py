"""merge heads

Revision ID: ebb856b9d934
Revises: 20251205_add_bookings_exclude_no_overlaps, 49b7bcee3392
Create Date: 2025-12-05 01:12:12.033092

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ebb856b9d934'
down_revision = ('20251205_add_bookings_exclude_no_overlaps', '49b7bcee3392')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
