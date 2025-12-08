"""merge heads

Revision ID: 724007b2c613
Revises: 20251205_cleanup_indexes_add_bookings_status_idx, 6ed53db5fc5c
Create Date: 2025-12-05 00:52:49.957908

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '724007b2c613'
down_revision = ('20251205_cleanup_indexes_add_bookings_status_idx', '6ed53db5fc5c')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
