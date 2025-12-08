"""merge heads

Revision ID: 9154df5cc02c
Revises: 20251206_delete_pending_6969151921_20251208, 20251209_recreate_partial_unique_index_bookings_active
Create Date: 2025-12-04 20:15:29.065841

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9154df5cc02c'
down_revision = ('20251206_delete_pending_6969151921_20251208', '20251209_recreate_partial_unique_index_bookings_active')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
