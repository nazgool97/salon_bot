"""merge heads after partial index + timezone

Revision ID: 6efd1a181179
Revises: 20251205_partial_unique_index_bookings_active, 5a785d91f3a6
Create Date: 2025-12-04 02:05:30.792770

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6efd1a181179'
down_revision = ('20251205_partial_unique_index_bookings_active', '5a785d91f3a6')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
