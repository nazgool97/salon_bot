"""final merge head

Revision ID: 26ff1d0f1b3c
Revises: 20251209_normalize_booking_items, 8e5d0a435ad7
Create Date: 2025-12-04 21:13:09.996769

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '26ff1d0f1b3c'
down_revision = ('20251209_normalize_booking_items', '8e5d0a435ad7')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
