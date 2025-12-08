"""merge heads

Revision ID: a419f6d31173
Revises: 20251205_add_price_to_booking_items, ff4a5289e9a3
Create Date: 2025-12-05 00:42:57.013696

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a419f6d31173'
down_revision = ('20251205_add_price_to_booking_items', 'ff4a5289e9a3')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
