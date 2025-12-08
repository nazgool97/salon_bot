"""merge heads

Revision ID: ff4a5289e9a3
Revises: 20251205_add_bookings_master_id_compat, 20251205_drop_legacy_booking_enums
Create Date: 2025-12-05 00:38:01.833034

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ff4a5289e9a3'
down_revision = ('20251205_add_bookings_master_id_compat', '20251205_drop_legacy_booking_enums')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
