"""merge heads

Revision ID: 34c54aa164b8
Revises: 20251205_normalize_booking_status_enum, 20251210_recreate_exclusion_constraint_bookings_active_statuses
Create Date: 2025-12-06 08:39:52.868468

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '34c54aa164b8'
down_revision = ('20251205_normalize_booking_status_enum', '20251210_recreate_exclusion_constraint_bookings_active_statuses')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
