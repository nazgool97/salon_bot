"""merge heads

Revision ID: 059d7e16020d
Revises: 20251205_add_fk_bookings_master_id, 724007b2c613
Create Date: 2025-12-05 01:00:43.232207

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '059d7e16020d'
down_revision = ('20251205_add_fk_bookings_master_id', '724007b2c613')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
