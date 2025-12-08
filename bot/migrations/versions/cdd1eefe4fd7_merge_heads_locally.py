"""merge heads locally

Revision ID: cdd1eefe4fd7
Revises: 20251204_rename_master_client_notes_client_id_to_user_id, 20251205_partial_unique_index_bookings_active
Create Date: 2025-12-04 02:27:09.992305

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cdd1eefe4fd7'
down_revision = ('20251204_rename_master_client_notes_client_id_to_user_id', '20251205_partial_unique_index_bookings_active')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
