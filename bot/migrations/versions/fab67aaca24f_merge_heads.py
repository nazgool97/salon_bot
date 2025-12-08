"""merge heads

Revision ID: fab67aaca24f
Revises: 20251204_unify_booking_status_enum, 576acd128a78
Create Date: 2025-12-04 21:49:52.674318

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fab67aaca24f'
down_revision = ('20251204_unify_booking_status_enum', '576acd128a78')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
