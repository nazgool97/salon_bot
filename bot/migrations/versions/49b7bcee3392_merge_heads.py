"""merge heads

Revision ID: 49b7bcee3392
Revises: 059d7e16020d, 20251205_consolidate_service_prices
Create Date: 2025-12-05 01:08:56.622379

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '49b7bcee3392'
down_revision = ('059d7e16020d', '20251205_consolidate_service_prices')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
