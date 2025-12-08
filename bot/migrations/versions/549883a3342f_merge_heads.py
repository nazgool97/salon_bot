"""merge heads

Revision ID: 549883a3342f
Revises: 20251206_consolidate_duration_into_master_services, ebb856b9d934
Create Date: 2025-12-05 01:34:18.698172

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '549883a3342f'
down_revision = ('20251206_consolidate_duration_into_master_services', 'ebb856b9d934')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
