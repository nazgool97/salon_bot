"""merge heads

Revision ID: 13773aaa69d1
Revises: 20251204_merge_service_profiles_into_services, 85674cb6b28d
Create Date: 2025-12-04 23:23:00.097325

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '13773aaa69d1'
down_revision = ('20251204_merge_service_profiles_into_services', '85674cb6b28d')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
