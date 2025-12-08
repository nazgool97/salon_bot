"""merge heads

Revision ID: 835d03546932
Revises: 20251204_drop_master_profiles_master_id, 26ff1d0f1b3c
Create Date: 2025-12-04 21:39:15.440137

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '835d03546932'
down_revision = ('20251204_drop_master_profiles_master_id', '26ff1d0f1b3c')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
