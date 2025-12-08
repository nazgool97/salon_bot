"""merge heads

Revision ID: 38c45933ecde
Revises: 20251207_add_index_master_schedules, daa9f5a9a242
Create Date: 2025-12-05 02:12:05.512434

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '38c45933ecde'
down_revision = ('20251207_add_index_master_schedules', 'daa9f5a9a242')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
