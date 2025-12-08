"""merge heads

Revision ID: 98392e138c34
Revises: 20251204_finalize_masters_id_migration, 6dde3cc7c8c7
Create Date: 2025-12-04 23:48:32.067115

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '98392e138c34'
down_revision = ('20251204_finalize_masters_id_migration', '6dde3cc7c8c7')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
