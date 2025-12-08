"""merge heads

Revision ID: d23fd252d49d
Revises: 20251207_drop_duplicate_master_fks, 549883a3342f
Create Date: 2025-12-05 01:52:28.754644

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd23fd252d49d'
down_revision = ('20251207_drop_duplicate_master_fks', '549883a3342f')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
