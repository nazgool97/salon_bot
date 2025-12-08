"""merge heads

Revision ID: 0b5b3de67b66
Revises: 20251204_add_exclusion_constraint_no_overlap, fab67aaca24f
Create Date: 2025-12-04 22:06:29.777762

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0b5b3de67b66'
down_revision = ('20251204_add_exclusion_constraint_no_overlap', 'fab67aaca24f')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
