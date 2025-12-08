"""merge heads

Revision ID: 85674cb6b28d
Revises: 0b5b3de67b66, 20251204_add_masters_surrogate_id
Create Date: 2025-12-04 22:18:15.110449

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '85674cb6b28d'
down_revision = ('0b5b3de67b66', '20251204_add_masters_surrogate_id')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
