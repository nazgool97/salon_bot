"""merge all heads — final head december 2025

Revision ID: 6be80c14c432
Revises: 20251203_force_full_enum, 6f11a89a3d86
Create Date: 2025-12-03 15:08:15.582659

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6be80c14c432'
down_revision = ('20251203_force_full_enum', '6f11a89a3d86')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
