"""merge heads

Revision ID: daa9f5a9a242
Revises: 20251207_archive_and_drop_app_settings, f2078123894b
Create Date: 2025-12-05 02:07:46.757697

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'daa9f5a9a242'
down_revision = ('20251207_archive_and_drop_app_settings', 'f2078123894b')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
