"""merge heads

Revision ID: f2078123894b
Revises: 20251207_backfill_settings_from_appsettings, d23fd252d49d
Create Date: 2025-12-05 02:04:21.881427

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f2078123894b'
down_revision = ('20251207_backfill_settings_from_appsettings', 'd23fd252d49d')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
