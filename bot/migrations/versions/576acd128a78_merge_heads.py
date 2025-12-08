"""merge heads

Revision ID: 576acd128a78
Revises: 20251204_migrate_settings_to_jsonb, 835d03546932
Create Date: 2025-12-04 21:43:16.433828

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '576acd128a78'
down_revision = ('20251204_migrate_settings_to_jsonb', '835d03546932')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
