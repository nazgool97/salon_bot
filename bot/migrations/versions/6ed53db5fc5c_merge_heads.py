"""merge heads

Revision ID: 6ed53db5fc5c
Revises: 20251205_drop_service_profiles_merge_audit, a419f6d31173
Create Date: 2025-12-05 00:48:16.368344

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6ed53db5fc5c'
down_revision = ('20251205_drop_service_profiles_merge_audit', 'a419f6d31173')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
