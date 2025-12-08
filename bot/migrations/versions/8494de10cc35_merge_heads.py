"""merge heads

Revision ID: 8494de10cc35
Revises: 13773aaa69d1, 20251204_drop_audit_tables
Create Date: 2025-12-04 23:29:47.214142

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8494de10cc35'
down_revision = ('13773aaa69d1', '20251204_drop_audit_tables')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
