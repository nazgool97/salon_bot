"""final merge head

Revision ID: 9d7329e52544
Revises: 20251209_unify_booking_status_type, 9154df5cc02c
Create Date: 2025-12-04 20:59:10.294080

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9d7329e52544'
down_revision = ('20251209_unify_booking_status_type', '9154df5cc02c')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
