"""final merge head

Revision ID: 8e5d0a435ad7
Revises: 20251209_consolidate_service_prices, 9d7329e52544
Create Date: 2025-12-04 21:04:35.211407

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8e5d0a435ad7'
down_revision = ('20251209_consolidate_service_prices', '9d7329e52544')
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
