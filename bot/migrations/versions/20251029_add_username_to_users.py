"""add username column to users

Revision ID: 20251029_add_username_to_users
Revises: 20251022_add_duration_minutes_and_price_cents_to_service_profiles
Create Date: 2025-10-29 12:00:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251029_add_username_to_users'
down_revision = '20251022_add_duration_minutes_and_price_cents_to_service_profiles'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add nullable username column to users table.

    The column is optional and stores Telegram username without the leading '@'.
    """
    with op.batch_alter_table('users') as batch_op:
        batch_op.add_column(sa.Column('username', sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('users') as batch_op:
        batch_op.drop_column('username')
