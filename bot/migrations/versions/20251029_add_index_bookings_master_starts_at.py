"""add composite index on bookings(master_id, starts_at)

Revision ID: 20251029_add_index_bookings_master_starts_at
Revises: 20251029_add_username_to_users
Create Date: 2025-10-29 12:30:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251029_add_index_bookings_master_starts_at'
down_revision = '20251029_add_username_to_users'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create a non-unique composite index on (master_id, starts_at).

    This improves queries that filter by master_id and a starts_at range.
    """
    # Use an explicit name to avoid colliding with other indexes
    op.create_index('ix_bookings_master_starts_at', 'bookings', ['master_id', 'starts_at'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_bookings_master_starts_at', table_name='bookings')
