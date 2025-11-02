"""add is_admin column to users

Revision ID: 20251014_add_is_admin
Revises: 0012_master_client_notes
Create Date: 2025-10-13 18:27:44.916454

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251014_add_is_admin'
down_revision = '0012_master_client_notes'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'users',
        sa.Column('is_admin', sa.Boolean(), nullable=False, server_default=sa.false())
    )
    # Drop server_default to avoid locking future inserts to a specific default at DB level (optional)
    op.alter_column('users', 'is_admin', server_default=None)


def downgrade() -> None:
    op.drop_column('users', 'is_admin')
