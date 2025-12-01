"""Add structured display name columns for masters and users

Revision ID: 20251122_add_master_user_names
Revises: 9b55e7a43db8
Create Date: 2025-11-22 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251122_add_master_user_names'
down_revision = '9b55e7a43db8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('masters', sa.Column('username', sa.String(length=64), nullable=True))
    op.add_column('masters', sa.Column('first_name', sa.String(length=80), nullable=True))
    op.add_column('masters', sa.Column('last_name', sa.String(length=80), nullable=True))
    op.add_column('users', sa.Column('first_name', sa.String(length=80), nullable=True))
    op.add_column('users', sa.Column('last_name', sa.String(length=80), nullable=True))


def downgrade():
    op.drop_column('users', 'last_name')
    op.drop_column('users', 'first_name')
    op.drop_column('masters', 'last_name')
    op.drop_column('masters', 'first_name')
    op.drop_column('masters', 'username')
