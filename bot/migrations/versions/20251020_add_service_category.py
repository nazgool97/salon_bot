"""add nullable category column to services

Revision ID: 20251020_add_service_category
Revises: 20251016_add_new_statuses
Create Date: 2025-10-20 10:00:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251020_add_service_category'
down_revision = '20251016_add_new_statuses'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add nullable category column to services
    with op.batch_alter_table('services') as batch_op:
        batch_op.add_column(sa.Column('category', sa.String(length=100), nullable=True))
        batch_op.create_index('ix_services_category', ['category'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('services') as batch_op:
        batch_op.drop_index('ix_services_category')
        batch_op.drop_column('category')
