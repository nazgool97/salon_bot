"""Add ON DELETE CASCADE for master_services.service_id FK

Revision ID: 20251103_add_cascade_master_services_service_id
Revises: 20251103_add_cascade_master_services_master_telegram_id
Create Date: 2025-11-03 09:50:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251103_add_cascade_master_services_service_id'
down_revision = '20251103_add_cascade_master_services_master_telegram_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Alter FK on master_services.service_id to ON DELETE CASCADE.

    This migration drops the existing FK constraint and recreates it with
    ON DELETE CASCADE so deleting a service will automatically remove the
    related rows in master_services.
    """
    with op.batch_alter_table('master_services') as batch_op:
        try:
            batch_op.drop_constraint('master_services_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_services_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
            ondelete='CASCADE',
        )


def downgrade() -> None:
    """Recreate the FK without ON DELETE CASCADE (rollback).
    """
    with op.batch_alter_table('master_services') as batch_op:
        try:
            batch_op.drop_constraint('master_services_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_services_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
        )
