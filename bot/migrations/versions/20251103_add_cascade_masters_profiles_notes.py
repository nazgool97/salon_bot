"""Add ON DELETE CASCADE for master_profiles and master_client_notes FKs

Revision ID: 20251103_add_cascade_masters_profiles_notes
Revises: 20251103_add_cascade_services_bookings
Create Date: 2025-11-03 10:05:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251103_add_cascade_masters_profiles_notes'
down_revision = '20251103_add_cascade_services_bookings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Alter master-related FKs to ON DELETE CASCADE.

    This allows deleting a master to automatically remove profile and
    per-master notes so the admin can delete the master when appropriate.
    """
    with op.batch_alter_table('master_profiles') as batch_op:
        try:
            batch_op.drop_constraint('master_profiles_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_profiles_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
            ondelete='CASCADE',
        )

    with op.batch_alter_table('master_client_notes') as batch_op:
        try:
            batch_op.drop_constraint('master_client_notes_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_client_notes_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
            ondelete='CASCADE',
        )


def downgrade() -> None:
    with op.batch_alter_table('master_profiles') as batch_op:
        try:
            batch_op.drop_constraint('master_profiles_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_profiles_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
        )

    with op.batch_alter_table('master_client_notes') as batch_op:
        try:
            batch_op.drop_constraint('master_client_notes_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_client_notes_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
        )
