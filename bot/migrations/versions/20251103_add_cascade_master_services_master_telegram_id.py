"""Add ON DELETE CASCADE for master_services.master_telegram_id FK

Revision ID: 20251103_add_cascade_master_services_master_telegram_id
Revises: 20251029_add_index_bookings_master_starts_at
Create Date: 2025-11-03 03:30:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251103_add_cascade_master_services_master_telegram_id'
down_revision = '20251029_add_index_bookings_master_starts_at'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Alter FK on master_services.master_telegram_id to ON DELETE CASCADE.

    This migration drops the existing FK constraint and recreates it with
    ON DELETE CASCADE so deleting a master will automatically remove the
    related rows in master_services. The constraint name used here is the
    default Postgres name produced by SQLAlchemy when creating the FK in
    the initial migration: "master_services_master_telegram_id_fkey".
    If your DB uses a different constraint name, adjust accordingly.
    """
    with op.batch_alter_table('master_services') as batch_op:
        # drop existing FK constraint and recreate it with ON DELETE CASCADE
        try:
            batch_op.drop_constraint('master_services_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            # best-effort: if the auto-generated name differs, let the create call run
            pass
        batch_op.create_foreign_key(
            'master_services_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
            ondelete='CASCADE',
        )


def downgrade() -> None:
    """Recreate the FK without ON DELETE CASCADE (rollback).

    Note: downgrading will restore the FK but without cascade. If the
    constraint name differs in your environment you may need to adapt this
    migration when downgrading.
    """
    with op.batch_alter_table('master_services') as batch_op:
        try:
            batch_op.drop_constraint('master_services_master_telegram_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'master_services_master_telegram_id_fkey',
            'masters',
            ['master_telegram_id'],
            ['telegram_id'],
        )
