"""Add ON DELETE CASCADE for services -> bookings and related FKs

Revision ID: 20251103_add_cascade_services_bookings
Revises: 20251103_add_cascade_master_services_service_id
Create Date: 2025-11-03 09:58:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251103_add_cascade_services_bookings'
down_revision = '20251103_add_cascade_master_services_service_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Alter FKs referencing services to ON DELETE CASCADE where appropriate.

    This migration makes deleting a service cascade to bookings and
    related booking_items and service_profiles so admins can remove
    services without manual FK cleanup.
    """
    # bookings.service_id -> services.id
    with op.batch_alter_table('bookings') as batch_op:
        try:
            batch_op.drop_constraint('bookings_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'bookings_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
            ondelete='CASCADE',
        )

    # booking_items.service_id -> services.id (defensive: cascade)
    with op.batch_alter_table('booking_items') as batch_op:
        try:
            batch_op.drop_constraint('booking_items_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'booking_items_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
            ondelete='CASCADE',
        )

    # service_profiles.service_id -> services.id
    with op.batch_alter_table('service_profiles') as batch_op:
        try:
            batch_op.drop_constraint('service_profiles_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'service_profiles_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
            ondelete='CASCADE',
        )


def downgrade() -> None:
    """Recreate the FKs without ON DELETE CASCADE (rollback).
    """
    with op.batch_alter_table('bookings') as batch_op:
        try:
            batch_op.drop_constraint('bookings_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'bookings_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
        )

    with op.batch_alter_table('booking_items') as batch_op:
        try:
            batch_op.drop_constraint('booking_items_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'booking_items_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
        )

    with op.batch_alter_table('service_profiles') as batch_op:
        try:
            batch_op.drop_constraint('service_profiles_service_id_fkey', type_='foreignkey')
        except Exception:
            pass
        batch_op.create_foreign_key(
            'service_profiles_service_id_fkey',
            'services',
            ['service_id'],
            ['id'],
        )
