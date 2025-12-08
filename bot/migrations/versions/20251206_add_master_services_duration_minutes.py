"""add duration_minutes to master_services

Revision ID: 20251206_add_master_services_duration_minutes
Revises: 20251206_add_users_is_admin
Create Date: 2025-12-04 11:25:30.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_add_master_services_duration_minutes'
down_revision = '20251206_add_users_is_admin'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent: add nullable integer column if missing and backfill from service_profiles
    op.execute('''
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='master_services' AND column_name='duration_minutes'
        ) THEN
            ALTER TABLE master_services ADD COLUMN duration_minutes INTEGER;
        END IF;

        -- Backfill duration from service_profiles.duration_minutes when possible
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='service_profiles' AND column_name='duration_minutes'
        ) THEN
            UPDATE master_services ms
            SET duration_minutes = sp.duration_minutes
            FROM service_profiles sp
            WHERE ms.service_id = sp.service_id AND ms.duration_minutes IS NULL;
        END IF;
    END
    $$;
    ''')


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS master_services DROP COLUMN IF EXISTS duration_minutes;")
