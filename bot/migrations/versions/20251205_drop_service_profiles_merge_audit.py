"""Drop transient `service_profiles_merge_audit` table after backing it up

Revision ID: 20251205_drop_service_profiles_merge_audit
Revises: 20251205_add_price_to_booking_items
Create Date: 2025-12-05 03:45:00

This guarded migration will, if present, back up the
`service_profiles_merge_audit` table into a timestamped backup table and
then drop the original audit table. The backup uses a timestamped name to
avoid accidental overwrites. The operation is executed inside a single
`DO $$` block to be safe with asyncpg / prepared-statement execution.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_drop_service_profiles_merge_audit"
down_revision = "20251205_add_price_to_booking_items"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # If the transient audit table exists, back it up to
    # service_profiles_merge_audit_backup_<YYYYMMDDHH24MISS> and drop the
    # original. Do this in a single DO $$ block so asyncpg treats it as a
    # single prepared statement.
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'service_profiles_merge_audit'
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE public.service_profiles_merge_audit_backup_%s AS SELECT * FROM public.service_profiles_merge_audit',
                        to_char(now(), 'YYYYMMDDHH24MISS')
                    );
                    EXECUTE 'DROP TABLE public.service_profiles_merge_audit';
                END IF;
            END$$;
            """
        )
    )


def downgrade() -> None:
    # Downgrade: do nothing. Restoring from backup is a manual operation.
    pass
