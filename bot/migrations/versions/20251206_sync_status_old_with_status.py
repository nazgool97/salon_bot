"""Sync bookings.status_old with status when they diverge

Revision ID: 20251206_sync_status_old_with_status
Revises: 20251206_fix_master_client_notes_columns
Create Date: 2025-12-04 12:25:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_sync_status_old_with_status'
down_revision = '20251206_fix_master_client_notes_columns'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # If bookings table exists and both columns exist, update status_old to match status
    # where they diverge. Compare/cast via text to avoid enum-type operator mismatches
    # and run in AUTOCOMMIT where possible (safe for enum operations).
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status_old') THEN
                    -- update only where status is not null and status_old (as text) differs from status (as text)
                    UPDATE bookings
                    SET status_old = (status::text)::booking_status
                    WHERE status IS NOT NULL
                      AND (status_old::text IS DISTINCT FROM status::text);
                END IF;
            END IF;
        END$$;
        """
    )
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        conn.execute(sql)


def downgrade() -> None:
    # No-op: don't revert data changes automatically
    pass
