"""Deduplicate bookings for identical master+start slots

Revision ID: 20251206_dedupe_bookings_same_slot
Revises: 20251206_sync_status_old_with_status
Create Date: 2025-12-04 12:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_dedupe_bookings_same_slot'
down_revision = '20251206_sync_status_old_with_status'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Guarded, idempotent dedupe: for any group with the same (master_id, starts_at)
    # keep the smallest id and set other bookings' status to CANCELLED.
    # This uses information_schema checks so it is safe on older DBs.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='id')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='master_id')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='starts_at')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status') THEN

                    WITH duplicates AS (
                        SELECT master_id, starts_at, array_agg(id ORDER BY id) AS ids
                        FROM bookings
                        GROUP BY master_id, starts_at
                        HAVING count(*) > 1
                    ),
                    to_cancel AS (
                        SELECT ids[idx]::bigint AS id_to_cancel
                        FROM duplicates, generate_subscripts(duplicates.ids, 1) idx
                        WHERE idx > 1
                    )
                    -- perform a safe update: turn duplicate rows into CANCELLED
                    UPDATE bookings
                    SET status = 'CANCELLED'::booking_status
                    WHERE id IN (SELECT id_to_cancel FROM to_cancel)
                      AND status::text IS DISTINCT FROM 'CANCELLED';

                END IF;
            END IF;
        END$$;
        """
    )

    # Try to execute under AUTOCOMMIT where available to avoid issues with enum writes.
    # If this fails (for example because the enum values are still new in this
    # alembic run) we intentionally swallow the error and skip this data-cleanup
    # step to allow the rest of migrations to proceed. A follow-up data-cleanup
    # migration can run later when enums are committed.
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        # Swallow errors such as UnsafeNewEnumValueUsageError and continue.
        pass


def downgrade() -> None:
    # no-op: we do not revert data-cleaning automatically
    pass
