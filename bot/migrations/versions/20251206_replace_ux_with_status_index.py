"""Replace legacy ux index with status-inclusive unique index

Revision ID: 20251206_replace_ux_with_status_index
Revises: 20251206_dedupe_bookings_same_slot
Create Date: 2025-12-04 17:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_replace_ux_with_status_index'
down_revision = '20251206_dedupe_bookings_same_slot'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    sql = sa.text(
        """
        DO $$
        BEGIN
            -- Drop legacy ux index if present (it used status_old predicate)
            IF EXISTS(
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'ux_bookings_master_start_active'
            ) THEN
                DROP INDEX IF EXISTS ux_bookings_master_start_active;
            END IF;

            -- Ensure status_old exists and sync it to status where applicable
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status_old') THEN
                    UPDATE bookings
                    SET status_old = (status::text)::booking_status
                    WHERE status IS NOT NULL
                      AND (status_old::text IS DISTINCT FROM status::text);
                END IF;
            END IF;

            -- Create safe unique index including status as part of the key
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'uq_bookings_master_starts_at_active'
            ) THEN
                CREATE UNIQUE INDEX uq_bookings_master_starts_at_active
                ON public.bookings (master_id, starts_at, status);
            END IF;

        END$$;
        """
    )

    # run under autocommit if possible (safe for enum writes)
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        conn.execute(sql)


def downgrade() -> None:
    conn = op.get_bind()
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'uq_bookings_master_starts_at_active'
            ) THEN
                DROP INDEX IF EXISTS uq_bookings_master_starts_at_active;
            END IF;

            -- no-op for status_old and legacy ux index
        END$$;
        """
    )

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        conn.execute(sql)
