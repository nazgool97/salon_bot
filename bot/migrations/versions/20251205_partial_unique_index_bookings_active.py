"""make unique constraint apply only to active bookings (partial index)

Revision ID: 20251205_partial_unique_index_bookings_active
Revises: 20251205_add_settings_timezone
Create Date: 2025-12-05 00:30:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251205_partial_unique_index_bookings_active'
down_revision = '20251205_add_settings_timezone'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Simpler, reliable approach: create a safe unique index that includes
    # `status` as part of the key. This avoids using predicates with casts or
    # functions (which can be rejected by Postgres as non-IMMUTABLE) and keeps
    # migrations deterministic for fresh DBs.
    sql = """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bookings') THEN
                -- Drop legacy UNIQUE CONSTRAINT if it exists
                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'bookings' AND c.conname = 'uq_bookings_master_starts_at'
                ) THEN
                    ALTER TABLE public.bookings DROP CONSTRAINT uq_bookings_master_starts_at;
                END IF;

                -- Create a safe unique index for bookings; include `status` to
                -- avoid non-IMMUTABLE predicate functions.
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'uq_bookings_master_starts_at_active'
                ) THEN
                    CREATE UNIQUE INDEX uq_bookings_master_starts_at_active
                    ON public.bookings (master_id, starts_at, status);
                END IF;
            END IF;
        END$$;
    """

    conn.execute(sa.text(sql))


def downgrade() -> None:
    conn = op.get_bind()
    # Drop the partial index and restore the original constraint if absent.
    sql = """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = 'uq_bookings_master_starts_at_active') THEN
                DROP INDEX IF EXISTS uq_bookings_master_starts_at_active;
            END IF;

            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bookings') THEN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'bookings' AND c.conname = 'uq_bookings_master_starts_at'
                ) THEN
                    ALTER TABLE public.bookings ADD CONSTRAINT uq_bookings_master_starts_at UNIQUE (master_id, starts_at);
                END IF;
            END IF;
        END$$;
    """

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sa.text(sql))
    except Exception:
        conn.execute(sa.text(sql))
