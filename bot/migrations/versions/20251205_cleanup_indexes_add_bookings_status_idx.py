"""Cleanup redundant indexes and add bookings(status, starts_at) index

Revision ID: 20251205_cleanup_indexes_add_bookings_status_idx
Revises: 20251205_drop_service_profiles_merge_audit
Create Date: 2025-12-05 04:15:00

This guarded migration removes leftover indexes that reference
`*_master_id_new` names (only if they are not backing a constraint),
and adds an index on `bookings(status, starts_at)` to speed queries that
filter by status and order/limit by start time.

All destructive operations are guarded and will only drop indexes that
are not used by primary/unique constraints.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_cleanup_indexes_add_bookings_status_idx"
down_revision = "20251205_drop_service_profiles_merge_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Drop leftover indexes matching '%_master_id_new' only when they are
    #    not used by a constraint (primary/unique). Executed in a DO $$ block.
    conn.execute(
        sa.text(
            """
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN SELECT indexname FROM pg_indexes WHERE indexname LIKE '%_master_id_new' LOOP
                    -- ensure index is not backing a constraint
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint c
                        JOIN pg_class i ON i.oid = c.conindid
                        WHERE i.relname = r.indexname
                    ) THEN
                        RAISE NOTICE 'Dropping index %', r.indexname;
                        EXECUTE format('DROP INDEX IF EXISTS %I', r.indexname);
                    ELSE
                        RAISE NOTICE 'Skipping index % (backed by constraint)', r.indexname;
                    END IF;
                END LOOP;
            END$$;
            """
        )
    )

    # 2) Create an index on bookings(status, starts_at) if missing. Use a
    #    guarded DO block to avoid syntax issues with asyncpg prepared stmts.
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_indexes WHERE tablename = 'bookings' AND indexname = 'ix_bookings_status_starts_at'
                ) THEN
                    EXECUTE 'CREATE INDEX ix_bookings_status_starts_at ON public.bookings (status, starts_at)';
                END IF;
            END$$;
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    # Downgrade will drop the newly added index if present. We do not
    # attempt to recreate dropped indexes automatically.
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_bookings_status_starts_at;"))
