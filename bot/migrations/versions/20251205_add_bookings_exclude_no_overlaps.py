"""Prevent overlapping bookings per master using GiST exclusion constraint

Revision ID: 20251205_add_bookings_exclude_no_overlaps
Revises: 20251205_consolidate_service_prices
Create Date: 2025-12-05 05:40:00

This guarded migration ensures the `btree_gist` extension exists and then
adds an exclusion constraint on `bookings` to prevent overlapping
bookings for the same `master_id`. It ignores rows whose status is in the
excluded set (e.g. cancelled/expired). The constraint is created only if
it doesn't already exist.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_add_bookings_exclude_no_overlaps"
down_revision = "20251205_consolidate_service_prices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Ensure btree_gist is available (provides GiST support for btree types)
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist;"))

    # Guarded creation: add exclusion constraint only if not present
    # Use tstzrange to include timezone-aware timestamps; compare status
    # to enum-typed literals (immutable) instead of casting to text.
    # Build the ALTER TABLE statement dynamically depending on which
    # booking enum type exists in the database. We prefer comparing
    # against enum-typed literals (immutable). If neither known enum
    # exists, fall back to creating the constraint without a WHERE
    # predicate (safer than failing the migration). This DO block is
    # executed in the DB to avoid relying on Python-side introspection
    # which may be out-of-sync with transactional DDL ordering.
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'bookings_prevent_overlaps_gist'
                ) THEN
                    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status') THEN
                        EXECUTE $cmd$ALTER TABLE public.bookings
                            ADD CONSTRAINT bookings_prevent_overlaps_gist
                            EXCLUDE USING gist (
                                master_id WITH =,
                                tstzrange(starts_at, ends_at) WITH &&
                            )
                            WHERE (status NOT IN ('cancelled'::booking_status, 'expired'::booking_status))$cmd$;
                    ELSIF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'bookingstatus') THEN
                        EXECUTE $cmd$ALTER TABLE public.bookings
                            ADD CONSTRAINT bookings_prevent_overlaps_gist
                            EXCLUDE USING gist (
                                master_id WITH =,
                                tstzrange(starts_at, ends_at) WITH &&
                            )
                            WHERE (status NOT IN ('cancelled'::bookingstatus, 'expired'::bookingstatus))$cmd$;
                    ELSE
                        -- No known booking enum type found; create the constraint
                        -- without a WHERE predicate as a conservative fallback.
                        EXECUTE $cmd$ALTER TABLE public.bookings
                            ADD CONSTRAINT bookings_prevent_overlaps_gist
                            EXCLUDE USING gist (
                                master_id WITH =,
                                tstzrange(starts_at, ends_at) WITH &&
                            )$cmd$;
                    END IF;
                END IF;
            END$$;
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    # Drop the constraint if present (downgrade); keep extension intact.
    conn.execute(sa.text("ALTER TABLE public.bookings DROP CONSTRAINT IF EXISTS bookings_prevent_overlaps_gist;"))
