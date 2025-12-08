"""add ends_at to bookings

Revision ID: 20251206_add_bookings_ends_at
Revises: ec7fe20db609
Create Date: 2025-12-06 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_add_bookings_ends_at'
down_revision = 'ec7fe20db609'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add column if missing (idempotent)
    op.execute(
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS ends_at TIMESTAMP WITH TIME ZONE;"
    )

    # Populate ends_at where possible. Use defensive checks so this is safe
    # on databases that may not have booking_items or service_profiles yet.
    op.execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'bookings' AND column_name = 'ends_at'
      ) THEN

        -- 1) For bookings that have booking_items, sum durations from booking_items -> service_profiles
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'booking_items')
           AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'service_profiles') THEN
          UPDATE bookings b
          SET ends_at = b.starts_at + (bi_totals.total_minutes || ' minutes')::interval
          FROM (
            SELECT bi.booking_id,
                   SUM(COALESCE(sp.duration_minutes, 60)) AS total_minutes
            FROM booking_items bi
            LEFT JOIN service_profiles sp ON bi.service_id = sp.service_id
            GROUP BY bi.booking_id
          ) bi_totals
          WHERE b.id = bi_totals.booking_id AND b.ends_at IS NULL;
        END IF;

        -- 2) For bookings without booking_items but with a single service_id, use service_profiles.duration_minutes
        IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'bookings' AND column_name = 'service_id')
           AND EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'service_profiles') THEN
          UPDATE bookings b
          SET ends_at = b.starts_at + (COALESCE(sp.duration_minutes, 60) || ' minutes')::interval
          FROM service_profiles sp
          WHERE b.service_id = sp.service_id AND b.ends_at IS NULL;
        END IF;

        -- 3) Fallback: set any remaining NULL ends_at to starts_at + 60 minutes
        UPDATE bookings SET ends_at = starts_at + interval '60 minutes' WHERE ends_at IS NULL;

      END IF;
    END$$;
    """)


def downgrade() -> None:
    # Remove column if it exists. Downgrade should be safe but guarded.
    op.execute("ALTER TABLE IF EXISTS bookings DROP COLUMN IF EXISTS ends_at;")
