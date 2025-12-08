"""ensure booking reminder columns exist

Revision ID: 20251206_add_bookings_reminder_columns
Revises: 20251206_add_bookings_ends_at
Create Date: 2025-12-06 12:10:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_add_bookings_reminder_columns'
down_revision = '20251206_add_bookings_ends_at'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add columns if missing
    op.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS remind_24h_sent BOOLEAN;")
    op.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS remind_1h_sent BOOLEAN;")
    op.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS last_reminder_sent_at TIMESTAMP WITH TIME ZONE;")
    op.execute("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS last_reminder_lead_minutes INTEGER;")

    # Set safe defaults for boolean flags and backfill existing rows
    op.execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_24h_sent'
      ) THEN
        EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN remind_24h_sent SET DEFAULT false';
        EXECUTE 'UPDATE public.bookings SET remind_24h_sent = false WHERE remind_24h_sent IS NULL';
      END IF;

      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_1h_sent'
      ) THEN
        EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN remind_1h_sent SET DEFAULT false';
        EXECUTE 'UPDATE public.bookings SET remind_1h_sent = false WHERE remind_1h_sent IS NULL';
      END IF;
    END$$;
    """)


def downgrade() -> None:
    # Drop columns if present (guarded)
    op.execute("ALTER TABLE IF EXISTS bookings DROP COLUMN IF EXISTS remind_24h_sent;")
    op.execute("ALTER TABLE IF EXISTS bookings DROP COLUMN IF EXISTS remind_1h_sent;")
    op.execute("ALTER TABLE IF EXISTS bookings DROP COLUMN IF EXISTS last_reminder_sent_at;")
    op.execute("ALTER TABLE IF EXISTS bookings DROP COLUMN IF EXISTS last_reminder_lead_minutes;")
