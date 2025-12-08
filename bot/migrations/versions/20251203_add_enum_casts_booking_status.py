"""Add safe casts between booking_status and bookingstatus enums

Revision ID: 20251203_add_enum_casts_booking_status
Revises: 20251203_convert_bookings_status_to_bookingstatus
Create Date: 2025-12-03 16:22:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251203_add_enum_casts_booking_status"
down_revision = "20251203_convert_bookings_status_to_bookingstatus"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Create a function to cast booking_status -> bookingstatus via text
    conn.execute(sa.text("""
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE p.proname = 'booking_status_to_bookingstatus'
      ) THEN
        CREATE FUNCTION booking_status_to_bookingstatus(booking_status) RETURNS bookingstatus AS $func$
          SELECT $1::text::bookingstatus;
        $func$ LANGUAGE SQL IMMUTABLE;
      END IF;
    END $do$ LANGUAGE plpgsql;
    """))

    # Create a function to cast bookingstatus -> booking_status via text
    conn.execute(sa.text("""
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE p.proname = 'bookingstatus_to_booking_status'
      ) THEN
        CREATE FUNCTION bookingstatus_to_booking_status(bookingstatus) RETURNS booking_status AS $func$
          SELECT $1::text::booking_status;
        $func$ LANGUAGE SQL IMMUTABLE;
      END IF;
    END $do$ LANGUAGE plpgsql;
    """))

    # Create casts using the functions if they don't already exist
    conn.execute(sa.text("""
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_cast c JOIN pg_type s ON c.castsource = s.oid JOIN pg_type t ON c.casttarget = t.oid WHERE s.typname = 'booking_status' AND t.typname = 'bookingstatus'
      ) THEN
        CREATE CAST (booking_status AS bookingstatus) WITH FUNCTION booking_status_to_bookingstatus(booking_status) AS IMPLICIT;
      END IF;
    END $do$ LANGUAGE plpgsql;
    """))

    conn.execute(sa.text("""
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_cast c JOIN pg_type s ON c.castsource = s.oid JOIN pg_type t ON c.casttarget = t.oid WHERE s.typname = 'bookingstatus' AND t.typname = 'booking_status'
      ) THEN
        CREATE CAST (bookingstatus AS booking_status) WITH FUNCTION bookingstatus_to_booking_status(bookingstatus) AS IMPLICIT;
      END IF;
    END $do$ LANGUAGE plpgsql;
    """))


def downgrade() -> None:
    # Keep downgrade empty to avoid accidental removal of casts/functions relied upon.
    pass
