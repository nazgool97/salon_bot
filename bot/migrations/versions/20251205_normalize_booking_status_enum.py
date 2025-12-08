"""
Normalize booking_status enum: remap legacy values and remove them from the enum.

Mapping applied (idempotent):
 - 'active' -> 'reserved'
 - 'awaiting_cash' -> 'pending_payment'

Strategy:
 - Create a temporary enum type with the desired final labels
 - Update rows to remap legacy values
 - Drop DEFAULT on the column (required!)
 - Alter the column to use the new enum type
 - Rename old enum -> *_old
 - Rename new enum -> canonical
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_normalize_booking_status_enum"
down_revision = "20251205_add_indexes_bookings_user_master_starts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Create the new enum
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized_new') THEN
                    CREATE TYPE booking_status_normalized_new AS ENUM (
                        'reserved',
                        'pending_payment',
                        'confirmed',
                        'paid',
                        'cancelled',
                        'done',
                        'no_show',
                        'expired'
                    );
                END IF;
            END$$;
            """
        )
    )

    # 2) Remap legacy values (safe/idempotent)
    conn.execute(sa.text("UPDATE bookings SET status = 'pending_payment' WHERE status = 'awaiting_cash';"))
    conn.execute(sa.text("UPDATE bookings SET status = 'reserved' WHERE status = 'active';"))

    # 2.5) Drop exclusion constraint/index that references `status` to avoid operator/type mismatches
    # when we ALTER the column type. We'll recreate it after the enum rename below.
    conn.execute(sa.text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_prevent_overlaps_gist;"))

    # 3) Drop default before altering type (CRITICAL)
    conn.execute(
        sa.text("ALTER TABLE bookings ALTER COLUMN status DROP DEFAULT;")
    )

    # 4) Alter the column to the new enum
    conn.execute(
        sa.text(
            """
            ALTER TABLE bookings
                ALTER COLUMN status
                TYPE booking_status_normalized_new
                USING status::text::booking_status_normalized_new;
            """
        )
    )

    # 5) Restore default (optional — choose your default)
    conn.execute(
        sa.text("ALTER TABLE bookings ALTER COLUMN status SET DEFAULT 'pending_payment';")
    )

    # 6) Rename old and new enum types
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                -- rename old enum
                IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized')
                   AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized_old') THEN
                    ALTER TYPE booking_status_normalized RENAME TO booking_status_normalized_old;
                END IF;

                -- rename new -> canonical
                IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized_new')
                   AND NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized') THEN
                    ALTER TYPE booking_status_normalized_new RENAME TO booking_status_normalized;
                END IF;
            END$$;
            """
        )
    )

    # 7) Recreate exclusion constraint if missing, casting literals to the current enum type
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bookings_prevent_overlaps_gist') THEN
                    IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized') THEN
                        EXECUTE $cmd$ALTER TABLE public.bookings
                            ADD CONSTRAINT bookings_prevent_overlaps_gist
                            EXCLUDE USING gist (
                                master_id WITH =,
                                tstzrange(starts_at, ends_at) WITH &&
                            )
                            WHERE (status NOT IN ('cancelled'::booking_status_normalized, 'expired'::booking_status_normalized));$cmd$;
                    ELSE
                        -- Fallback: add constraint without predicate if enum type missing
                        EXECUTE $cmd$ALTER TABLE public.bookings
                            ADD CONSTRAINT bookings_prevent_overlaps_gist
                            EXCLUDE USING gist (
                                master_id WITH =,
                                tstzrange(starts_at, ends_at) WITH &&
                            );$cmd$;
                    END IF;
                END IF;
            END$$;
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()

    # Best-effort restore
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized_old') THEN

                    -- try dropping current canonical
                    BEGIN
                        DROP TYPE booking_status_normalized;
                    EXCEPTION WHEN others THEN
                        NULL;
                    END;

                    -- restore old
                    ALTER TYPE booking_status_normalized_old RENAME TO booking_status_normalized;
                END IF;

            END$$;
            """
        )
    )
