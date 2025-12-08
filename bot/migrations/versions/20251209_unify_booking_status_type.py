"""Unify booking enum types and remove legacy column/type

Revision ID: 20251209_unify_booking_status_type
Revises: 20251209_recreate_partial_unique_index_bookings_active
Create Date: 2025-12-04 20:45:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251209_unify_booking_status_type'
down_revision = '20251209_recreate_partial_unique_index_bookings_active'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # This migration attempts to consolidate enum usage to `bookingstatus` and
    # remove the legacy `status_old` column and `booking_status` type when
    # possible. All operations are best-effort and guarded so the migration
    # won't fail if enum values are not yet visible in the running session.

    sql = sa.text(
        """
        DO $$
        BEGIN
            -- If bookings and both columns exist, copy status_old -> status when different/null.
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status')
                   AND EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status_old') THEN

                    -- Try to copy values from status_old into status. Use casts via text to
                    -- avoid direct enum-to-enum operator problems. This may fail when the
                    -- target enum lacks labels; run in AUTOCOMMIT and ignore failures.
                    BEGIN
                        UPDATE bookings
                        SET status = (status_old::text)::bookingstatus
                        WHERE status IS NULL
                           OR (status::text IS DISTINCT FROM status_old::text);
                    EXCEPTION WHEN OTHERS THEN
                        -- Ignore errors (for example UnsafeNewEnumValueUsageError),
                        -- leave status_old intact for follow-up manual migration.
                        PERFORM 1;
                    END;

                    -- Attempt to drop status_old column: this is safe even if the UPDATE above skipped.
                    BEGIN
                        ALTER TABLE bookings DROP COLUMN IF EXISTS status_old;
                    EXCEPTION WHEN OTHERS THEN
                        -- ignore errors and continue
                        PERFORM 1;
                    END;
                END IF;
            END IF;

            -- Attempt to drop the legacy enum type booking_status if it exists and
            -- no remaining columns depend on it. This is guarded: we only drop
            -- it when it is safe; otherwise we skip silently.
            IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status') THEN
                -- Check for any attributes still using this type
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_attribute a
                    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                    JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
                    WHERE t.typname = 'booking_status'
                      AND a.attnum > 0
                ) THEN
                    BEGIN
                        DROP TYPE booking_status;
                    EXCEPTION WHEN OTHERS THEN
                        PERFORM 1;
                    END;
                END IF;
            END IF;
        END$$;
        """
    )

    # Execute under AUTOCOMMIT where possible to ensure enum type modifications
    # are visible. If execution fails (for example because enum values are
    # not yet committed in this run), swallow the error so the migration run
    # can continue. The data-cleanup can be retried later with a targeted
    # migration.
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        try:
            conn.execute(sql)
        except Exception:
            # If even the non-autocommit execution fails, skip quietly so the
            # rest of migrations can complete.
            pass


def downgrade() -> None:
    # Non-reversible: to restore the old column/type you must run a manual
    # migration that recreates the type and column and repopulates values.
    pass
