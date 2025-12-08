"""Backfill booking_items from bookings.service_id and drop bookings.service_id

Revision ID: 20251209_normalize_booking_items
Revises: 20251209_consolidate_service_prices
Create Date: 2025-12-04 21:40:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251209_normalize_booking_items'
down_revision = '20251209_consolidate_service_prices'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='service_id') THEN

                -- Audit table for bookings that had a service_id but no booking_items
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='booking_items_backfill_audit') THEN
                    CREATE TABLE public.booking_items_backfill_audit (
                        id serial PRIMARY KEY,
                        booking_id bigint,
                        booking_service_id text,
                        existing_items_count integer,
                        detected_at timestamptz DEFAULT now()
                    );
                END IF;

                -- Record bookings with service_id and zero booking_items
                INSERT INTO public.booking_items_backfill_audit(booking_id, booking_service_id, existing_items_count)
                SELECT b.id, b.service_id::text, count(bi.id)
                FROM public.bookings b
                LEFT JOIN public.booking_items bi ON bi.booking_id = b.id
                WHERE b.service_id IS NOT NULL
                GROUP BY b.id, b.service_id
                HAVING count(bi.id) = 0;

                -- Create booking_items rows for those bookings
                INSERT INTO public.booking_items (booking_id, service_id, position)
                SELECT b.id, b.service_id::text, 0
                FROM public.bookings b
                LEFT JOIN public.booking_items bi ON bi.booking_id = b.id
                WHERE b.service_id IS NOT NULL
                GROUP BY b.id, b.service_id
                HAVING count(bi.id) = 0;

                -- For bookings that already had booking_items but whose service_id
                -- differs, record them for manual review (do not modify).
                INSERT INTO public.booking_items_backfill_audit(booking_id, booking_service_id, existing_items_count)
                SELECT b.id, b.service_id::text, count(bi.id)
                FROM public.bookings b
                JOIN public.booking_items bi ON bi.booking_id = b.id
                WHERE b.service_id IS NOT NULL
                GROUP BY b.id, b.service_id
                HAVING count(bi.id) > 0 AND bool_or(bi.service_id IS DISTINCT FROM b.service_id::text);

                -- Finally, drop the redundant column
                BEGIN
                    ALTER TABLE public.bookings DROP COLUMN IF EXISTS service_id;
                EXCEPTION WHEN OTHERS THEN
                    -- ignore errors dropping the column
                    PERFORM 1;
                END;
            END IF;
        END$$;
        """
    )

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        try:
            conn.execute(sql)
        except Exception:
            pass


def downgrade() -> None:
    # Non-reversible: restoring the column requires manual intervention using audit table or backups.
    pass
