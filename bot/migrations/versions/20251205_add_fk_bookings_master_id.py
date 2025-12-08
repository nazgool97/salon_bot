"""Add foreign key constraint for bookings.master_id -> masters.id

Revision ID: 20251205_add_fk_bookings_master_id
Revises: 20251205_cleanup_indexes_add_bookings_status_idx
Create Date: 2025-12-05 04:40:00

This guarded migration will add a foreign key constraint on
`bookings.master_id` referencing `masters(id)` with `ON DELETE SET NULL`.
If any bookings reference a non-existent master, the migration writes an
audit table `bookings_master_fk_audit` with offending rows and aborts so
the operator can inspect and resolve them.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_add_fk_bookings_master_id"
down_revision = "20251205_cleanup_indexes_add_bookings_status_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Use a single DO $$ block to perform checks and create the FK atomically
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                -- If constraint already exists, nothing to do
                IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_bookings_master_id_masters_id') THEN
                    RETURN;
                END IF;

                -- If orphaned bookings.master_id exist, write audit and abort
                IF EXISTS (
                    SELECT 1 FROM public.bookings b LEFT JOIN public.masters m ON b.master_id = m.id
                    WHERE b.master_id IS NOT NULL AND m.id IS NULL
                ) THEN
                    -- Create audit table if missing
                    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings_master_fk_audit') THEN
                        EXECUTE 'CREATE TABLE public.bookings_master_fk_audit (id serial PRIMARY KEY, booking_id integer, master_id bigint, detected_at timestamptz DEFAULT now())';
                    END IF;

                    -- Insert offending rows for operator inspection
                    EXECUTE '
                        INSERT INTO public.bookings_master_fk_audit(booking_id, master_id)
                        SELECT b.id, b.master_id FROM public.bookings b LEFT JOIN public.masters m ON b.master_id = m.id
                        WHERE b.master_id IS NOT NULL AND m.id IS NULL
                    ';

                    RAISE EXCEPTION 'Found bookings rows with missing masters; audit written to bookings_master_fk_audit. Resolve or re-link before re-running this migration.';
                END IF;

                -- Add the FK constraint with ON DELETE SET NULL (preserves bookings if a master is removed)
                EXECUTE 'ALTER TABLE public.bookings ADD CONSTRAINT fk_bookings_master_id_masters_id FOREIGN KEY (master_id) REFERENCES public.masters(id) ON DELETE SET NULL';
            END$$;
            """
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("ALTER TABLE public.bookings DROP CONSTRAINT IF EXISTS fk_bookings_master_id_masters_id;"))
