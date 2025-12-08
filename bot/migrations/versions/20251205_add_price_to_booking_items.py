"""Add price_cents to booking_items and backfill from services

Revision ID: 20251205_add_price_to_booking_items
Revises: 20251205_drop_legacy_booking_enums
Create Date: 2025-12-05 03:10:00

Conservative, idempotent migration that adds a `price_cents` integer
column to `booking_items`, backfills it from `services.price_cents` when
possible, and writes audit rows for any remaining NULLs so an operator can
review them before enforcing NOT NULL in a follow-up migration.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_add_price_to_booking_items"
down_revision = "20251205_drop_legacy_booking_enums"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Add the nullable column if missing.
    conn.execute(sa.text("ALTER TABLE public.booking_items ADD COLUMN IF NOT EXISTS price_cents INTEGER;"))

    # Backfill from services.price_cents for rows that don't have a snapshot.
    # Use an UPDATE .. FROM join which is atomic within a transaction.
    conn.execute(
        sa.text(
            """
            UPDATE public.booking_items bi
            SET price_cents = COALESCE(s.price_cents, 0)
            FROM public.services s
            WHERE bi.price_cents IS NULL AND bi.service_id = s.id::text;
            """
        )
    )

    # Create an audit table if any booking_items remain without price_cents
    # Create audit table (single statement)
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS public.booking_items_price_backfill_audit (
                id serial PRIMARY KEY,
                booking_item_id integer,
                booking_id integer,
                service_id text,
                detected_at timestamptz DEFAULT now()
            );
            """
        )
    )

    # Insert audit rows separately (avoid multiple statements in one prepared stmt)
    conn.execute(
        sa.text(
            """
            INSERT INTO public.booking_items_price_backfill_audit (booking_item_id, booking_id, service_id)
            SELECT bi.id, bi.booking_id, bi.service_id
            FROM public.booking_items bi
            WHERE bi.price_cents IS NULL;
            """
        )
    )

    # Note: We intentionally leave the column nullable. Operator can inspect
    # `booking_items_price_backfill_audit` and, when confident, add NOT NULL in
    # a separate, controlled migration.


def downgrade() -> None:
    # Non-destructive downgrade: drop the column if it exists.
    conn = op.get_bind()
    conn.execute(sa.text("ALTER TABLE public.booking_items DROP COLUMN IF EXISTS price_cents;"))
