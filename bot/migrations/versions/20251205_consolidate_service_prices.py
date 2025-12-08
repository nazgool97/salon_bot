"""Consolidate prices: backfill `services.price_cents` from `service_profiles.base_price_cents`

Revision ID: 20251205_consolidate_service_prices
Revises: 20251205_drop_service_profiles_merge_audit
Create Date: 2025-12-05 05:10:00

This guarded migration backfills `services.price_cents` from
`service_profiles.base_price_cents` when the former is NULL. It then
checks for mismatches where both values are present but differ and writes
an audit table `service_price_consolidation_audit` for operator review and
aborts the migration so the operator can resolve price discrepancies.

The migration is conservative: it only updates NULL service prices and
will not overwrite an existing `services.price_cents` without manual
resolution.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251205_consolidate_service_prices"
down_revision = "20251205_drop_service_profiles_merge_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Backfill NULL services.price_cents from service_profiles.base_price_cents
    conn.execute(
        sa.text(
            """
            UPDATE public.services s
            SET price_cents = sp.base_price_cents
            FROM public.service_profiles sp
            WHERE s.price_cents IS NULL AND sp.base_price_cents IS NOT NULL AND sp.service_id::text = s.id::text;
            """
        )
    )

    # 2) Create audit table (single statement)
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS public.service_price_consolidation_audit (
                id serial PRIMARY KEY,
                service_id text,
                services_price integer,
                profile_price integer,
                issue text,
                detected_at timestamptz DEFAULT now()
            );
            """
        )
    )

    # 3) Insert mismatches where both values exist but differ
    conn.execute(
        sa.text(
            """
            INSERT INTO public.service_price_consolidation_audit(service_id, services_price, profile_price, issue)
            SELECT s.id::text, s.price_cents, sp.base_price_cents, 'mismatch'
            FROM public.services s
            JOIN public.service_profiles sp ON sp.service_id::text = s.id::text
            WHERE s.price_cents IS NOT NULL AND sp.base_price_cents IS NOT NULL AND s.price_cents IS DISTINCT FROM sp.base_price_cents;
            """
        )
    )

    # 4) If any mismatches were recorded, abort so operator can resolve.
    mismatch_count = conn.execute(sa.text("SELECT COUNT(*) FROM public.service_price_consolidation_audit WHERE issue = 'mismatch';")).scalar()
    if mismatch_count and int(mismatch_count) > 0:
        raise RuntimeError(
            f"Found {int(mismatch_count)} service price mismatches between services and service_profiles. Audit written to service_price_consolidation_audit. Resolve before dropping duplicate column."
        )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP TABLE IF EXISTS public.service_price_consolidation_audit;"))
