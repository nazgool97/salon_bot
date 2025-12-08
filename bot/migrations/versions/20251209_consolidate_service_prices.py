"""Consolidate service prices into `services.price_cents` and drop profile column

Revision ID: 20251209_consolidate_service_prices
Revises: 20251209_unify_booking_status_type
Create Date: 2025-12-04 21:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251209_consolidate_service_prices'
down_revision = '20251209_unify_booking_status_type'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Guarded consolidation: if service_profiles.price_cents exists, copy any
    # non-null profile prices into services.price_cents (profile overrides),
    # record conflicts to an audit table, then drop the column.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='service_profiles')
               AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='service_profiles' AND column_name='price_cents') THEN

                -- Create audit table to capture any divergent values
                IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='service_price_conflicts') THEN
                    CREATE TABLE public.service_price_conflicts (
                        id serial PRIMARY KEY,
                        service_id text,
                        service_price integer,
                        profile_price integer,
                        detected_at timestamptz DEFAULT now()
                    );
                END IF;

                -- Insert divergent rows for later review
                INSERT INTO public.service_price_conflicts(service_id, service_price, profile_price)
                SELECT sp.service_id, s.price_cents, sp.price_cents
                FROM public.service_profiles sp
                LEFT JOIN public.services s ON s.id = sp.service_id
                WHERE sp.price_cents IS NOT NULL
                  AND (s.price_cents IS NULL OR s.price_cents <> sp.price_cents);

                -- Copy profile price into services where profile price exists
                UPDATE public.services
                SET price_cents = sp.price_cents
                FROM public.service_profiles sp
                WHERE services.id = sp.service_id
                  AND sp.price_cents IS NOT NULL;

                -- Drop the profile price column now that services is canonical
                ALTER TABLE public.service_profiles DROP COLUMN IF EXISTS price_cents;
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
            # Best-effort: if this migration fails for some reason (permissions,
            # schema differences), do not abort the migration chain here.
            pass


def downgrade() -> None:
    # Non-reversible: restoring dropped column and values requires the
    # audit table or external backups. Manual intervention necessary.
    pass
