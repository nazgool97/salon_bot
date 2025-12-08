"""set default false for booking boolean flags

Revision ID: 20251204_set_bookings_boolean_defaults
Revises: 20251204_backfill_status_old_and_default
Create Date: 2025-12-04 01:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251204_set_bookings_boolean_defaults'
down_revision = '20251204_backfill_status_old_and_default'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # set server defaults so inserts that omit these booleans don't fail
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_24h_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "remind_24h_sent" SET DEFAULT false';
            END IF;
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_1h_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "remind_1h_sent" SET DEFAULT false';
            END IF;
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'feedback_prompt_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "feedback_prompt_sent" SET DEFAULT false';
            END IF;
        END
        $$;
        """
    ))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_24h_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "remind_24h_sent" DROP DEFAULT';
            END IF;
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'remind_1h_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "remind_1h_sent" DROP DEFAULT';
            END IF;
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bookings' AND column_name = 'feedback_prompt_sent') THEN
                EXECUTE 'ALTER TABLE public.bookings ALTER COLUMN "feedback_prompt_sent" DROP DEFAULT';
            END IF;
        END
        $$;
        """
    ))
