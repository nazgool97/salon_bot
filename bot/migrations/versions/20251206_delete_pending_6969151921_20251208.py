"""One-off: delete pending booking for slot that already has EXPIRED

Revision ID: 20251206_delete_pending_6969151921_20251208
Revises: 20251206_delete_nonexpired_if_expired_exists
Create Date: 2025-12-04 18:05:00.000000

This is a targeted, idempotent migration created to remove a single
PENDING_PAYMENT booking that conflicts with an existing EXPIRED booking
for master_id=6969151921 and starts_at='2025-12-08 08:05:00+00'.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_delete_pending_6969151921_20251208'
down_revision = '20251206_delete_nonexpired_if_expired_exists'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Guarded, idempotent delete for the exact conflicting slot.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                -- Delete any non-EXPIRED booking for the specific slot
                DELETE FROM bookings
                WHERE master_id = 6969151921
                  AND starts_at = TIMESTAMPTZ '2025-12-08 08:05:00+00'
                  AND status::text IS DISTINCT FROM 'EXPIRED';
            END IF;
        END$$;
        """
    )

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        conn.execute(sql)


def downgrade() -> None:
    # no-op: data deletion is not reversible automatically
    pass
