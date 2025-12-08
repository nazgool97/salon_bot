"""Delete non-EXPIRED bookings when an EXPIRED booking exists for same slot

Revision ID: 20251206_delete_nonexpired_if_expired_exists
Revises: 20251206_remove_duplicate_expired_rows
Create Date: 2025-12-04 17:58:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_delete_nonexpired_if_expired_exists'
down_revision = '20251206_remove_duplicate_expired_rows'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Delete any non-EXPIRED booking rows that share a slot with an EXPIRED row.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status') THEN

                    WITH expired_slots AS (
                        SELECT master_id, starts_at
                        FROM bookings
                        WHERE status::text = 'EXPIRED'
                        GROUP BY master_id, starts_at
                    ),
                    to_delete AS (
                        SELECT b.id
                        FROM bookings b
                        JOIN expired_slots e ON b.master_id = e.master_id AND b.starts_at = e.starts_at
                        WHERE b.status::text IS DISTINCT FROM 'EXPIRED'
                    )
                    DELETE FROM bookings WHERE id IN (SELECT id FROM to_delete);

                END IF;
            END IF;
        END$$;
        """
    )

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    except Exception:
        conn.execute(sql)


def downgrade() -> None:
    # no-op
    pass
