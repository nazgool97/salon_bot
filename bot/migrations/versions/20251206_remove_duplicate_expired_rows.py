"""Remove duplicate EXPIRED bookings for the same slot

Revision ID: 20251206_remove_duplicate_expired_rows
Revises: 20251206_resolve_expired_duplicates
Create Date: 2025-12-04 17:50:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_remove_duplicate_expired_rows'
down_revision = '20251206_resolve_expired_duplicates'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Delete duplicate EXPIRED rows per (master_id, starts_at), keep smallest id.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status') THEN

                    WITH expired_groups AS (
                        SELECT master_id, starts_at, array_agg(id ORDER BY id) AS ids, count(*) AS cnt
                        FROM bookings
                        WHERE status::text = 'EXPIRED'
                        GROUP BY master_id, starts_at
                        HAVING count(*) > 1
                    ),
                    to_delete AS (
                        SELECT unnest(ids[2:])::bigint AS id
                        FROM expired_groups
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
