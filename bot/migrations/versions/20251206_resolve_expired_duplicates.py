"""Resolve duplicates where an EXPIRED booking already exists for the same slot

Revision ID: 20251206_resolve_expired_duplicates
Revises: 20251206_replace_ux_with_status_index
Create Date: 2025-12-04 17:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251206_resolve_expired_duplicates'
down_revision = '20251206_replace_ux_with_status_index'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # For any (master_id, starts_at) that already has an EXPIRED booking,
    # keep one canonical row (prefer an EXPIRED row) and delete other duplicates.
    # Deleting avoids creating status collisions against the unique index.
    sql = sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='bookings') THEN
                IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status') THEN

                    -- For groups that have at least one EXPIRED row, determine a keeper id
                    WITH keepers AS (
                        SELECT master_id, starts_at,
                               min(id) FILTER (WHERE status::text = 'EXPIRED') AS keeper_expired,
                               min(id) AS keeper_any
                        FROM bookings
                        GROUP BY master_id, starts_at
                    ),
                    groups_to_fix AS (
                        SELECT master_id, starts_at, COALESCE(keeper_expired, keeper_any) AS keeper_id
                        FROM keepers
                        WHERE keeper_expired IS NOT NULL
                    ),
                    to_remove AS (
                        SELECT b.id
                        FROM bookings b
                        JOIN groups_to_fix g ON b.master_id = g.master_id AND b.starts_at = g.starts_at
                        WHERE b.id <> g.keeper_id
                    )
                    DELETE FROM bookings WHERE id IN (SELECT id FROM to_remove);

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
