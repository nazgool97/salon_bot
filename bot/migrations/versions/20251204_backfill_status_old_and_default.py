"""backfill status_old and set default

Revision ID: 20251204_backfill_status_old_and_default
Revises: 20251204_service_profiles_id_and_master_id_nullable
Create Date: 2025-12-04 01:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251204_backfill_status_old_and_default'
down_revision = '20251204_service_profiles_id_and_master_id_nullable'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Backfill status_old from status when possible, else set to RESERVED.
    # Execute in autocommit so any ALTER TYPE/ADD VALUE operations that created
    # the 'RESERVED' label are committed and visible to this UPDATE.
    sql_do = sa.text(
        """
        DO $$
        BEGIN
            UPDATE bookings
            SET status_old = (
                CASE
                    WHEN status IS NOT NULL THEN status::text::booking_status
                    ELSE 'reserved'::booking_status
                END
            )
            WHERE status_old IS NULL;
        END$$;
        """
    )
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql_do)
    except Exception:
        conn.execute(sql_do)

    # Ensure server default exists so inserts that omit status_old succeed.
    sql_default = sa.text("ALTER TABLE bookings ALTER COLUMN status_old SET DEFAULT 'reserved'::booking_status")
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql_default)
    except Exception:
        conn.execute(sql_default)


def downgrade() -> None:
    conn = op.get_bind()
    # Remove default on downgrade
    conn.execute(sa.text("ALTER TABLE bookings ALTER COLUMN status_old DROP DEFAULT"))
