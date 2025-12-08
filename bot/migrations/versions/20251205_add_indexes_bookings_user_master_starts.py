"""Add composite indexes to speed up bookings pagination by user/master.

Creates:
 - ix_bookings_user_starts ON bookings (user_id, starts_at DESC)
 - ix_bookings_master_starts ON bookings (master_id, starts_at DESC)

Idempotent: uses CREATE INDEX IF NOT EXISTS so it is safe to run multiple times.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_add_indexes_bookings_user_master_starts"
down_revision = "20251205_add_masters_is_active"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Composite index for user bookings pagination
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_bookings_user_starts ON bookings (user_id, starts_at DESC);"
        )
    )

    # Composite index for master bookings pagination
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_bookings_master_starts ON bookings (master_id, starts_at DESC);"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_bookings_user_starts;"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_bookings_master_starts;"))
