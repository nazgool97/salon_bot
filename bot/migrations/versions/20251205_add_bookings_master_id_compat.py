"""Add nullable bookings.master_id column for runtime compatibility

Revision ID: 20251205_add_bookings_master_id_compat
Revises: 98392e138c34
Create Date: 2025-12-05 01:30:00

This migration is a conservative, idempotent compatibility shim that ensures
the `bookings` table has a `master_id` column so the running application
does not raise "column bookings.master_id does not exist" errors while the
data migrations are finalized. The column is nullable and no FK is added
here to keep this operation cheap and safe for live systems.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_add_bookings_master_id_compat"
down_revision = "98392e138c34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Add nullable master_id if missing (safe, idempotent)
    conn.execute(sa.text("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS master_id BIGINT;"))

    # Create non-unique index to avoid degrading queries that expect it
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_bookings_master_id ON bookings (master_id);"))


def downgrade() -> None:
    conn = op.get_bind()

    # Best-effort rollback: drop index and drop column if present.
    try:
        conn.execute(sa.text("DROP INDEX IF EXISTS ix_bookings_master_id;"))
    except Exception:
        pass
    try:
        conn.execute(sa.text("ALTER TABLE bookings DROP COLUMN IF EXISTS master_id;"))
    except Exception:
        pass
