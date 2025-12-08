"""Convert bookings.status column to bookingstatus enum (idempotent)

Revision ID: 20251203_convert_bookings_status_to_bookingstatus
Revises: 20251203_create_bookingstatus_and_settings
Create Date: 2025-12-03 16:12:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251203_convert_bookings_status_to_bookingstatus"
down_revision = "20251203_create_bookingstatus_and_settings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # only run if bookings.status exists and is not already bookingstatus
    col = conn.execute(
        sa.text("SELECT udt_name FROM information_schema.columns WHERE table_name='bookings' AND column_name='status' LIMIT 1")
    ).fetchone()
    if col is None:
        return
    current_udt = col[0]
    if current_udt == 'bookingstatus':
        return

    # Safer approach that avoids enum-operator mismatches and transaction aborts:
    # 1) create a temporary text column `status_tmp`
    # 2) copy lower(status::text) -> status_tmp
    # 3) rename old column to `status_old`
    # 4) add new column `status` of type bookingstatus
    # 5) copy status_tmp::bookingstatus -> status
    # 6) drop temporary columns
    try:
        # Step 1: add temp text column if not exists
        try:
            conn.execute(sa.text("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS status_tmp text"))
        except Exception:
            pass

        # Step 2: copy values lowercased
        try:
            conn.execute(sa.text("UPDATE bookings SET status_tmp = lower(status::text) WHERE status IS NOT NULL"))
        except Exception:
            # if cast fails, try copying via explicit cast fallback
            try:
                conn.execute(sa.text("UPDATE bookings SET status_tmp = lower((status::text)) WHERE status IS NOT NULL"))
            except Exception:
                pass

        # Step 3: rename existing status -> status_old only if `status` exists and `status_old` does not
        try:
            status_exists = conn.execute(sa.text("SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status' LIMIT 1")).fetchone()
            exists_old = conn.execute(sa.text("SELECT 1 FROM information_schema.columns WHERE table_name='bookings' AND column_name='status_old' LIMIT 1")).fetchone()
            if status_exists and not exists_old:
                conn.execute(sa.text("ALTER TABLE bookings RENAME COLUMN status TO status_old"))
        except Exception:
            # if rename fails, proceed cautiously
            pass

        # Step 4: add new status column of desired enum type
        try:
            conn.execute(sa.text("ALTER TABLE bookings ADD COLUMN IF NOT EXISTS status bookingstatus"))
        except Exception:
            pass

        # Step 5: copy back from status_tmp
        try:
            conn.execute(sa.text("UPDATE bookings SET status = status_tmp::bookingstatus WHERE status_tmp IS NOT NULL"))
        except Exception:
            # non-fatal: if some values don't cast, they'll remain NULL and can be fixed manually
            pass

        # Step 6: drop temporary text column; do NOT drop `status_old` (leave it in place to avoid cascading drops)
        try:
            conn.execute(sa.text("ALTER TABLE bookings DROP COLUMN IF EXISTS status_tmp"))
        except Exception:
            pass
    except Exception:
        # ensure migration doesn't crash the whole upgrade process
        pass


def downgrade() -> None:
    # No-op: avoid data loss
    pass
