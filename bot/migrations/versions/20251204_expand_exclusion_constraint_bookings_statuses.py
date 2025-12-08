"""Expand exclusion constraint to treat multiple booked statuses as occupied

Revision ID: 20251204_expand_exclusion_constraint_bookings_statuses
Revises: 20251204_add_exclusion_constraint_no_overlap
Create Date: 2025-12-04 14:10:00

This guarded migration replaces the existing `no_overlap_bookings_master_excl`
constraint so it treats multiple booking statuses as occupying a time slot.
It audits any existing overlaps for the broader set of statuses and aborts
so the operator can resolve them before the constraint is installed.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_expand_exclusion_constraint_bookings_statuses"
down_revision = "20251204_add_exclusion_constraint_no_overlap"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Ensure GiST support is available
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist;"))

    # Define the set of statuses that count as occupying a time slot.
    # Extend this tuple if you consider more statuses to block a time range.
    statuses_sql = "('active','paid','confirmed')"

    # 1) Determine which column holds the master reference on bookings.
    # Fall back through common candidates so this migration is resilient
    # against intermediate schema states (master_id, master_id_new, master_id_old).
    def _col_exists(col: str) -> bool:
        return bool(
            conn.execute(
                sa.text(
                    "SELECT 1 FROM information_schema.columns WHERE table_name = 'bookings' AND column_name = :col"
                ),
                {"col": col},
            ).first()
        )

    master_col = None
    for candidate in ("master_id", "master_id_new", "master_id_old", "master_telegram_id"):
        if _col_exists(candidate):
            master_col = candidate
            break

    if master_col is None:
        # Nothing to do if bookings has no master reference column yet.
        print("Skipping exclusion-constraint expansion: no master column found on bookings")
        return

    # Build SQL that uses the discovered column name.
    conflict_sql = f'''
        SELECT 1
        FROM bookings b1
        JOIN bookings b2 ON b1.{master_col} = b2.{master_col} AND b1.id < b2.id
        WHERE b1.status IN {statuses_sql} AND b2.status IN {statuses_sql}
          AND b1.ends_at IS NOT NULL AND b2.ends_at IS NOT NULL
          AND tstzrange(b1.starts_at, b1.ends_at) && tstzrange(b2.starts_at, b2.ends_at)
        LIMIT 1;
    '''

    conflict = conn.execute(sa.text(conflict_sql)).first()

    if conflict:
        # Populate full audit details and abort so operator can resolve overlaps first.
        insert_sql = f'''
            INSERT INTO bookings_overlap_audit(booking_a, booking_b, master_id, a_starts_at, a_ends_at, b_starts_at, b_ends_at)
            SELECT b1.id, b2.id, b1.{master_col}, b1.starts_at, b1.ends_at, b2.starts_at, b2.ends_at
            FROM bookings b1
            JOIN bookings b2 ON b1.{master_col} = b2.{master_col} AND b1.id < b2.id
            WHERE b1.status IN {statuses_sql} AND b2.status IN {statuses_sql}
              AND b1.ends_at IS NOT NULL AND b2.ends_at IS NOT NULL
              AND tstzrange(b1.starts_at, b1.ends_at) && tstzrange(b2.starts_at, b2.ends_at);
        '''

        conn.execute(sa.text(insert_sql))
        raise RuntimeError(
            "Found overlapping bookings for the expanded occupied-status set; audit written to bookings_overlap_audit. "
            "Resolve overlaps (or backfill ends_at) before re-running this migration."
        )

    # 2) Replace the constraint: drop if exists then add the new constrained definition
    # Use DROP CONSTRAINT IF EXISTS and then ADD CONSTRAINT. If the constraint was
    # absent, this will just add it. If present, it's replaced atomically here.
    conn.execute(sa.text(f"ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlap_bookings_master_excl;"))

    # Add the updated exclusion constraint applying to the chosen statuses on the discovered master column.
    add_excl = (
        "ALTER TABLE bookings ADD CONSTRAINT no_overlap_bookings_master_excl "
        "EXCLUDE USING gist (" + f"{master_col} WITH =, tstzrange(starts_at, ends_at) WITH &&) "
        "WHERE (status IN ('active','paid','confirmed') AND ends_at IS NOT NULL);"
    )
    conn.execute(sa.text(add_excl))


def downgrade() -> None:
    conn = op.get_bind()

    # Revert to the earlier, narrower constraint that only treated 'active' as occupied.
    try:
        conn.execute(
            sa.text(
                "ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlap_bookings_master_excl;"
            )
        )
        conn.execute(
            sa.text(
                "ALTER TABLE bookings ADD CONSTRAINT no_overlap_bookings_master_excl "
                "EXCLUDE USING gist (master_id WITH =, tstzrange(starts_at, ends_at) WITH &&) "
                "WHERE (status = 'active' AND ends_at IS NOT NULL);"
            )
        )
    except Exception:
        # Downgrade should be best-effort; ignore errors to avoid blocking rollbacks.
        pass
