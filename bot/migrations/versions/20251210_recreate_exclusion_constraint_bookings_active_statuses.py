"""Recreate exclusion constraint to only treat explicit active statuses as occupied

Revision ID: 20251210_recreate_exclusion_constraint_bookings_active_statuses
Revises: 20251209_recreate_partial_unique_index_bookings_active
Create Date: 2025-12-10 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251210_recreate_exclusion_constraint_bookings_active_statuses"
down_revision = "20251209_recreate_partial_unique_index_bookings_active"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Ensure GiST support is available
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist;"))

    # The explicit set of booking statuses that should be treated as occupying a time slot.
    desired_statuses = ('reserved', 'pending_payment', 'confirmed', 'paid')

    # Detect a booking-status enum type if present so we can use typed literals.
    def _find_status_enum() -> str | None:
        candidates = ('booking_status', 'bookingstatus', 'booking_status_enum')
        for typ in candidates:
            exists = conn.execute(
                sa.text("SELECT 1 FROM pg_type WHERE typname = :typ"), {"typ": typ}
            ).first()
            if exists:
                return typ
        return None

    status_enum = _find_status_enum()

    if status_enum:
        statuses_sql = "(" + ", ".join(f"'{s}'::{status_enum}" for s in desired_statuses) + ")"
    else:
        statuses_sql = "(" + ", ".join(f"'{s}'" for s in desired_statuses) + ")"

    # Discover which column is the master reference on bookings to be resilient across schema states.
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
        print("Skipping exclusion-constraint recreation: no master column found on bookings")
        return

    # 1) Check for existing overlaps among the desired statuses before installing the constraint.
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
            "Found overlapping bookings for the active-status set; audit written to bookings_overlap_audit. "
            "Resolve overlaps (or backfill ends_at) before re-running this migration."
        )

    # 2) Replace the existing overlap constraint with a new one that only applies to the desired statuses.
    # Try dropping any previous constraint names we know about.
    try:
        conn.execute(sa.text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_prevent_overlaps_gist;"))
    except Exception:
        pass
    try:
        conn.execute(sa.text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlap_bookings_master_excl;"))
    except Exception:
        pass

    add_excl = (
        "ALTER TABLE bookings ADD CONSTRAINT bookings_prevent_overlaps_gist "
        "EXCLUDE USING gist (" + f"{master_col} WITH =, tstzrange(starts_at, ends_at) WITH &&) "
        "WHERE (status IN " + statuses_sql + " AND ends_at IS NOT NULL);"
    )
    conn.execute(sa.text(add_excl))


def downgrade() -> None:
    conn = op.get_bind()

    # Best-effort downgrade: replace our constraint with a conservative fallback that
    # excludes cancelled and expired (older migration behaviour) if possible.
    try:
        conn.execute(sa.text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_prevent_overlaps_gist;"))
    except Exception:
        pass

    # Try to recreate the common older predicate that excluded cancelled/expired.
    try:
        # Prefer using the booking_status enum if present, else fall back to string literals.
        enum_exists = bool(
            conn.execute(sa.text("SELECT 1 FROM pg_type WHERE typname = 'booking_status' LIMIT 1")).first()
        )
        if enum_exists:
            pred = "(status NOT IN ('cancelled'::booking_status, 'expired'::booking_status) AND ends_at IS NOT NULL)"
        else:
            pred = "(status NOT IN ('cancelled', 'expired') AND ends_at IS NOT NULL)"

        conn.execute(sa.text(
            "ALTER TABLE bookings ADD CONSTRAINT bookings_prevent_overlaps_gist "
            "EXCLUDE USING gist (master_id WITH =, tstzrange(starts_at, ends_at) WITH &&) "
            f"WHERE {pred};"
        ))
    except Exception:
        # Best-effort downgrade; ignore errors to allow rollbacks to proceed.
        pass
