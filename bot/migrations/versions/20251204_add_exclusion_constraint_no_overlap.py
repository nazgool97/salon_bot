"""Add exclusion constraint to prevent overlapping active bookings per master

Revision ID: 20251204_add_exclusion_constraint_no_overlap
Revises: 20251204_unify_booking_status_enum
Create Date: 2025-12-04 13:30:00

This guarded migration installs the `btree_gist` extension (if missing),
audits any existing overlapping `active` bookings (writing them to
`bookings_overlap_audit`), and only adds the exclusion constraint if no
conflicts are found. The exclusion constraint uses `tstzrange(starts_at, ends_at)`
and a GIST index to prevent overlapping time ranges per `master_id`.

The migration is intentionally conservative: it requires `ends_at` to be
non-null for the constraint to apply. Any rows with NULL `ends_at` are
excluded from the constraint and should be backfilled before tightening
the rule if desired.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_add_exclusion_constraint_no_overlap"
down_revision = "20251204_unify_booking_status_enum"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Ensure btree_gist extension exists (provides GiST index support for b-tree types)
    conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist;"))

    # 2) Create an audit table to store any overlapping pairs we find
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS bookings_overlap_audit (
                booking_a bigint,
                booking_b bigint,
                master_id bigint,
                a_starts_at timestamptz,
                a_ends_at timestamptz,
                b_starts_at timestamptz,
                b_ends_at timestamptz,
                inserted_at timestamptz DEFAULT now()
            );
            """
        )
    )

    # 3) Detect any overlapping active bookings (both ends_at must be NOT NULL)
    conflict = conn.execute(
        sa.text(
            """
            SELECT 1
            FROM bookings b1
            JOIN bookings b2 ON b1.master_id = b2.master_id AND b1.id < b2.id
            WHERE b1.status = 'active' AND b2.status = 'active'
              AND b1.ends_at IS NOT NULL AND b2.ends_at IS NOT NULL
              AND tstzrange(b1.starts_at, b1.ends_at) && tstzrange(b2.starts_at, b2.ends_at)
            LIMIT 1;
            """
        )
    ).first()

    if conflict:
        # Populate full audit details and abort so operator can resolve overlaps first.
        conn.execute(
            sa.text(
                """
                INSERT INTO bookings_overlap_audit(booking_a, booking_b, master_id, a_starts_at, a_ends_at, b_starts_at, b_ends_at)
                SELECT b1.id, b2.id, b1.master_id, b1.starts_at, b1.ends_at, b2.starts_at, b2.ends_at
                FROM bookings b1
                JOIN bookings b2 ON b1.master_id = b2.master_id AND b1.id < b2.id
                WHERE b1.status = 'active' AND b2.status = 'active'
                  AND b1.ends_at IS NOT NULL AND b2.ends_at IS NOT NULL
                  AND tstzrange(b1.starts_at, b1.ends_at) && tstzrange(b2.starts_at, b2.ends_at);
                """
            )
        )
        raise RuntimeError(
            "Found overlapping active bookings; audit written to bookings_overlap_audit. "
            "Resolve overlaps (or backfill ends_at) before re-running this migration."
        )

    # 4) Add exclusion constraint if it doesn't already exist
    exists = conn.execute(
        sa.text("SELECT 1 FROM pg_constraint WHERE conname = 'no_overlap_bookings_master_excl'")
    ).first()
    if not exists:
        conn.execute(
            sa.text(
                "ALTER TABLE bookings ADD CONSTRAINT no_overlap_bookings_master_excl "
                "EXCLUDE USING gist (master_id WITH =, tstzrange(starts_at, ends_at) WITH &&) "
                "WHERE (status = 'active' AND ends_at IS NOT NULL);"
            )
        )


def downgrade() -> None:
    conn = op.get_bind()
    # Drop the constraint if it exists
    try:
        conn.execute(
            sa.text(
                "ALTER TABLE bookings DROP CONSTRAINT IF EXISTS no_overlap_bookings_master_excl;"
            )
        )
    except Exception:
        pass
