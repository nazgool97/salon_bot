"""Add exclusion constraints to prevent overlapping bookings per master and per user

Revision ID: 20251111_add_exclusion_constraints_bookings
Revises: 20251111_add_partial_unique_index_bookings_master_starts_at_active
Create Date: 2025-11-11 18:50:00.000000

This migration:
- adds a nullable `ends_at` timestamptz column to `bookings`
- populates `ends_at` based on booking_items -> service_profiles durations (fallback 60m)
- deduplicates overlapping bookings by cancelling the later booking (keep lowest id)
- enables `btree_gist` extension and creates two exclusion constraints that
  prevent overlapping tstzrange(starts_at, ends_at) for the same master_id and
  for the same user_id for active statuses.

Note: This migration chooses a conservative dedupe strategy: keep the record
with the smallest id and mark other overlapping bookings as CANCELLED. If you
prefer a different policy (prefer PAID/CONFIRMED), tell me and I can change it.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text


# revision identifiers, used by Alembic.
revision = "20251111_add_exclusion_constraints_bookings"
down_revision = "20251111_add_partial_unique_index_bookings_master_starts_at_active"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Add ends_at column
    op.add_column("bookings", sa.Column("ends_at", sa.DateTime(timezone=True), nullable=True))

    # 2) Populate ends_at from booking_items -> service_profiles (sum durations), fallback 60 minutes
    # First, for bookings having booking_items
    conn.execute(text("""
    WITH bi_totals AS (
      SELECT bi.booking_id, COALESCE(SUM(COALESCE(sp.duration_minutes, 60)), 60) AS total_minutes
      FROM booking_items bi
      LEFT JOIN service_profiles sp ON sp.service_id = bi.service_id
      GROUP BY bi.booking_id
    )
    UPDATE bookings b
    SET ends_at = b.starts_at + (bi_totals.total_minutes || ' minutes')::interval
    FROM bi_totals
    WHERE b.id = bi_totals.booking_id
    """))

    # Next, for bookings without booking_items, use the single service_id -> service_profiles
    conn.execute(text("""
    UPDATE bookings b
    SET ends_at = b.starts_at + (COALESCE(sp.duration_minutes, 60) || ' minutes')::interval
    FROM service_profiles sp
    WHERE b.service_id = sp.service_id AND b.ends_at IS NULL
    """))

    # Fallback for any remaining rows: default to 60 minutes
    conn.execute(text("""
    UPDATE bookings
    SET ends_at = starts_at + interval '60 minutes'
    WHERE ends_at IS NULL
    """))

    # 3) Deterministic dedupe: for master_id keep the lowest id and cancel overlapping later ones
    # Active statuses set
    active = ("RESERVED", "PENDING_PAYMENT", "CONFIRMED", "AWAITING_CASH", "PAID", "ACTIVE")
    conn.execute(text(f"""
    WITH candidate AS (
      SELECT id, master_id, starts_at, ends_at
      FROM bookings
      WHERE status IN {active}
    ), pairs AS (
      SELECT a.id AS keep_id, b.id AS remove_id
      FROM candidate a
      JOIN candidate b ON a.master_id = b.master_id AND a.id < b.id
      WHERE tstzrange(a.starts_at, a.ends_at) && tstzrange(b.starts_at, b.ends_at)
    )
    UPDATE bookings
    SET status = 'CANCELLED'
    WHERE id IN (SELECT DISTINCT remove_id FROM pairs)
    """))

    # 4) Dedupe for user_id similarly
    conn.execute(text(f"""
    WITH candidate AS (
      SELECT id, user_id, starts_at, ends_at
      FROM bookings
      WHERE status IN {active}
    ), pairs AS (
      SELECT a.id AS keep_id, b.id AS remove_id
      FROM candidate a
      JOIN candidate b ON a.user_id = b.user_id AND a.id < b.id
      WHERE tstzrange(a.starts_at, a.ends_at) && tstzrange(b.starts_at, b.ends_at)
    )
    UPDATE bookings
    SET status = 'CANCELLED'
    WHERE id IN (SELECT DISTINCT remove_id FROM pairs)
    """))

    # 5) Enable btree_gist extension (required for integer equality in gist indexes)
    conn.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gist"))

    # 6) Create exclusion constraints using gist on (master_id =, tstzrange overlaps)
    conn.execute(text("""
    ALTER TABLE bookings
    ADD CONSTRAINT bookings_no_overlap_master EXCLUDE USING GIST (
      master_id WITH =,
      tstzrange(starts_at, ends_at) WITH &&
    ) WHERE (status IN ('RESERVED','PENDING_PAYMENT','CONFIRMED','AWAITING_CASH','PAID','ACTIVE'))
    """))

    conn.execute(text("""
    ALTER TABLE bookings
    ADD CONSTRAINT bookings_no_overlap_user EXCLUDE USING GIST (
      user_id WITH =,
      tstzrange(starts_at, ends_at) WITH &&
    ) WHERE (status IN ('RESERVED','PENDING_PAYMENT','CONFIRMED','AWAITING_CASH','PAID','ACTIVE'))
    """))


def downgrade() -> None:
    conn = op.get_bind()
    # Drop exclusion constraints if exist
    try:
        conn.execute(text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_no_overlap_master"))
    except Exception:
        pass
    try:
        conn.execute(text("ALTER TABLE bookings DROP CONSTRAINT IF EXISTS bookings_no_overlap_user"))
    except Exception:
        pass

    # Drop ends_at column
    try:
        op.drop_column("bookings", "ends_at")
    except Exception:
        pass
