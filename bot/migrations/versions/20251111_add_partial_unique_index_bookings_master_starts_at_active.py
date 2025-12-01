"""Add partial unique index on bookings(master_id, starts_at) for active statuses

Revision ID: 20251111_add_partial_unique_index_bookings_master_starts_at_active
Revises: 20251103_add_cascade_services_bookings
Create Date: 2025-11-11 16:50:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251111_add_partial_unique_index_bookings_master_starts_at_active'
down_revision = '20251103_add_cascade_services_bookings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create a partial unique index preventing duplicate active bookings per master/start.

    The index enforces uniqueness for a reasonable set of "active" statuses so
    that finished/cancelled bookings don't block the same slot being reused.
    """
    # Before creating the unique index, deduplicate any existing conflicting rows.
    # Strategy: for each (master_id, starts_at) pair with more than one row in an
    # active status, keep the row with the smallest id and mark the others as
    # CANCELLED. This avoids migration failure due to existing duplicates.
    op.execute(
        """
        WITH active AS (
            SELECT id, master_id, starts_at
            FROM bookings
            WHERE status IN ('RESERVED','PENDING_PAYMENT','CONFIRMED','AWAITING_CASH','PAID','ACTIVE')
        ),
        dup_groups AS (
            SELECT master_id, starts_at, array_agg(id ORDER BY id) AS ids, count(*) AS cnt
            FROM active
            GROUP BY master_id, starts_at
            HAVING count(*) > 1
        ),
        to_cancel AS (
            SELECT (ids[2:array_length(ids,1)])::int[] AS cancel_ids
            FROM dup_groups
        ),
        flat AS (
            SELECT unnest(cancel_ids) AS id FROM to_cancel
        )
        UPDATE bookings
        SET status = 'CANCELLED'
        WHERE id IN (SELECT id FROM flat)
        """
    )

    # Postgres-only partial unique index
    op.create_index(
        'ux_bookings_master_start_active',
        'bookings',
        ['master_id', 'starts_at'],
        unique=True,
        postgresql_where=sa.text("status IN ('RESERVED','PENDING_PAYMENT','CONFIRMED','AWAITING_CASH','PAID','ACTIVE')"),
    )


def downgrade() -> None:
    op.drop_index('ux_bookings_master_start_active', table_name='bookings')
