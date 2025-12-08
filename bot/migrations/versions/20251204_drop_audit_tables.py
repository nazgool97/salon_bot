"""Drop temporary audit tables: booking_items_backfill_audit, service_price_conflicts

Revision ID: 20251204_drop_audit_tables
Revises: 20251204_merge_service_profiles_into_services
Create Date: 2025-12-04 15:10:00

This migration drops two temporary/audit tables that were used during
data-fixing scripts. These tables are non-essential once the fixes have
been applied and are safe to remove provided you have a backup or have
exported the data.

The downgrade recreates minimal table structures so the migration is
reversible in a conservative manner.
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251204_drop_audit_tables"
down_revision = "20251204_merge_service_profiles_into_services"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Drop audit tables if present
    conn.execute(sa.text("DROP TABLE IF EXISTS booking_items_backfill_audit;"))
    conn.execute(sa.text("DROP TABLE IF EXISTS service_price_conflicts;"))


def downgrade() -> None:
    conn = op.get_bind()
    # Recreate minimal audit table schemas (no guarantees about original indexes)
    conn.execute(sa.text(
        "CREATE TABLE IF NOT EXISTS booking_items_backfill_audit (booking_id bigint, booking_service_id text, existing_items_count int, inserted_at timestamptz DEFAULT now());"
    ))
    conn.execute(sa.text(
        "CREATE TABLE IF NOT EXISTS service_price_conflicts (service_id text, service_price bigint, profile_price bigint, inserted_at timestamptz DEFAULT now());"
    ))
