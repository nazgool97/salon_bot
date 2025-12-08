"""Add remaining bookingstatus labels (idempotent)

Revision ID: 20251203_add_bookingstatus_remaining_labels
Revises: 20251203_add_bookingstatus_uppercase_labels
Create Date: 2025-12-03 16:45:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251203_add_bookingstatus_remaining_labels'
down_revision = '20251203_add_bookingstatus_uppercase_labels'
branch_labels = None
depends_on = None


def _do_add_label_if_missing(conn, typname: str, label: str):
    sql = f"""
    DO $do$\n    BEGIN\n        IF NOT EXISTS (\n            SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid\n            WHERE t.typname = {sa.text(repr(typname)).text} AND e.enumlabel = {sa.text(repr(label)).text}\n        ) THEN\n            EXECUTE 'ALTER TYPE "' || {sa.text(repr(typname)).text} || '" ADD VALUE ' || quote_literal({sa.text(repr(label)).text});\n        END IF;\n    END\n    $do$ LANGUAGE plpgsql;
    """
    # Run the DO block in autocommit so the ALTER TYPE is committed immediately
    # and subsequent migrations can use the new enum label safely.
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sa.text(sql))
    except Exception:
        conn.execute(sa.text(sql))


def upgrade():
    conn = op.get_bind()
    labels = [
        'RESERVED', 'PENDING_PAYMENT', 'CONFIRMED', 'AWAITING_CASH',
        'PAID', 'ACTIVE', 'CANCELLED', 'DONE', 'NO_SHOW', 'EXPIRED'
    ]
    for typ in ('bookingstatus', 'booking_status'):
        for lbl in labels:
            try:
                _do_add_label_if_missing(conn, typ, lbl)
            except Exception:
                pass


def downgrade():
    # Safe no-op
    pass
