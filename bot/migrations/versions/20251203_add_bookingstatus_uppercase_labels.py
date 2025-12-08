"""Add uppercase bookingstatus labels (idempotent)

Revision ID: 20251203_add_bookingstatus_uppercase_labels
Revises: 20251203_add_enum_casts_booking_status
Create Date: 2025-12-03 16:30:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251203_add_bookingstatus_uppercase_labels'
down_revision = '20251203_add_enum_casts_booking_status'
branch_labels = None
depends_on = None


def _do_add_label_if_missing(conn, typname: str, label: str):
    # Use plpgsql DO block to check pg_enum and add label only if missing.
    sql = f"""
    DO $do$\n    BEGIN\n        IF NOT EXISTS (\n            SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid\n            WHERE t.typname = {sa.text(repr(typname)).text} AND e.enumlabel = {sa.text(repr(label)).text}\n        ) THEN\n            EXECUTE 'ALTER TYPE "' || {sa.text(repr(typname)).text} || '" ADD VALUE ' || quote_literal({sa.text(repr(label)).text});\n        END IF;\n    END\n    $do$ LANGUAGE plpgsql;
    """
    # Run the DO block in autocommit so the ALTER TYPE (which is non-transactional)
    # is committed immediately and subsequent migrations can safely use the new label.
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sa.text(sql))
    except Exception:
        # Fallback to plain execute if execution_options isn't supported in this context
        conn.execute(sa.text(sql))


def upgrade():
    conn = op.get_bind()
    # Labels to ensure (uppercase ones used by app)
    labels = [
        'CONFIRMED', 'PAID', 'RESERVED', 'AWAITING_CASH',
        'PENDING_PAYMENT', 'EXPIRED', 'NO_SHOW'
    ]
    # Ensure both type names (historic and current) contain these labels
    for typ in ('bookingstatus', 'booking_status'):
        for lbl in labels:
            # Wrap in try/except to avoid failing if type doesn't exist yet
            try:
                _do_add_label_if_missing(conn, typ, lbl)
            except Exception:
                # no-op: either type doesn't exist or another concurrent change
                pass


def downgrade():
    # We intentionally do not remove enum labels on downgrade to be safe.
    pass
