"""Create compact bookingstatus enum and minimal settings table (idempotent)

Revision ID: 20251203_create_bookingstatus_and_settings
Revises: 6be80c14c432
Create Date: 2025-12-03 16:05:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20251203_create_bookingstatus_and_settings"
down_revision = "6be80c14c432"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # canonical lowercase labels
    labels = [
        'pending', 'pending_payment', 'paid', 'awaiting_cash',
        'active', 'done', 'cancelled', 'no_show',
        'reserved', 'confirmed', 'expired',
    ]

    # Ensure the canonical underscored enum exists
    booking_status = postgresql.ENUM(*labels, name='booking_status')
    booking_status.create(conn, checkfirst=True)

    # Ensure the historical compact enum name exists so code using ::bookingstatus works
    bookingstatus = postgresql.ENUM(*labels, name='bookingstatus')
    bookingstatus.create(conn, checkfirst=True)

    # Create a minimal `settings` table if it does not yet exist. This table matches how
    # the app reads settings and is intentionally minimal and idempotent.
    exists = conn.execute(
        sa.text("SELECT 1 FROM information_schema.tables WHERE table_name = 'settings' LIMIT 1")
    ).fetchone()
    if not exists:
        op.create_table(
            'settings',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('key', sa.String(length=255), nullable=False, unique=True),
            sa.Column('value', sa.Text(), nullable=True),
            sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        )


def downgrade() -> None:
    # Keep downgrade empty to avoid accidental data loss in production.
    pass
