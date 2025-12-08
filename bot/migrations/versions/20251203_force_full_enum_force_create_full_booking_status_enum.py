"""force create full booking_status enum with all values

Revision ID: 20251203_force_full_enum
Revises: 0001_initial_schema
Create Date: 2025-12-03
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '20251203_force_full_enum'
down_revision = '0001_initial_schema'

def upgrade():
    # Recreate enum with the full canonical set of lowercase labels.
    # Handle both possible type names (booking_status vs bookingstatus) by renaming the existing type
    # to a temporary name and creating a fresh one with the expected name `booking_status`.
    conn = op.get_bind()
    # If a compact typname exists, rename it to avoid conflicts; otherwise rename underscored name
    try:
        conn.execute("ALTER TYPE bookingstatus RENAME TO bookingstatus_old")
    except Exception:
        try:
            conn.execute("ALTER TYPE booking_status RENAME TO booking_status_old")
        except Exception:
            # If neither exists, continue and create new type
            pass

    booking_status = postgresql.ENUM(
        'pending', 'pending_payment', 'paid', 'awaiting_cash',
        'active', 'done', 'cancelled', 'no_show',
        'reserved', 'confirmed', 'expired',
        name='booking_status'
    )
    booking_status.create(op.get_bind(), checkfirst=True)

    # Also ensure the compact type name many parts of the codebase may query
    # (historical name `bookingstatus`) exists with the same labels so SQL
    # that references `::bookingstatus` will succeed.
    bookingstatus = postgresql.ENUM(
        'pending', 'pending_payment', 'paid', 'awaiting_cash',
        'active', 'done', 'cancelled', 'no_show',
        'reserved', 'confirmed', 'expired',
        name='bookingstatus'
    )
    bookingstatus.create(op.get_bind(), checkfirst=True)

    # If bookings.status column existed and had an old type, convert using text casting when possible
    try:
        # Prefer converting to the compact historical type `bookingstatus` if
        # that is what the application queries; fall back to `booking_status`.
        try:
            conn.execute("ALTER TABLE bookings ALTER COLUMN status TYPE bookingstatus USING status::text::bookingstatus")
        except Exception:
            conn.execute("ALTER TABLE bookings ALTER COLUMN status TYPE booking_status USING status::text::booking_status")
    except Exception:
        # Best-effort: ignore if the conversion fails here; earlier migrations aim to keep types compatible.
        pass

    # Drop any old temporary types if they exist
    try:
        conn.execute("DROP TYPE IF EXISTS bookingstatus_old")
    except Exception:
        pass
    try:
        conn.execute("DROP TYPE IF EXISTS booking_status_old")
    except Exception:
        pass

def downgrade():
    # Обратная миграция — если нужно, но можно оставить как есть
    pass