"""add username to users and masters

Revision ID: 20251206_add_users_masters_username
Revises: 20251206_add_bookings_reminder_columns
Create Date: 2025-12-06 12:20:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_add_users_masters_username'
down_revision = '20251206_add_bookings_reminder_columns'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add username columns if missing (nullable)
    op.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS username VARCHAR;")
    op.execute("ALTER TABLE masters ADD COLUMN IF NOT EXISTS username VARCHAR;")

    # Optionally create indexes if desired (skip for now to avoid locking):
    # op.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);")
    # op.execute("CREATE INDEX IF NOT EXISTS idx_masters_username ON masters(username);")


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS username;")
    op.execute("ALTER TABLE IF EXISTS masters DROP COLUMN IF EXISTS username;")
