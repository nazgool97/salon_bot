"""add first_name and last_name to users and masters

Revision ID: 20251206_add_users_masters_name_fields
Revises: 20251206_add_users_masters_username
Create Date: 2025-12-04 11:30:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_add_users_masters_name_fields'
down_revision = '20251206_add_users_masters_username'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add nullable first_name/last_name columns if missing and backfill simple values
    op.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='first_name'
        ) THEN
            ALTER TABLE users ADD COLUMN first_name VARCHAR(80);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='last_name'
        ) THEN
            ALTER TABLE users ADD COLUMN last_name VARCHAR(80);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='masters' AND column_name='first_name'
        ) THEN
            ALTER TABLE masters ADD COLUMN first_name VARCHAR(80);
        END IF;

        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='masters' AND column_name='last_name'
        ) THEN
            ALTER TABLE masters ADD COLUMN last_name VARCHAR(80);
        END IF;

        -- Backfill first_name from name where available (safe heuristic)
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='name'
        ) THEN
            UPDATE users SET first_name = split_part(name, ' ', 1) WHERE first_name IS NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='masters' AND column_name='name'
        ) THEN
            UPDATE masters SET first_name = split_part(name, ' ', 1) WHERE first_name IS NULL;
        END IF;
    END
    $$;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS first_name;")
    op.execute("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS last_name;")
    op.execute("ALTER TABLE IF EXISTS masters DROP COLUMN IF EXISTS first_name;")
    op.execute("ALTER TABLE IF EXISTS masters DROP COLUMN IF EXISTS last_name;")
