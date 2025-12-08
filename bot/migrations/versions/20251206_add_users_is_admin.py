"""add is_admin flag to users

Revision ID: 20251206_add_users_is_admin
Revises: 20251206_add_users_masters_name_fields
Create Date: 2025-12-04 11:25:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_add_users_is_admin'
down_revision = '20251206_add_users_masters_name_fields'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent creation + safe backfill of users.is_admin
    op.execute('''
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='is_admin'
        ) THEN
            ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT false;
        END IF;

        -- Ensure no NULLs remain
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='is_admin'
        ) THEN
            UPDATE users SET is_admin = false WHERE is_admin IS NULL;
            ALTER TABLE users ALTER COLUMN is_admin SET DEFAULT false;
            ALTER TABLE users ALTER COLUMN is_admin SET NOT NULL;
        END IF;
    END
    $$;
    ''')


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS is_admin;")
