"""add master_telegram_id to master_profiles

Revision ID: 20251204_add_master_profiles_master_telegram_id
Revises: 20251204_add_master_profiles_id
Create Date: 2025-12-04 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251204_add_master_profiles_master_telegram_id'
down_revision = '20251204_add_master_profiles_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add column if it does not exist
    conn = op.get_bind()
    # add column nullable for safety
    op.add_column('master_profiles', sa.Column('master_telegram_id', sa.BigInteger(), nullable=True))

    # Populate values from existing master_id where present
    conn.execute(
        sa.text(
            """
            UPDATE master_profiles
            SET master_telegram_id = master_id
            WHERE master_telegram_id IS NULL AND master_id IS NOT NULL
            """
        )
    )

    # Create FK constraint to masters.telegram_id if not exists
    # Use a DO block to avoid "IF NOT EXISTS" limitation for ADD CONSTRAINT
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'master_profiles' AND c.conname = 'master_profiles_master_telegram_id_fkey'
                ) THEN
                    ALTER TABLE master_profiles
                    ADD CONSTRAINT master_profiles_master_telegram_id_fkey
                    FOREIGN KEY (master_telegram_id)
                    REFERENCES masters(telegram_id) ON DELETE CASCADE;
                END IF;
            END$$;
            """
        )
    )

    # If all rows have master_telegram_id, set NOT NULL for stricter schema
    # Check for nulls and alter only when safe
    has_nulls = conn.execute(sa.text("SELECT COUNT(*) FROM master_profiles WHERE master_telegram_id IS NULL")).scalar()
    if has_nulls == 0:
        conn.execute(sa.text("ALTER TABLE master_profiles ALTER COLUMN master_telegram_id SET NOT NULL"))


def downgrade() -> None:
    # Drop FK constraint if present, then drop column
    conn = op.get_bind()
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'master_profiles' AND c.conname = 'master_profiles_master_telegram_id_fkey'
                ) THEN
                    ALTER TABLE master_profiles DROP CONSTRAINT master_profiles_master_telegram_id_fkey;
                END IF;
            END$$;
            """
        )
    )
    op.drop_column('master_profiles', 'master_telegram_id')
