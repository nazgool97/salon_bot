"""add id to service_profiles and make master_id nullable

Revision ID: 20251204_service_profiles_id_and_master_id_nullable
Revises: 20251204_add_master_profiles_master_telegram_id
Create Date: 2025-12-04 00:30:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251204_service_profiles_id_and_master_id_nullable'
down_revision = '20251204_add_master_profiles_master_telegram_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) Add id to service_profiles and make it the PK
    # Create sequence if missing
    conn.execute(sa.text("CREATE SEQUENCE IF NOT EXISTS service_profiles_id_seq START 1"))

    # Add nullable id column
    op.add_column('service_profiles', sa.Column('id', sa.BigInteger(), nullable=True))

    # Populate id for existing rows using nextval
    conn.execute(sa.text("UPDATE service_profiles SET id = nextval('service_profiles_id_seq') WHERE id IS NULL"))

    # Ensure sequence is advanced to max(id)
    conn.execute(sa.text("SELECT setval('service_profiles_id_seq', COALESCE((SELECT MAX(id) FROM service_profiles), 1), true)"))

    # Make id NOT NULL and set default
    conn.execute(sa.text("ALTER TABLE service_profiles ALTER COLUMN id SET DEFAULT nextval('service_profiles_id_seq')"))
    conn.execute(sa.text("ALTER TABLE service_profiles ALTER COLUMN id SET NOT NULL"))

    # Drop old PK on service_id and create new PK on id
    # Use DO block to check existence to be safe
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'service_profiles' AND c.contype = 'p') THEN
                ALTER TABLE service_profiles DROP CONSTRAINT service_profiles_pkey;
            END IF;
        END$$;
        """
    ))

    conn.execute(sa.text("ALTER TABLE service_profiles ADD PRIMARY KEY (id)"))

    # Preserve service_id uniqueness
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'service_profiles' AND c.conname = 'service_profiles_service_id_key') THEN
                ALTER TABLE service_profiles ADD CONSTRAINT service_profiles_service_id_key UNIQUE (service_id);
            END IF;
        END$$;
        """
    ))

    # 2) Make master_profiles.master_id nullable so inserts using master_telegram_id succeed
    # Only alter if column exists and is NOT NULL
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='master_profiles' AND column_name='master_id' AND is_nullable='NO') THEN
                ALTER TABLE master_profiles ALTER COLUMN master_id DROP NOT NULL;
            END IF;
        END$$;
        """
    ))


def downgrade() -> None:
    conn = op.get_bind()

    # Revert master_id nullable -> NOT NULL if possible (will fail if nulls exist)
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='master_profiles' AND column_name='master_id' AND is_nullable='YES') THEN
                -- Attempt to set NOT NULL only when safe
                IF (SELECT COUNT(*) FROM master_profiles WHERE master_id IS NULL) = 0 THEN
                    ALTER TABLE master_profiles ALTER COLUMN master_id SET NOT NULL;
                END IF;
            END IF;
        END$$;
        """
    ))

    # Drop primary key on id and restore primary key on service_id
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'service_profiles' AND c.contype = 'p') THEN
                ALTER TABLE service_profiles DROP CONSTRAINT service_profiles_pkey;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'service_profiles' AND c.conname = 'service_profiles_pkey') THEN
                ALTER TABLE service_profiles ADD PRIMARY KEY (service_id);
            END IF;
        END$$;
        """
    ))

    # Remove id column
    op.drop_column('service_profiles', 'id')

    # Optionally drop the unique constraint on service_id if it was added by upgrade
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid WHERE t.relname = 'service_profiles' AND c.conname = 'service_profiles_service_id_key') THEN
                ALTER TABLE service_profiles DROP CONSTRAINT service_profiles_service_id_key;
            END IF;
        END$$;
        """
    ))
