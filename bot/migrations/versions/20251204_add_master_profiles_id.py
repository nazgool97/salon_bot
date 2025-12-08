"""Add integer primary key `id` to master_profiles and migrate FKs

Revision ID: 20251204_add_master_profiles_id
Revises: 20251203_add_bookingstatus_remaining_labels
Create Date: 2025-12-04 01:00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_add_master_profiles_id"
down_revision = "20251203_add_bookingstatus_remaining_labels"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Bind/connection
    conn = op.get_bind()

    # Defensive: some init flows may not have created `master_profiles` yet
    # (schema dump vs migrations ordering). Create minimal table if absent
    # so the rest of this migration can safely run.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS master_profiles (
            master_id BIGINT,
            bio VARCHAR(2048),
            specialties VARCHAR(1024),
            instagram_url VARCHAR(512),
            portfolio_url VARCHAR(512),
            photo_file_id VARCHAR(256),
            avg_rating DOUBLE PRECISION,
            reviews_count INTEGER,
            updated_at TIMESTAMP WITHOUT TIME ZONE
        );
        """
    )

    # Defensive: ensure `master_schedules` exists so we can update and change FKs safely.
    # This mirrors the minimal expected schema used by later migrations.
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS master_schedules (
            id BIGINT,
            master_profile_id BIGINT,
            day_of_week INTEGER,
            start_time TIME WITHOUT TIME ZONE,
            end_time TIME WITHOUT TIME ZONE,
            is_day_off BOOLEAN DEFAULT false NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE
        );
        """
    )

    # 1) Ensure sequence exists and add `id` column if missing
    op.execute("CREATE SEQUENCE IF NOT EXISTS master_profiles_id_seq")
    try:
        op.add_column(
            "master_profiles",
            sa.Column("id", sa.BigInteger(), nullable=True),
        )
    except Exception:
        # If column already exists, continue
        pass

    # 2) Populate id for existing rows where missing using the sequence
    op.execute("UPDATE master_profiles SET id = nextval('master_profiles_id_seq') WHERE id IS NULL")

    # 3) Set sequence last_value to MAX(id) or 1 if table empty
    op.execute("SELECT setval('master_profiles_id_seq', COALESCE((SELECT MAX(id) FROM master_profiles), 1))")

    # 4) Make id NOT NULL and default to nextval()
    op.execute("ALTER TABLE master_profiles ALTER COLUMN id SET NOT NULL")
    op.execute("ALTER TABLE master_profiles ALTER COLUMN id SET DEFAULT nextval('master_profiles_id_seq')")

    # 5) Add a UNIQUE constraint on id so other tables can reference it safely
    op.execute(
        """
        DO $do$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'master_profiles_id_key') THEN
                ALTER TABLE master_profiles ADD CONSTRAINT master_profiles_id_key UNIQUE (id);
            END IF;
        END
        $do$;
        """
    )

    # 6) Update master_schedules.master_profile_id to use master_profiles.id and recreate FK
    op.execute(
        """
        DO $do$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='master_schedules')
            AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='master_schedules' AND column_name='master_profile_id')
            THEN
                -- Update values by joining on the old master_id
                UPDATE master_schedules ms
                SET master_profile_id = mp.id
                FROM master_profiles mp
                WHERE ms.master_profile_id = mp.master_id;

                -- Drop old FK if exists and create FK to master_profiles(id)
                ALTER TABLE master_schedules DROP CONSTRAINT IF EXISTS master_schedules_master_profile_id_fkey;
                ALTER TABLE master_schedules
                ADD CONSTRAINT master_schedules_master_profile_id_fkey FOREIGN KEY (master_profile_id) REFERENCES master_profiles(id) ON DELETE CASCADE;
            END IF;
        END
        $do$ LANGUAGE plpgsql;
        """
    )

    # 7) Replace primary key on master_profiles to use `id`
    # Drop FK referencing id, drop old PK, drop temporary UNIQUE, create PK on id, then recreate FK
    op.execute("ALTER TABLE master_schedules DROP CONSTRAINT IF EXISTS master_schedules_master_profile_id_fkey")
    op.execute("ALTER TABLE master_profiles DROP CONSTRAINT IF EXISTS master_profiles_pkey")
    op.execute("ALTER TABLE master_profiles DROP CONSTRAINT IF EXISTS master_profiles_id_key")
    op.execute("ALTER TABLE master_profiles ADD CONSTRAINT master_profiles_pkey PRIMARY KEY (id)")
    op.execute("ALTER TABLE master_schedules ADD CONSTRAINT master_schedules_master_profile_id_fkey FOREIGN KEY (master_profile_id) REFERENCES master_profiles(id) ON DELETE CASCADE")


def downgrade() -> None:
    # Best-effort revert: restore FK to master_profiles.master_id if possible
    op.execute("ALTER TABLE master_schedules DROP CONSTRAINT IF EXISTS master_schedules_master_profile_id_fkey")
    op.execute(
        """
        DO $do$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='master_profiles' AND column_name='master_id') THEN
                ALTER TABLE master_schedules ADD CONSTRAINT master_schedules_master_profile_id_fkey FOREIGN KEY (master_profile_id) REFERENCES master_profiles(master_id) ON DELETE CASCADE;
            END IF;
        END
        $do$ LANGUAGE plpgsql;
        """
    )

    # Try to restore old PK on master_id if present
    op.execute("ALTER TABLE master_profiles DROP CONSTRAINT IF EXISTS master_profiles_pkey")
    op.execute(
        "DO $do$\n        BEGIN\n            IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='master_profiles' AND column_name='master_id') THEN\n                ALTER TABLE master_profiles ADD CONSTRAINT master_profiles_pkey PRIMARY KEY (master_id);\n            END IF;\n        END\n        $do$ LANGUAGE plpgsql;"
    )

    # leave new id column and sequence in place for manual cleanup if desired
