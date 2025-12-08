"""Ensure master_client_notes has master_telegram_id and user_id with FKs

Revision ID: 20251206_fix_master_client_notes_columns
Revises: 20251206_create_master_client_notes
Create Date: 2025-12-04 12:18:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_fix_master_client_notes_columns'
down_revision = '20251206_create_master_client_notes'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add master_telegram_id and ensure user_id exists (rename client_id if necessary)
    op.execute('''
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN

            -- add master_telegram_id column if missing
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'master_telegram_id'
            ) THEN
                ALTER TABLE public.master_client_notes ADD COLUMN master_telegram_id BIGINT;
                EXECUTE 'UPDATE public.master_client_notes SET master_telegram_id = master_id WHERE master_telegram_id IS NULL AND master_id IS NOT NULL';
            END IF;

            -- add FK for master_telegram_id if missing
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'master_client_notes' AND c.conname = 'master_client_notes_master_telegram_id_fkey'
            ) THEN
                ALTER TABLE public.master_client_notes
                ADD CONSTRAINT master_client_notes_master_telegram_id_fkey
                FOREIGN KEY (master_telegram_id)
                REFERENCES public.masters(telegram_id) ON DELETE CASCADE;
            END IF;

            -- ensure user_id column exists: rename client_id -> user_id if needed, otherwise add
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'user_id'
            ) THEN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'client_id'
                ) THEN
                    ALTER TABLE public.master_client_notes RENAME COLUMN client_id TO user_id;
                ELSE
                    ALTER TABLE public.master_client_notes ADD COLUMN user_id INTEGER;
                END IF;
            END IF;

            -- add FK for user_id if missing
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'master_client_notes' AND pg_get_constraintdef(c.oid) LIKE '%(user_id) REFERENCES users(id)%'
            ) THEN
                ALTER TABLE public.master_client_notes
                ADD CONSTRAINT fk_master_client_notes_user_id FOREIGN KEY (user_id) REFERENCES public.users(id);
            END IF;

            -- add indexes if missing
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ix_master_client_notes_master_telegram_id') THEN
                CREATE INDEX ix_master_client_notes_master_telegram_id ON public.master_client_notes(master_telegram_id);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = 'ix_master_client_notes_user_id') THEN
                CREATE INDEX ix_master_client_notes_user_id ON public.master_client_notes(user_id);
            END IF;

        END IF;
    END$$;
    ''')


def downgrade() -> None:
    # Drop the added constraints and columns if they exist (non-destructive where possible)
    op.execute('''
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN
            -- drop FK for master_telegram_id
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'master_client_notes' AND c.conname = 'master_client_notes_master_telegram_id_fkey'
            ) THEN
                ALTER TABLE public.master_client_notes DROP CONSTRAINT master_client_notes_master_telegram_id_fkey;
            END IF;
            -- drop master_telegram_id column if exists
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'master_telegram_id'
            ) THEN
                ALTER TABLE public.master_client_notes DROP COLUMN master_telegram_id;
            END IF;

            -- drop FK for user_id if exists (do not drop the column to avoid data loss)
            IF EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'master_client_notes' AND pg_get_constraintdef(c.oid) LIKE '%(user_id) REFERENCES users(id)%'
            ) THEN
                ALTER TABLE public.master_client_notes DROP CONSTRAINT IF EXISTS fk_master_client_notes_user_id;
            END IF;
        END IF;
    END$$;
    ''')
