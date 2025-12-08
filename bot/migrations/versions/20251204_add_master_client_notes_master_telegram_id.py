"""add master_telegram_id to master_client_notes

Revision ID: 20251204_add_master_client_notes_master_telegram_id
Revises: 20251204_set_bookings_boolean_defaults
Create Date: 2025-12-04 02:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251204_add_master_client_notes_master_telegram_id'
down_revision = '20251204_set_bookings_boolean_defaults'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Do nothing if the table doesn't exist (keeps migration safe for fresh/divergent DBs)
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN
                -- add column if missing
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'master_telegram_id'
                ) THEN
                    ALTER TABLE public.master_client_notes ADD COLUMN master_telegram_id BIGINT;
                END IF;

                -- populate from existing master_id (safe even if column was just added)
                EXECUTE 'UPDATE public.master_client_notes SET master_telegram_id = master_id WHERE master_telegram_id IS NULL AND master_id IS NOT NULL';

                -- add FK constraint if missing
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

                -- set NOT NULL if no nulls remain
                IF (SELECT COUNT(*) FROM public.master_client_notes WHERE master_telegram_id IS NULL) = 0 THEN
                    ALTER TABLE public.master_client_notes ALTER COLUMN master_telegram_id SET NOT NULL;
                END IF;
            END IF;
        END$$;
        """
    ))


def downgrade() -> None:
    conn = op.get_bind()
    # Drop FK and column only if the table exists
    conn.execute(sa.text(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN
                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'master_client_notes' AND c.conname = 'master_client_notes_master_telegram_id_fkey'
                ) THEN
                    ALTER TABLE public.master_client_notes DROP CONSTRAINT master_client_notes_master_telegram_id_fkey;
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'master_client_notes' AND column_name = 'master_telegram_id'
                ) THEN
                    ALTER TABLE public.master_client_notes DROP COLUMN master_telegram_id;
                END IF;
            END IF;
        END$$;
        """
    ))
