"""Rename master_client_notes.client_id to user_id and add FK to users

Revision ID: 20251204_rename_master_client_notes_client_id_to_user_id
Revises: 20251204_add_master_client_notes_master_telegram_id
Create Date: 2025-12-04 03:20:00.000000
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "20251204_rename_master_client_notes_client_id_to_user_id"
down_revision = "20251204_add_master_client_notes_master_telegram_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rename column client_id -> user_id if needed, then add FK to users(id).
    # Guard all operations by checking the table exists so migrations are safe on fresh/divergent DBs.
    op.execute(
        """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN
            -- rename column if client_id exists and user_id does not
            IF EXISTS(
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name='master_client_notes' AND column_name='client_id'
            ) AND NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name='master_client_notes' AND column_name='user_id'
            ) THEN
                ALTER TABLE public.master_client_notes RENAME COLUMN client_id TO user_id;
            END IF;

            -- add FK constraint to users.id if not present
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_class t ON c.conrelid = t.oid
                WHERE t.relname = 'master_client_notes' AND c.contype = 'f'
                  AND pg_get_constraintdef(c.oid) LIKE '%(user_id) REFERENCES users(id)%'
            ) THEN
                ALTER TABLE public.master_client_notes
                  ADD CONSTRAINT fk_master_client_notes_user_id FOREIGN KEY (user_id) REFERENCES public.users(id);
            END IF;
        END IF;
    END$$;
        """
    )


def downgrade() -> None:
    # Reverse the rename if possible: rename user_id back to client_id if client_id missing.
    op.execute(
        """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'master_client_notes') THEN
            IF EXISTS(
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name='master_client_notes' AND column_name='user_id'
            ) AND NOT EXISTS(
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name='master_client_notes' AND column_name='client_id'
            ) THEN
                -- drop FK if exists
                IF EXISTS (
                    SELECT 1 FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    WHERE t.relname = 'master_client_notes' AND c.contype = 'f'
                      AND pg_get_constraintdef(c.oid) LIKE '%(user_id) REFERENCES users(id)%'
                ) THEN
                    ALTER TABLE public.master_client_notes DROP CONSTRAINT IF EXISTS fk_master_client_notes_user_id;
                END IF;
                ALTER TABLE public.master_client_notes RENAME COLUMN user_id TO client_id;
            END IF;
        END IF;
    END$$;
        """
    )
