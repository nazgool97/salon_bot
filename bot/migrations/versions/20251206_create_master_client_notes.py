"""create master_client_notes table

Revision ID: 20251206_create_master_client_notes
Revises: 20251206_create_booking_ratings
Create Date: 2025-12-04 12:09:30.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_create_master_client_notes'
down_revision = '20251206_create_booking_ratings'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create master_client_notes table if it doesn't exist (idempotent)
    op.execute('''
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_name = 'master_client_notes'
        ) THEN
            CREATE TABLE master_client_notes (
                id SERIAL PRIMARY KEY,
                master_id BIGINT,
                client_id INTEGER,
                note TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
            );

            CREATE INDEX ix_master_client_notes_master_id ON master_client_notes (master_id);
            CREATE INDEX ix_master_client_notes_client_id ON master_client_notes (client_id);
        END IF;
    END
    $$;
    ''')


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS master_client_notes;")

