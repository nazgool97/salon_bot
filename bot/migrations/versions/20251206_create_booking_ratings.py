"""create booking_ratings table

Revision ID: 20251206_create_booking_ratings
Revises: 20251206_add_services_category
Create Date: 2025-12-04 12:07:30.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_create_booking_ratings'
down_revision = '20251206_add_services_category'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create booking_ratings table if it doesn't exist (idempotent)
    op.execute('''
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_name = 'booking_ratings'
        ) THEN
            CREATE TABLE booking_ratings (
                id SERIAL PRIMARY KEY,
                booking_id INTEGER REFERENCES bookings(id) ON DELETE CASCADE,
                rating INTEGER NOT NULL,
                comment TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
            );
        END IF;
    END
    $$;
    ''')


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS booking_ratings;")
