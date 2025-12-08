"""create booking_items table

Revision ID: 20251206_create_booking_items
Revises: 20251206_add_master_services_duration_minutes
Create Date: 2025-12-04 11:29:30.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_create_booking_items'
down_revision = '20251206_add_master_services_duration_minutes'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create booking_items table if it doesn't exist.
    op.execute('''
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables WHERE table_name = 'booking_items'
        ) THEN
            CREATE TABLE booking_items (
                id SERIAL PRIMARY KEY,
                booking_id INTEGER REFERENCES bookings(id) ON DELETE CASCADE,
                service_id VARCHAR(64) REFERENCES services(id) ON DELETE CASCADE,
                position INTEGER DEFAULT 0 NOT NULL
            );
        END IF;
    END
    $$;
    ''')


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS booking_items;")
