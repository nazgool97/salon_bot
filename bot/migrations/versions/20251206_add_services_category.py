"""add category to services

Revision ID: 20251206_add_services_category
Revises: 20251206_create_booking_items
Create Date: 2025-12-04 12:05:00.000000

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '20251206_add_services_category'
down_revision = '20251206_create_booking_items'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Idempotent: add nullable `category` column to `services` if missing
    op.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='services' AND column_name='category'
        ) THEN
            ALTER TABLE services ADD COLUMN category VARCHAR(100);
        END IF;
    END
    $$;
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE IF EXISTS services DROP COLUMN IF EXISTS category;")
