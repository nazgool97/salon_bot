"""add timezone setting with default UTC

Revision ID: 20251205_add_settings_timezone
Revises: 20251204_add_master_client_notes_master_telegram_id
Create Date: 2025-12-05 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251205_add_settings_timezone'
down_revision = '20251204_add_master_client_notes_master_telegram_id'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Insert timezone setting with default 'UTC' if it does not exist
    conn.execute(sa.text(
        """
        INSERT INTO settings (key, value, updated_at)
        SELECT 'timezone', 'UTC', now()
        WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = 'timezone');
        """
    ))


def downgrade() -> None:
    conn = op.get_bind()
    # Remove the timezone setting (best-effort)
    conn.execute(sa.text("DELETE FROM settings WHERE key = 'timezone'"))
