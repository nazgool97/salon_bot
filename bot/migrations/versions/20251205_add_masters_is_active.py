"""Add `is_active` boolean to `masters` for soft delete.

Idempotent migration: adds column with default TRUE and backfills existing
rows. Keeps physical rows intact so historical data keeps master references.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_add_masters_is_active"
down_revision = "20251205_add_settings_value_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # add column if missing, default true for existing rows
    conn.execute(sa.text("ALTER TABLE masters ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true;"))
    # ensure existing rows have true (idempotent)
    conn.execute(sa.text("UPDATE masters SET is_active = true WHERE is_active IS NULL;"))
    # add index to quickly filter active masters
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_masters_is_active ON masters (is_active);"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_masters_is_active;"))
    conn.execute(sa.text("ALTER TABLE masters DROP COLUMN IF EXISTS is_active;"))
