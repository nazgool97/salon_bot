"""Add JSONB column `value_json` to settings and backfill from text values.

This migration is idempotent: it creates the column/index only if missing
and backfills rows using a PL/pgSQL block that attempts to cast each
text value to JSONB, skipping rows that fail to cast.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_add_settings_value_json"
down_revision = "20251205_add_master_schedule_exceptions_and_uniq_master_schedules"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Add JSONB column if not exists
    conn.execute(sa.text("ALTER TABLE settings ADD COLUMN IF NOT EXISTS value_json JSONB;"))

    # Backfill: attempt to cast each non-null value into JSONB; skip failures
    conn.execute(
        sa.text(
            """
            DO $$
            DECLARE r record;
            BEGIN
              FOR r IN SELECT id, value FROM settings WHERE value IS NOT NULL LOOP
                BEGIN
                  UPDATE settings SET value_json = r.value::jsonb WHERE id = r.id;
                EXCEPTION WHEN others THEN
                  -- Skip rows that are not valid JSON
                  CONTINUE;
                END;
              END LOOP;
            END$$;
            """
        )
    )

    # Create GIN index to accelerate JSON queries (idempotent)
    conn.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_settings_value_json_gin ON settings USING gin (value_json);"))


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_settings_value_json_gin;"))
    conn.execute(sa.text("ALTER TABLE settings DROP COLUMN IF EXISTS value_json;"))
