"""Migrate EAV `settings` (key,value) into a single-row JSONB `app_settings`.

Revision ID: 20251204_migrate_settings_to_jsonb
Revises: 20251204_drop_master_profiles_master_id
Create Date: 2025-12-04 12:45:00

This migration is guarded and idempotent:
 - If `app_settings` already exists, it is a no-op.
 - If `settings` table doesn't exist, it will create `app_settings` with an
   empty JSON object.
 - It preserves the old `settings` table (no DROP) so rollback / inspection
   is possible.

After this migration you may want to update application code to read from
`app_settings.data` (JSONB) rather than the `settings` key/value table.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_migrate_settings_to_jsonb"
down_revision = "20251204_drop_master_profiles_master_id"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # If app_settings already exists, nothing to do
    exists = conn.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='app_settings')")
    ).scalar()
    if exists:
        return

    # Create app_settings table with a single-row jsonb column
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            id bigint PRIMARY KEY,
            data jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at timestamptz DEFAULT now()
        );
        """
    )

    # Backfill from existing settings key/value table when present
    has_kv = conn.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='settings')")
    ).scalar()

    if has_kv:
        # Build JSONB object from key/value pairs. Use empty object when none.
        # Use string values as-is; application can cast/parse types as needed.
        conn.execute(
            sa.text(
                """
                WITH kv AS (
                    SELECT jsonb_object_agg(key, value) AS obj FROM settings
                )
                INSERT INTO app_settings(id, data)
                SELECT 1, COALESCE(kv.obj, '{}'::jsonb)
                FROM kv;
                """
            )
        )
    else:
        # No old settings table: ensure single empty row exists
        conn.execute(sa.text("INSERT INTO app_settings(id, data) VALUES (1, '{}'::jsonb) ON CONFLICT (id) DO NOTHING"))


def downgrade() -> None:
    # We intentionally do not drop app_settings in downgrade to avoid data loss.
    pass
