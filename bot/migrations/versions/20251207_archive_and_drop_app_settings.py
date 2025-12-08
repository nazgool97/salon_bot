"""Archive `app_settings` contents and drop the table.

This guarded migration copies the single-row `app_settings` into
`app_settings_archive` (with timestamp) and then drops the original
`app_settings` table. It is idempotent: if `app_settings` is already
missing, the migration is a no-op. If `app_settings_archive` already
exists, it will still insert the current row (if any) to keep a record.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251207_archive_and_drop_app_settings"
down_revision = "20251207_backfill_settings_from_appsettings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # If app_settings doesn't exist, nothing to do
    exists = conn.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='app_settings')")
    ).scalar()
    if not exists:
        return

    # Ensure archive table exists
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS app_settings_archive (id bigint, data jsonb, updated_at timestamptz, archived_at timestamptz DEFAULT now())"
        )
    )

    # Copy current contents (if any) into archive
    conn.execute(
        sa.text(
            "INSERT INTO app_settings_archive (id, data, updated_at) SELECT id, data, updated_at FROM app_settings"
        )
    )

    # Backup done — drop the original table
    conn.execute(sa.text("DROP TABLE IF EXISTS app_settings CASCADE"))


def downgrade() -> None:
    # Downgrade is a no-op: restoring dropped app_settings requires manual intervention.
    pass
