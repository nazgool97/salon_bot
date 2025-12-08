"""Add index on master_schedules(master_profile_id, day_of_week)

This migration adds a composite btree index to speed up queries that
select schedules by `master_profile_id` and `day_of_week`.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251207_add_index_master_schedules"
down_revision = "20251207_archive_and_drop_app_settings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Create index if not exists (idempotent)
    conn.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_master_schedules_master_profile_id_day_of_week ON master_schedules (master_profile_id, day_of_week);"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ix_master_schedules_master_profile_id_day_of_week;"))
