"""Create master_schedule_exceptions table and add unique index on master_schedules

Idempotent, audit-first migration that creates a table to store per-date
exceptions and adds a unique index on master_schedules to allow safe
idempotent upserts from backfill scripts.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_add_master_schedule_exceptions_and_uniq_master_schedules"
down_revision = "38c45933ecde"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # Create exceptions table if not exists (audit-first, idempotent)
    conn.execute(
        sa.text(
            """
            CREATE TABLE IF NOT EXISTS master_schedule_exceptions (
                id SERIAL PRIMARY KEY,
                master_profile_id INTEGER NOT NULL REFERENCES master_profiles(id) ON DELETE CASCADE,
                exception_date DATE NOT NULL,
                start_time TIME NOT NULL,
                end_time TIME NOT NULL,
                reason TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
    )

    # Create unique index on exceptions to make backfills idempotent
    conn.execute(
        sa.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_master_schedule_exceptions_master_profile_date_times ON master_schedule_exceptions (master_profile_id, exception_date, start_time, end_time);"
        )
    )

    # Add a unique index on master_schedules to allow ON CONFLICT upserts
    conn.execute(
        sa.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_master_schedules_master_profile_day_start_end ON master_schedules (master_profile_id, day_of_week, start_time, end_time);"
        )
    )


def downgrade() -> None:
    conn = op.get_bind()
    conn.execute(sa.text("DROP INDEX IF EXISTS ux_master_schedules_master_profile_day_start_end;"))
    conn.execute(sa.text("DROP INDEX IF EXISTS ux_master_schedule_exceptions_master_profile_date_times;"))
    conn.execute(sa.text("DROP TABLE IF EXISTS master_schedule_exceptions;"))
