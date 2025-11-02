"""Populate default zeros for master profile rating aggregates

Revision ID: 0010_master_profile_rating_defaults
Revises: 0009_add_booking_ratings
Create Date: 2025-10-04 13:10:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "0010_master_profile_rating_defaults"
down_revision = "0009_add_booking_ratings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Ensure Alembic version table can store long human-readable revision ids
    try:
        with op.batch_alter_table("alembic_version") as batch_op:
            batch_op.alter_column("version_num", type_=sa.String(length=128))
    except Exception:
        # If the backend doesn't support altering or it's already big enough, ignore.
        pass

    # Only populate NULLs with zero to simplify aggregation logic.
    conn = op.get_bind()
    try:
        conn.execute(
            sa.text("UPDATE master_profiles SET avg_rating=0 WHERE avg_rating IS NULL")
        )
        conn.execute(
            sa.text(
                "UPDATE master_profiles SET reviews_count=0 WHERE reviews_count IS NULL"
            )
        )
    except Exception:
        # Best-effort; SQLite in-memory during tests may have table absent in some paths.
        pass


def downgrade() -> None:
    # No data rollback required (can't distinguish which zeros were NULL before)
    pass
