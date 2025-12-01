"""Add updated_at columns to settings and master_schedules

Revision ID: 20251119061000_add_updated_at_columns
Revises: 9b55e7a43db8
Create Date: 2025-11-19 06:10:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = "20251119061000_add_updated_at_columns"
down_revision = "9b55e7a43db8"
branch_labels = None
depends_on = None


def upgrade():
    # settings.updated_at
    op.add_column(
        "settings",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )
    # master_schedules.updated_at
    op.add_column(
        "master_schedules",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    # Ensure existing rows get a timestamp (server_default covers new ones)
    conn = op.get_bind()
    try:
        conn.execute(sa.text("UPDATE settings SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"))
    except Exception:
        pass
    try:
        conn.execute(sa.text("UPDATE master_schedules SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL"))
    except Exception:
        pass

    # Remove server_default so future updates rely on application logic
    with op.batch_alter_table("settings") as batch:
        batch.alter_column("updated_at", server_default=None)
    with op.batch_alter_table("master_schedules") as batch:
        batch.alter_column("updated_at", server_default=None)


def downgrade():
    with op.batch_alter_table("master_schedules") as batch:
        batch.drop_column("updated_at")
    with op.batch_alter_table("settings") as batch:
        batch.drop_column("updated_at")
