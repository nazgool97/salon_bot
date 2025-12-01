"""Add nullable duration_minutes to master_services

Revision ID: 20251118_master_service_duration
Revises: 20251117_set_service_created_at_timestamptz
Create Date: 2025-11-18 00:00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251118_master_service_duration"
down_revision = "20251117_set_service_created_at_timestamptz"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "master_services",
        sa.Column("duration_minutes", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("master_services", "duration_minutes")
