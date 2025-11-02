"""Add master and service profile tables

Revision ID: 0007_add_profile_tables
Revises: 0006_add_price_tracking_columns
Create Date: 2025-10-04 12:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0007_add_profile_tables"
down_revision = "0006_add_price_tracking_columns"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "master_profiles",
        sa.Column(
            "master_id",
            sa.BigInteger(),
            sa.ForeignKey("masters.telegram_id"),
            primary_key=True,
        ),
        sa.Column("bio", sa.String(length=2048), nullable=True),
        sa.Column("specialties", sa.String(length=1024), nullable=True),
        sa.Column("instagram_url", sa.String(length=512), nullable=True),
        sa.Column("portfolio_url", sa.String(length=512), nullable=True),
        sa.Column("photo_file_id", sa.String(length=256), nullable=True),
        sa.Column("avg_rating", sa.Integer(), nullable=True),
        sa.Column("reviews_count", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )
    op.create_table(
        "service_profiles",
        sa.Column(
            "service_id",
            sa.String(length=64),
            sa.ForeignKey("services.id"),
            primary_key=True,
        ),
        sa.Column("description", sa.String(length=2048), nullable=True),
        sa.Column("duration_minutes", sa.Integer(), nullable=True),
        sa.Column("base_price_cents", sa.Integer(), nullable=True),
        sa.Column("portfolio_url", sa.String(length=512), nullable=True),
        sa.Column("photo_file_id", sa.String(length=256), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("service_profiles")
    op.drop_table("master_profiles")
