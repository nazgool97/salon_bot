"""Add booking_ratings table

Revision ID: 0009_add_booking_ratings
Revises: 0008_add_reminder_flags
Create Date: 2025-10-04 12:40:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0009_add_booking_ratings"
down_revision = "0008_add_reminder_flags"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "booking_ratings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "booking_id",
            sa.Integer(),
            sa.ForeignKey("bookings.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "master_id",
            sa.BigInteger(),
            sa.ForeignKey("masters.telegram_id"),
            nullable=False,
        ),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("rating", sa.Integer(), nullable=False),
        sa.Column("comment", sa.String(length=2048), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_booking_ratings_master_id", "booking_ratings", ["master_id"], unique=False
    )
    op.create_index(
        "ix_booking_ratings_user_id", "booking_ratings", ["user_id"], unique=False
    )
    op.create_index(
        "ix_booking_ratings_rating", "booking_ratings", ["rating"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_booking_ratings_rating", table_name="booking_ratings")
    op.drop_index("ix_booking_ratings_user_id", table_name="booking_ratings")
    op.drop_index("ix_booking_ratings_master_id", table_name="booking_ratings")
    op.drop_table("booking_ratings")
