"""Add booking_items table for multi-service bookings

Revision ID: 20251021_add_booking_items
Revises: 20251020_add_service_category
Create Date: 2025-10-21 10:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251021_add_booking_items"
down_revision = "20251020_add_service_category"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "booking_items",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("booking_id", sa.Integer(), sa.ForeignKey("bookings.id", ondelete="CASCADE"), nullable=False),
        sa.Column("service_id", sa.String(length=64), sa.ForeignKey("services.id"), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_index("ix_booking_items_booking_id", "booking_items", ["booking_id"]) 
    op.create_index("ix_booking_items_service_id", "booking_items", ["service_id"]) 


def downgrade() -> None:
    op.drop_index("ix_booking_items_service_id", table_name="booking_items")
    op.drop_index("ix_booking_items_booking_id", table_name="booking_items")
    op.drop_table("booking_items")
