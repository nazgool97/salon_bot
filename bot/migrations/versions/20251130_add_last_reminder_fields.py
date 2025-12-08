"""Add last_reminder_sent_at and last_reminder_lead_minutes to bookings

Revision ID: 20251130_add_last_reminder_fields
Revises: 20251122_merge_updated_at_and_master_names
Create Date: 2025-11-30 12:00:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20251130_add_last_reminder_fields"
down_revision = "20251122_merge_updated_at_and_master_names"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "bookings",
        sa.Column("last_reminder_sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "bookings",
        sa.Column("last_reminder_lead_minutes", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    with op.batch_alter_table("bookings") as batch_op:
        batch_op.drop_column("last_reminder_lead_minutes")
        batch_op.drop_column("last_reminder_sent_at")
