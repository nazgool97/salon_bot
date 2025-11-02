"""Add reminder & feedback flags to bookings

Revision ID: 0008_add_reminder_flags
Revises: 0007_add_profile_tables
Create Date: 2025-10-04 12:20:00.000000
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0008_add_reminder_flags"
down_revision = "0007_add_profile_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "bookings",
        sa.Column(
            "remind_24h_sent", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )
    op.add_column(
        "bookings",
        sa.Column(
            "remind_1h_sent", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )
    op.add_column(
        "bookings",
        sa.Column("feedback_prompt_scheduled_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "bookings",
        sa.Column(
            "feedback_prompt_sent",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Remove server_default after migration so future inserts rely on ORM defaults
    with op.batch_alter_table("bookings") as batch_op:
        batch_op.alter_column("remind_24h_sent", server_default=None)
        batch_op.alter_column("remind_1h_sent", server_default=None)
        batch_op.alter_column("feedback_prompt_sent", server_default=None)


def downgrade() -> None:
    with op.batch_alter_table("bookings") as batch_op:
        batch_op.drop_column("feedback_prompt_sent")
        batch_op.drop_column("feedback_prompt_scheduled_at")
        batch_op.drop_column("remind_1h_sent")
        batch_op.drop_column("remind_24h_sent")
