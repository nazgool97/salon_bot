"""Add master_client_notes table

Revision ID: 0012_master_client_notes
Revises: 0011_avg_rating_float
Create Date: 2025-10-04 13:55:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "0012_master_client_notes"
down_revision = "0011_avg_rating_float"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "master_client_notes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "master_id",
            sa.BigInteger(),
            sa.ForeignKey("masters.telegram_id"),
            nullable=False,
        ),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("note", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint(
            "master_id", "client_id", name="uq_master_client_unique_note"
        ),
    )
    op.create_index(
        "ix_master_client_notes_master_id",
        "master_client_notes",
        ["master_id"],
        unique=False,
    )
    op.create_index(
        "ix_master_client_notes_client_id",
        "master_client_notes",
        ["client_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_master_client_notes_client_id", table_name="master_client_notes")
    op.drop_index("ix_master_client_notes_master_id", table_name="master_client_notes")
    op.drop_table("master_client_notes")
