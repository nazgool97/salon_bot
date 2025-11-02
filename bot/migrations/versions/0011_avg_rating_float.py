"""Alter master_profiles.avg_rating to float

Revision ID: 0011_avg_rating_float
Revises: 0010_master_profile_rating_defaults
Create Date: 2025-10-04 13:25:00.000000
"""

import sqlalchemy as sa
from alembic import op

revision = "0011_avg_rating_float"
down_revision = "0010_master_profile_rating_defaults"
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        op.alter_column("master_profiles", "avg_rating", type_=sa.Float())
    except Exception:
        # SQLite or already altered – non-fatal
        pass


def downgrade() -> None:
    try:
        op.alter_column("master_profiles", "avg_rating", type_=sa.Integer())
    except Exception:
        pass
