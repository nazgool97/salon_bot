"""Set services.created_at to timestamptz (timezone-aware)

Revision ID: 20251117_set_service_created_at_timestamptz
Revises: 20251111_add_exclusion_constraints_bookings
Create Date: 2025-11-17 10:00:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251117_set_service_created_at_timestamptz"
down_revision = "20251111_add_exclusion_constraints_bookings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Alter services.created_at from timestamp without time zone -> with time zone
    # Use USING clause to cast values explicitly
    with op.batch_alter_table("services") as batch_op:
        batch_op.alter_column(
            "created_at",
            type_=sa.DateTime(timezone=True),
            existing_type=sa.DateTime(timezone=False),
            existing_nullable=True,
            postgresql_using="created_at AT TIME ZONE 'UTC'",
        )


def downgrade() -> None:
    # Revert to timestamp without time zone
    with op.batch_alter_table("services") as batch_op:
        batch_op.alter_column(
            "created_at",
            type_=sa.DateTime(timezone=False),
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=True,
            postgresql_using="created_at",
        )
