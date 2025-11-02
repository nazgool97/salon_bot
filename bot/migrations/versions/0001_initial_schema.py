"""Initial database schema

Revision ID: 0001_initial_schema
Revises:
Create Date: 2025-10-01 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create users table
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("telegram_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("locale", sa.String(length=8), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("telegram_id"),
    )

    # Create masters table
    op.create_table(
        "masters",
        sa.Column("telegram_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("telegram_id"),
    )

    # Create services table
    op.create_table(
        "services",
        sa.Column("id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("price_cents", sa.Integer(), nullable=True),
        sa.Column("currency", sa.String(length=8), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create master_services junction table
    op.create_table(
        "master_services",
        sa.Column("master_telegram_id", sa.BigInteger(), nullable=False),
        sa.Column("service_id", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["master_telegram_id"],
            ["masters.telegram_id"],
        ),
        sa.ForeignKeyConstraint(
            ["service_id"],
            ["services.id"],
        ),
        sa.PrimaryKeyConstraint("master_telegram_id", "service_id"),
    )

    # Create bookings table with all fields
    op.create_table(
        "bookings",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("master_id", sa.BigInteger(), nullable=True),
        sa.Column("service_id", sa.String(), nullable=False),
        sa.Column("starts_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "active",
                "cancelled",
                "done",
                "no_show",
                "awaiting_cash",
                "pending_payment",
                "paid",
                name="booking_status",
            ),
            nullable=False,
        ),
        # Payment fields
        sa.Column("payment_provider", sa.String(length=128), nullable=True),
        sa.Column("payment_id", sa.String(length=256), nullable=True),
        sa.Column("paid_at", sa.DateTime(), nullable=True),
        sa.Column("cash_hold_expires_at", sa.DateTime(), nullable=True),
        sa.Column("original_price_cents", sa.Integer(), nullable=True),
        sa.Column("final_price_cents", sa.Integer(), nullable=True),
        sa.Column("discount_applied", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["master_id"],
            ["masters.telegram_id"],
        ),
        sa.ForeignKeyConstraint(
            ["service_id"],
            ["services.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "master_id", "starts_at", name="uq_bookings_master_starts_at"
        ),
    )

    # Create indexes
    op.create_index("ix_bookings_master_id", "bookings", ["master_id"], unique=False)
    op.create_index("ix_bookings_user_id", "bookings", ["user_id"], unique=False)
    op.create_index(
        "ix_master_services_service_id", "master_services", ["service_id"], unique=False
    )
    op.create_index(
        op.f("ix_bookings_starts_at"), "bookings", ["starts_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_bookings_starts_at"), table_name="bookings")
    op.drop_index("ix_master_services_service_id", table_name="master_services")
    op.drop_index("ix_bookings_user_id", table_name="bookings")
    op.drop_index("ix_bookings_master_id", table_name="bookings")
    op.drop_table("bookings")
    op.drop_table("master_services")
    op.drop_table("services")
    op.drop_table("masters")
    op.drop_table("users")
