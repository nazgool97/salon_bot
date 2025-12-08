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
    # defensive: make sure alembic_version.version_num can hold long merge ids
    # Defensive: ensure alembic_version.version_num can hold long merge revision ids.
    # Some client DBs were initialized with varchar(32) and Alembic will fail
    # when trying to write longer merge revision identifiers. This ALTER is
    # safe to run on fresh DBs (IF EXISTS) and will no-op if the table is absent.
    op.execute(
            "ALTER TABLE IF EXISTS public.alembic_version ALTER COLUMN version_num TYPE varchar(128);"
    )
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
        sa.ForeignKeyConstraint(["master_telegram_id"], ["masters.telegram_id"]),
        sa.ForeignKeyConstraint(["service_id"], ["services.id"]),
        sa.PrimaryKeyConstraint("master_telegram_id", "service_id"),
    )

    # Create bookings table with booking_status enum
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
                "reserved",
                "confirmed",
                "expired",
                name="booking_status",
            ),
            nullable=False,
        ),
        sa.Column("payment_provider", sa.String(length=128), nullable=True),
        sa.Column("payment_id", sa.String(length=256), nullable=True),
        sa.Column("paid_at", sa.DateTime(), nullable=True),
        sa.Column("cash_hold_expires_at", sa.DateTime(), nullable=True),
        sa.Column("original_price_cents", sa.Integer(), nullable=True),
        sa.Column("final_price_cents", sa.Integer(), nullable=True),
        sa.Column("discount_applied", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["master_id"], ["masters.telegram_id"]),
        sa.ForeignKeyConstraint(["service_id"], ["services.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("master_id", "starts_at", name="uq_bookings_master_starts_at"),
    )

    # Create indexes
    op.create_index("ix_bookings_master_id", "bookings", ["master_id"], unique=False)
    op.create_index("ix_bookings_user_id", "bookings", ["user_id"], unique=False)
    op.create_index("ix_master_services_service_id", "master_services", ["service_id"], unique=False)
    op.create_index(op.f("ix_bookings_starts_at"), "bookings", ["starts_at"], unique=False)

    # Create master_profiles table (present in schema dumps used by some deployments)
    op.create_table(
        "master_profiles",
        sa.Column("master_id", sa.BigInteger(), nullable=False),
        sa.Column("bio", sa.String(length=2048), nullable=True),
        sa.Column("specialties", sa.String(length=1024), nullable=True),
        sa.Column("instagram_url", sa.String(length=512), nullable=True),
        sa.Column("portfolio_url", sa.String(length=512), nullable=True),
        sa.Column("photo_file_id", sa.String(length=256), nullable=True),
        sa.Column("avg_rating", sa.Float(), nullable=True),
        sa.Column("reviews_count", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("master_id"),
    )

    # Create master_schedules table (used by schedule migrations)
    op.create_table(
        "master_schedules",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("master_profile_id", sa.BigInteger(), nullable=False),
        sa.Column("day_of_week", sa.Integer(), nullable=False),
        sa.Column("start_time", sa.Time(), nullable=True),
        sa.Column("end_time", sa.Time(), nullable=True),
        sa.Column("is_day_off", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    # Create service_profiles table (present in full schema)
    op.create_table(
        "service_profiles",
        sa.Column("service_id", sa.String(length=64), nullable=False),
        sa.Column("description", sa.String(length=2048), nullable=True),
        sa.Column("duration_minutes", sa.Integer(), nullable=True),
        sa.Column("base_price_cents", sa.Integer(), nullable=True),
        sa.Column("portfolio_url", sa.String(length=512), nullable=True),
        sa.Column("photo_file_id", sa.String(length=256), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("price_cents", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("service_id"),
    )


def downgrade() -> None:
    # Drop tables created in upgrade
    op.drop_table("service_profiles")
    op.drop_table("master_schedules")
    op.drop_table("master_profiles")
    op.drop_index(op.f("ix_bookings_starts_at"), table_name="bookings")
    op.drop_index("ix_master_services_service_id", table_name="master_services")
    op.drop_index("ix_bookings_user_id", table_name="bookings")
    op.drop_index("ix_bookings_master_id", table_name="bookings")
    op.drop_table("bookings")
    op.drop_table("master_services")
    op.drop_table("services")
    op.drop_table("masters")
    op.drop_table("users")
    op.execute("DROP TYPE booking_status")