"""add duration_minutes and price_cents to service_profiles

Revision ID: 20251022_add_duration_minutes_and_price_cents_to_service_profiles
Revises: 20251021_add_booking_items
Create Date: 2025-10-22 10:00:00

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251022_add_duration_minutes_and_price_cents_to_service_profiles'
down_revision = '20251021_add_booking_items'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем столбец duration_minutes
    op.add_column(
        "service_profiles",
        sa.Column("duration_minutes", sa.Integer, nullable=False, default=30)
    )
    # Добавляем столбец price_cents (если используется)
    op.add_column(
        "service_profiles",
        sa.Column("price_cents", sa.Integer, nullable=False, default=0)
    )
    # Заполняем duration_minutes для существующих записей
    op.execute("UPDATE service_profiles SET duration_minutes = 30 WHERE duration_minutes IS NULL")
    op.execute("UPDATE service_profiles SET price_cents = 0 WHERE price_cents IS NULL")

def downgrade() -> None:
    op.drop_column("service_profiles", "duration_minutes")
    op.drop_column("service_profiles", "price_cents")