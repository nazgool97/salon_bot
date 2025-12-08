"""FINAL MIGRATION: add missing enum values + deduplicate + create partial unique index

Revision ID: 6f11a89a3d86
Revises: 20251130_add_last_reminder_fields
Create Date: 2025-12-03 20:00:00

"""
from alembic import op
import sqlalchemy as sa


revision = '6f11a89a3d86'
down_revision = '20251130_add_last_reminder_fields'
branch_labels = None
depends_on = None


def upgrade():
    # 1. Добавляем все недостающие значения в enum
    op.execute("ALTER TYPE booking_status ADD VALUE IF NOT EXISTS 'reserved'")
    op.execute("ALTER TYPE booking_status ADD VALUE IF NOT EXISTS 'confirmed'")
    op.execute("ALTER TYPE booking_status ADD VALUE IF NOT EXISTS 'expired'")
    op.execute("ALTER TYPE booking_status ADD VALUE IF NOT EXISTS 'awaiting_cash'")

    # 2. Чистим дубликаты активных броней
    op.execute("""
        WITH active AS (
            SELECT id, master_id, starts_at
            FROM bookings
            WHERE status IN ('reserved','pending_payment','confirmed','awaiting_cash','paid','active')
        ),
        dup_groups AS (
            SELECT master_id, starts_at, array_agg(id ORDER BY id) AS ids
            FROM active
            GROUP BY master_id, starts_at
            HAVING count(*) > 1
        ),
        to_cancel AS (
            SELECT unnest(ids[2:])::int AS id
            FROM dup_groups
        )
        UPDATE bookings
        SET status = 'cancelled'
        WHERE id IN (SELECT id FROM to_cancel)
    """)

    # 3. Теперь безопасно создаём partial unique index
    op.create_index(
        'ux_bookings_master_start_active',
        'bookings',
        ['master_id', 'starts_at'],
        unique=True,
        postgresql_where=sa.text(
            "status IN ('reserved','pending_payment','confirmed','awaiting_cash','paid','active')"
        ),
    )


def downgrade():
    op.drop_index('ux_bookings_master_start_active', table_name='bookings')
    # enum значения не удаляем — безопасно
    pass