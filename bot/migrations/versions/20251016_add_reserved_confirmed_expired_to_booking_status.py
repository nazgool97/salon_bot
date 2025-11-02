"""add reserved/confirmed/expired to booking status enum

Revision ID: 20251016_add_new_statuses
Revises: 20251014_add_is_admin
Create Date: 2025-10-16 21:45:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '20251016_add_new_statuses'
down_revision = '20251014_add_is_admin'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The live DB uses enum type name 'bookingstatus' (detected at runtime). Append values there.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'bookingstatus') THEN
                ALTER TYPE bookingstatus ADD VALUE IF NOT EXISTS 'RESERVED';
                ALTER TYPE bookingstatus ADD VALUE IF NOT EXISTS 'CONFIRMED';
                ALTER TYPE bookingstatus ADD VALUE IF NOT EXISTS 'EXPIRED';
            END IF;
        END
        $$;
        """
    )


def downgrade() -> None:
    # Postgres does not support removing enum values without complex workaround; leave as no-op.
    pass
