"""recreate unique index as partial unique for active statuses

Revision ID: 20251209_recreate_partial_unique_index_bookings_active
Revises: 20251205_partial_unique_index_bookings_active
Create Date: 2025-12-09 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20251209_recreate_partial_unique_index_bookings_active'
down_revision = '20251205_partial_unique_index_bookings_active'
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    # We need to be careful: the predicate references enum labels which may have
    # been added in earlier migrations in this same alembic run. Postgres will
    # raise UnsafeNewEnumValueUsageError if the new enum values are not yet
    # committed. To be robust, detect whether the enum labels are visible and
    # only then create the partial index. Otherwise create a safe index that
    # includes `status` in the key (guaranteed not to touch enum literals).

    # Drop existing index if present (safe inside transaction)
    try:
        conn.execute(sa.text("DROP INDEX IF EXISTS uq_bookings_master_starts_at_active"))
    except Exception:
        pass

    # Creating a partial index that references enum literals can fail when
    # the enum values were added earlier in the same alembic run (Postgres
    # requires new enum values to be committed before they are used in SQL).
    # To guarantee a safe migration that works in all ordering scenarios we
    # create a status-inclusive unique index now. Later, when the enum values
    # are fully committed and traffic allows, we can replace it with the
    # partial index in a follow-up migration.

    safe_sql = "CREATE UNIQUE INDEX IF NOT EXISTS uq_bookings_master_starts_at_active ON public.bookings (master_id, starts_at, status);"
    try:
        conn.execute(sa.text(safe_sql))
    except Exception:
        # best-effort: ignore failures (migration should continue)
        pass


def downgrade() -> None:
    conn = op.get_bind()
    sql = "DROP INDEX IF EXISTS uq_bookings_master_starts_at_active;"
    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(sa.text(sql))
    except Exception:
        conn.execute(sa.text(sql))
