"""Finalize masters ID migration: rename master_id_new -> master_id and drop telegram columns

Revision ID: 20251204_finalize_masters_id_migration
Revises: 20251204_add_masters_surrogate_id
Create Date: 2025-12-04 14:40:00

This guarded migration finalizes the staged masters PK rollout by renaming
`*_master_id_new` columns to the canonical `master_id`, validating that the
backfill completed, adding NOT NULL + FK constraints, and removing legacy
telegram-based columns where safe.

The migration is conservative: it audits any rows that would be left without
`master_id` after the rename and aborts so an operator can reconcile them.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_finalize_masters_id_migration"
down_revision = "20251204_add_masters_surrogate_id"
branch_labels = None
depends_on = None


def _col_exists(conn, table: str, column: str) -> bool:
    return bool(
        conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.columns WHERE table_name = :table AND column_name = :col"
            ),
            {"table": table, "col": column},
        ).first()
    )


def upgrade() -> None:
    conn = op.get_bind()

    refs = [
        ("bookings", "master_id", "master_id_new"),
        ("master_services", "master_telegram_id", "master_id_new"),
        ("master_profiles", "master_telegram_id", "master_id_new"),
        ("master_client_notes", "master_telegram_id", "master_id_new"),
    ]

    # For each table: verify backfill completed; if not, write audit and abort.
    for table, old_col, new_col in refs:
        # If the new_col does not exist, skip (nothing to finalize here)
        if not _col_exists(conn, table, new_col):
            continue

        # Determine condition: rows that would be left without master_id after rename
        # For bookings old_col is the old master_id (telegram), for others it's master_telegram_id
        missing_sql = f"SELECT COUNT(*) FROM {table} WHERE ({old_col} IS NOT NULL) AND {new_col} IS NULL"
        missing = conn.execute(sa.text(missing_sql)).scalar() or 0

        if missing and missing > 0:
            # Create audit table for this table if not exists and populate sample rows
            audit_table = f"{table}_master_id_finalize_audit"
            conn.execute(
                sa.text(
                    f"CREATE TABLE IF NOT EXISTS {audit_table} (row_id bigint, {old_col} text, {new_col} bigint, issue text, row jsonb, inserted_at timestamptz DEFAULT now());"
                )
            )

            conn.execute(
                sa.text(
                    f"INSERT INTO {audit_table} (row_id, {old_col}, {new_col}, issue, row) "
                    f"SELECT id, {old_col}::text, {new_col}, 'missing_master_id_new', to_jsonb(t) FROM (SELECT * FROM {table} WHERE ({old_col} IS NOT NULL) AND {new_col} IS NULL LIMIT 100) t;"
                )
            )

            raise RuntimeError(
                f"Backfill incomplete for table {table}: {missing} rows have {old_col} but no {new_col}. "
                f"Audit written to {audit_table}. Resolve these rows before re-running this migration."
            )

    # If we reach here, all backfills are complete (or missing new_col entirely)
    # Proceed to perform safe renames and constraints.
    for table, old_col, new_col in refs:
        if not _col_exists(conn, table, new_col):
            continue

        # If a column named `master_id` already exists (legacy name), rename it to *_old
        if _col_exists(conn, table, "master_id"):
            conn.execute(
                sa.text(f"ALTER TABLE {table} RENAME COLUMN master_id TO master_id_old;")
            )

        # Rename new_col -> master_id
        conn.execute(sa.text(f"ALTER TABLE {table} RENAME COLUMN {new_col} TO master_id;"))

        # Make not null
        conn.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN master_id SET NOT NULL;"))

        # Add an index for performance (if missing)
        conn.execute(sa.text(f"CREATE INDEX IF NOT EXISTS ix_{table}_master_id ON {table} (master_id);"))

        # Add FK constraint if not exists
        conn.execute(
            sa.text(
                f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_{table}_master_id_masters_id') THEN "
                f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_master_id_masters_id FOREIGN KEY (master_id) REFERENCES masters(id) ON DELETE CASCADE; END IF; END $$;"
            )
        )

        # Drop legacy columns if they exist (master_id_old and the old telegram id column)
        # We drop master_id_old only if it exists and is fully redundant.
        if _col_exists(conn, table, "master_id_old"):
            conn.execute(sa.text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS master_id_old;"))

        if _col_exists(conn, table, old_col):
            # old_col for bookings was numeric master_id (telegram), for others it's master_telegram_id
            conn.execute(sa.text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {old_col};"))

    # Done


def downgrade() -> None:
    # Downgrade is intentionally a no-op / best-effort because reversing destructive
    # column drops reliably is not always possible. We attempt to recreate the
    # master_id_new columns and drop the FK added, but this is best-effort.
    conn = op.get_bind()
    refs = [
        ("bookings", "master_id", "master_id_new"),
        ("master_services", "master_telegram_id", "master_id_new"),
        ("master_profiles", "master_telegram_id", "master_id_new"),
        ("master_client_notes", "master_telegram_id", "master_id_new"),
    ]

    for table, old_col, new_col in refs:
        # Recreate new_col if missing
        try:
            if not _col_exists(conn, table, new_col):
                conn.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {new_col} BIGINT;"))

            # Drop FK we added
            conn.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS fk_{table}_master_id_masters_id;"))
            conn.execute(sa.text(f"DROP INDEX IF EXISTS ix_{table}_master_id;"))
        except Exception:
            pass
