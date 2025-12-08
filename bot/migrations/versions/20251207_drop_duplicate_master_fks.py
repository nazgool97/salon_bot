"""Drop duplicate foreign-key constraints referencing `masters(id)`.

This guarded migration inspects a short list of master-related tables and
detects duplicate FK constraints that reference `masters(id)` for the
`master_id` column. For safety it:

- Writes an audit table `<table>_duplicate_master_fk_audit` with the
  constraint names and definitions.
- If a canonical constraint named `fk_{table}_master_id_masters_id` exists,
  drops all other FK constraints that reference `masters(id)` and include
  the `master_id` column in their definition.
- If the canonical constraint is missing, the migration aborts and
  requires operator review (audit is left for inspection).

This migration is idempotent and safe to run repeatedly.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251207_drop_duplicate_master_fks"
down_revision = "20251204_finalize_masters_id_migration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    tables = [
        "master_client_notes",
        "master_profiles",
        "master_services",
    ]

    for table in tables:
        # Each loop runs a single DO block that is safe to execute in one
        # prepared statement. The block will create an audit table, insert
        # rows describing duplicate constraints, and then drop redundant
        # constraints only when the canonical one exists.
        canonical = f"fk_{table}_master_id_masters_id"

        do_sql = f"""
DO $$
DECLARE
    r record;
    cnt int;
    canonical text := '{canonical}';
BEGIN
    -- If the table doesn't exist, nothing to do.
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = '{table}') THEN
        RETURN;
    END IF;

    -- Count FK constraints on this table referencing masters(id) that mention master_id
    SELECT COUNT(*) INTO cnt
    FROM pg_constraint c
    WHERE c.conrelid = '{table}'::regclass
      AND c.confrelid = 'masters'::regclass
      AND pg_get_constraintdef(c.oid) LIKE '%(master_id)%';

    IF cnt <= 1 THEN
        RETURN; -- nothing to do
    END IF;

    -- Create an audit table and capture all matching constraints
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I_duplicate_master_fk_audit (conname text, condef text, inserted_at timestamptz DEFAULT now())', '{table}');

    FOR r IN
        SELECT conname, pg_get_constraintdef(oid) as condef
        FROM pg_constraint c
        WHERE c.conrelid = '{table}'::regclass
          AND c.confrelid = 'masters'::regclass
          AND pg_get_constraintdef(c.oid) LIKE '%(master_id)%'
    LOOP
        EXECUTE format('INSERT INTO %I_duplicate_master_fk_audit (conname, condef) VALUES (%L, %L)', '{table}', r.conname, r.condef);
    END LOOP;

    -- If the canonical constraint exists, drop all others; otherwise abort
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = canonical AND conrelid = '{table}'::regclass) THEN
        FOR r IN
            SELECT conname FROM pg_constraint c
            WHERE c.conrelid = '{table}'::regclass
              AND c.confrelid = 'masters'::regclass
              AND pg_get_constraintdef(c.oid) LIKE '%(master_id)%'
              AND conname != canonical
        LOOP
            EXECUTE format('ALTER TABLE {table} DROP CONSTRAINT IF EXISTS %I', r.conname);
        END LOOP;
    ELSE
        RAISE EXCEPTION 'Canonical constraint % not found on table {table}; manual review required. Audit table % created', canonical, '{table}_duplicate_master_fk_audit';
    END IF;
END
$$;
"""

        conn.execute(sa.text(do_sql))


def downgrade() -> None:
    # Downgrade is a no-op: we do not attempt to restore dropped FKs.
    pass
