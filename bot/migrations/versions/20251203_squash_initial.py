"""Squashed initial schema migration

Revision ID: 20251203_squash_initial
Revises: None
Create Date: 2025-12-03 16:55:00.000000

This migration acts as a single initial/squashed migration representing the
current database schema (tables, indexes, types, constraints). It expects a
SQL schema file at `bot/migrations/schemas/squashed_schema.sql` which should
be produced from a live database (see `scripts/generate_squashed_schema.sh`).

If you prefer, you can directly embed the CREATE statements into this
migration's `upgrade()` body instead of using the external SQL file.

IMPORTANT: Once this squashed migration is verified on a fresh DB, you can
remove historical migration files from `bot/migrations/versions/`.
"""
from alembic import op
import sqlalchemy as sa
import pathlib
import os

# revision identifiers, used by Alembic.
revision = '20251203_squash_initial'
down_revision = None
branch_labels = None
depends_on = None


def _sql_file_path() -> str:
    base = pathlib.Path(__file__).resolve().parent.parent
    return str(base / "schemas" / "squashed_schema.sql")


def upgrade():
    """Apply the squashed SQL schema.

    This will execute the SQL found in `bot/migrations/schemas/squashed_schema.sql`.
    The file should be produced with `pg_dump -s` (schema only) from a DB
    that has already had all historical migrations applied.
    """
    path = _sql_file_path()
    if not os.path.exists(path):
        raise RuntimeError(
            f"Squashed schema file not found: {path}.\n" \
            "Generate it with scripts/generate_squashed_schema.sh or embed SQL here."
        )

    conn = op.get_bind()
    sql = pathlib.Path(path).read_text(encoding="utf8")

    # Execute entire DDL in a single statement. SQL should be idempotent for
    # a fresh DB; for safety we don't attempt makeshift idempotency here.
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        conn.execute(sa.text(stmt))


def downgrade():
    # Downgrade of a squashed base is intentionally unsupported.
    raise NotImplementedError("Downgrade is not supported for squashed migration")
