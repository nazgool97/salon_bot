"""Drop legacy booking enum types and helper casts/functions when safe

Revision ID: 20251205_drop_legacy_booking_enums
Revises: 20251204_unify_booking_status_enum
Create Date: 2025-12-05 02:30:00

This guarded migration will attempt to remove the historical enum types
`booking_status` and `bookingstatus` and the helper cast/functions that
were created to ease conversions. It only drops these objects when it's
safe (no remaining table columns depend on them). If dependent columns
are found, the migration writes audit rows and aborts so you can
manually inspect and resolve them.

This migration is idempotent and conservative: it will never drop types
that are still in use.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_drop_legacy_booking_enums"
down_revision = "20251204_unify_booking_status_enum"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # We'll drop legacy types only when there are no dependent columns.
    # If any dependency remains, write an audit table and abort.
    for legacy in ("booking_status", "bookingstatus"):
        # Check if type exists
        exists = conn.execute(sa.text("SELECT 1 FROM pg_type WHERE typname = :t"), {"t": legacy}).scalar()
        if not exists:
            continue

        # Find any table/column still using this type
        deps = conn.execute(
            sa.text(
                """
                SELECT n.nspname AS schema, c.relname AS table, a.attname AS column
                FROM pg_type ty
                JOIN pg_catalog.pg_namespace n ON n.oid = ty.typnamespace
                JOIN pg_catalog.pg_attribute a ON a.atttypid = ty.oid
                JOIN pg_class c ON c.oid = a.attrelid
                WHERE ty.typname = :t AND a.attnum > 0 AND c.relkind IN ('r','p')
                """
            ),
            {"t": legacy},
        ).fetchall()

        if deps and len(deps) > 0:
            # Write audit table and abort so operator can inspect
            for schema, table, col in deps:
                conn.execute(
                    sa.text(
                        """
                        CREATE TABLE IF NOT EXISTS legacy_booking_status_type_audit (
                            typname text,
                            schema_name text,
                            table_name text,
                            column_name text,
                            inspected_at timestamptz DEFAULT now()
                        );

                        INSERT INTO legacy_booking_status_type_audit(typname, schema_name, table_name, column_name)
                        VALUES (:t, :schema, :table, :col);
                        """
                    ),
                    {"t": legacy, "schema": schema, "table": table, "col": col},
                )
            raise RuntimeError(
                f"Legacy enum type '{legacy}' is still used by some table columns. Audit written to legacy_booking_status_type_audit; inspect and migrate those columns before re-running this migration."
            )

        # No dependent columns — safe to drop helper casts/functions and the type.
        # Drop known cast entries if they exist, then functions, then type.
        try:
            conn.execute(sa.text("DROP CAST IF EXISTS (booking_status AS bookingstatus)"))
        except Exception:
            pass
        try:
            conn.execute(sa.text("DROP CAST IF EXISTS (bookingstatus AS booking_status)"))
        except Exception:
            pass

        # Drop the helper functions if present
        try:
            conn.execute(sa.text("DROP FUNCTION IF EXISTS booking_status_to_bookingstatus(booking_status)"))
        except Exception:
            pass
        try:
            conn.execute(sa.text("DROP FUNCTION IF EXISTS bookingstatus_to_booking_status(bookingstatus)"))
        except Exception:
            pass

        # Finally drop the legacy type
        try:
            conn.execute(sa.text(f"DROP TYPE IF EXISTS {legacy}"))
        except Exception:
            # If DROP TYPE fails for unexpected reasons, raise to avoid silent divergence
            raise


def downgrade() -> None:
    # Do not attempt to recreate the historical types — manual restore required.
    pass
