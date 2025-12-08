"""Backfill `settings` key/value table from single-row `app_settings` JSONB.

This guarded migration copies keys from `app_settings.data` into the
`settings` EAV table when the key is missing. Conflicting keys where a
different value already exists are recorded in `app_settings_backfill_audit`
for operator review.
"""
from alembic import op
import sqlalchemy as sa
import json


# revision identifiers, used by Alembic.
revision = "20251207_backfill_settings_from_appsettings"
down_revision = "20251204_migrate_settings_to_jsonb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # audit table
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS app_settings_backfill_audit (key text, existing_value jsonb, issue text, inserted_at timestamptz DEFAULT now())"
        )
    )

    exists = conn.execute(
        sa.text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='app_settings')")
    ).scalar()
    if not exists:
        # Nothing to do
        return

    row = conn.execute(sa.text("SELECT data FROM app_settings WHERE id = 1 LIMIT 1")).first()
    if not row:
        return

    data = row[0]
    if not isinstance(data, dict):
        # Unexpected shape; record and abort for manual inspection.
        conn.execute(
            sa.text("INSERT INTO app_settings_backfill_audit (key, existing_value, issue) VALUES (:k, :v::jsonb, 'invalid_shape')"),
            {"k": "__app_settings_root__", "v": json.dumps(data, ensure_ascii=False)},
        )
        return

    for k, v in data.items():
        try:
            found = conn.execute(sa.text("SELECT value FROM settings WHERE key = :k"), {"k": k}).scalar()
            if found is not None:
                # Compare stringified forms; if different, audit
                candidate = v if isinstance(v, (str, int, float, bool)) else json.dumps(v, ensure_ascii=False)
                if str(found) != str(candidate):
                    conn.execute(
                        sa.text(
                            "INSERT INTO app_settings_backfill_audit (key, existing_value, issue) VALUES (:k, :v::jsonb, 'conflict')"
                        ),
                        {"k": k, "v": json.dumps(v, ensure_ascii=False)},
                    )
                continue

            # Insert missing key into settings table
            val = v if isinstance(v, (str, int, float, bool)) else json.dumps(v, ensure_ascii=False)
            conn.execute(
                sa.text("INSERT INTO settings (key, value, updated_at) VALUES (:k, :val, now())"),
                {"k": k, "val": str(val)},
            )
        except Exception:
            # Any per-key error should be audited and not abort the whole migration
            try:
                conn.execute(
                    sa.text(
                        "INSERT INTO app_settings_backfill_audit (key, existing_value, issue) VALUES (:k, :v::jsonb, 'error')"
                    ),
                    {"k": k, "v": json.dumps(v, ensure_ascii=False)},
                )
            except Exception:
                pass


def downgrade() -> None:
    # No-op: we don't remove inserted keys on downgrade.
    pass
