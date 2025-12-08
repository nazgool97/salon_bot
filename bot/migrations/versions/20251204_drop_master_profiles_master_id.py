"""Drop legacy master_profiles.master_id column (guarded)

Revision ID: 20251204_drop_master_profiles_master_id
Revises: 20251204_service_profiles_id_and_master_id_nullable
Create Date: 2025-12-04 12:30:00

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_drop_master_profiles_master_id"
down_revision = "20251204_service_profiles_id_and_master_id_nullable"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # 1) If column is absent, nothing to do
    col_exists = conn.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='master_profiles' AND column_name='master_id')"
        )
    ).scalar()
    if not col_exists:
        return

    # 2) If there are any non-null master_id values, preserve them into an audit
    # table and abort so operator can inspect before destructive change.
    nonnull = conn.execute(sa.text("SELECT COUNT(*) FROM master_profiles WHERE master_id IS NOT NULL")).scalar()
    if nonnull and int(nonnull) > 0:
        conn.execute(
            sa.text(
                """
                CREATE TABLE IF NOT EXISTS master_profiles_master_id_audit (
                    master_profile_pk bigint,
                    master_id bigint,
                    master_telegram_id bigint,
                    migrated_at timestamptz DEFAULT now()
                );

                INSERT INTO master_profiles_master_id_audit(master_profile_pk, master_id, master_telegram_id)
                SELECT id, master_id, master_telegram_id FROM master_profiles WHERE master_id IS NOT NULL;
                """
            )
        )
        raise RuntimeError(
            "Refusing to drop master_profiles.master_id: non-null values found; audit created as master_profiles_master_id_audit"
        )

    # 3) Ensure there are no constraints referencing master_profiles.master_id
    fk_count = conn.execute(
        sa.text(
            "SELECT COUNT(*) FROM information_schema.constraint_column_usage WHERE table_name='master_profiles' AND column_name='master_id'"
        )
    ).scalar()
    if fk_count and int(fk_count) > 0:
        raise RuntimeError(
            "Refusing to drop master_profiles.master_id: foreign-key constraints reference the column. Inspect and remove constraints first."
        )

    # 4) Drop the column and its sequence if present
    conn.execute(sa.text("ALTER TABLE master_profiles DROP COLUMN IF EXISTS master_id"))
    conn.execute(sa.text("DROP SEQUENCE IF EXISTS master_profiles_master_id_seq"))


def downgrade() -> None:
    # Downgrade is intentionally a no-op: restoring the exact previous
    # state (including sequence values) requires operator action and
    # inspection of audit data if present.
    pass
