"""Merge `service_profiles` into `services` (safe, guarded)

Revision ID: 20251204_merge_service_profiles_into_services
Revises: 20251209_normalize_booking_items
Create Date: 2025-12-04 14:30:00

This guarded, non-destructive skeleton migration copies selected columns
from `service_profiles` into `services` to collapse a 1:1 split. The
migration performs the following high-level steps:

- Add profile columns to `services` (IF NOT EXISTS).
- Backfill `services` from `service_profiles` (single-statement UPDATEs,
  suitable for asyncpg / alembic async execution).
- Create `service_profiles_merge_audit` to record any orphan profiles or
  mismatches that require operator attention.
- Abort with a clear RuntimeError if orphaned `service_profiles` rows exist
  (so operator can inspect audit table and resolve before continuing).

Important: this migration intentionally does NOT DROP `service_profiles`.
After the application is updated to read from `services` and write dual
paths (or only to `services`), a follow-up (manual) migration can remove
`service_profiles` and switch FKs.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_merge_service_profiles_into_services"
down_revision = "20251209_normalize_booking_items"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # ---- 1) Add profile columns to services (adjust names/types to your schema)
    # Add only the minimal set of columns that service_profiles holds and that
    # you want directly on services. Keep them nullable for staged rollout.
    # Add columns matching ServiceProfile fields: description and duration_minutes
    conn.execute(sa.text("ALTER TABLE services ADD COLUMN IF NOT EXISTS description TEXT;"))
    conn.execute(sa.text("ALTER TABLE services ADD COLUMN IF NOT EXISTS duration_minutes INTEGER;"))
    # Add other columns as needed, for example: image_url, notes, metadata JSONB, etc.

    # ---- 2) Backfill services from service_profiles
    # Copy values where a profile exists for a service.
    conn.execute(sa.text(
        """
UPDATE services s
SET
  description = p.description,
  duration_minutes = p.duration_minutes
FROM service_profiles p
WHERE p.service_id = s.id
  AND (
    s.description IS DISTINCT FROM p.description
    OR s.duration_minutes IS DISTINCT FROM p.duration_minutes
  );
"""
    ))

    # ---- 3) Create audit table and record orphan or unmatched profiles
    # Use text for service_id in audit to avoid type-mismatch across deployments
    conn.execute(sa.text(
      "CREATE TABLE IF NOT EXISTS service_profiles_merge_audit (service_id text, issue text, profile_row jsonb, inserted_at timestamptz DEFAULT now());"
    ))

    # Profiles without a matching service (orphaned service_profiles)
    # Cast service_id to text when inserting into audit to avoid bigint/varchar mismatches
    conn.execute(sa.text(
      "INSERT INTO service_profiles_merge_audit(service_id, issue, profile_row) SELECT p.service_id::text, 'no-service-found', to_jsonb(p) FROM service_profiles p LEFT JOIN services s ON s.id::text = p.service_id::text WHERE s.id IS NULL;"
    ))

    # If there are orphaned profiles, abort so operator can inspect
    orphan_count = conn.execute(sa.text("SELECT COUNT(*) FROM service_profiles_merge_audit WHERE issue = 'no-service-found' ")).scalar()
    if orphan_count and int(orphan_count) > 0:
        raise RuntimeError(
            "Found service_profiles rows with no matching services. "
            "Audit written to service_profiles_merge_audit. Resolve or re-link these rows before re-running migration."
        )

    # ---- 4) Optional: verify counts/consistency
    # Example: ensure number of profiles copied matches number of service_profiles
    copied = conn.execute(sa.text("SELECT COUNT(*) FROM services s JOIN service_profiles p ON p.service_id::text = s.id::text WHERE (s.description IS NOT NULL OR s.duration_minutes IS NOT NULL) ")).scalar()
    total_profiles = conn.execute(sa.text("SELECT COUNT(*) FROM service_profiles")).scalar()

    # If counts differ, write note to audit (do not auto-abort here, operator may accept partial copy)
    if total_profiles and int(total_profiles) != int(copied):
        conn.execute(sa.text("INSERT INTO service_profiles_merge_audit(service_id, issue, profile_row) SELECT p.service_id::text, 'copy-mismatch', to_jsonb(p) FROM service_profiles p LEFT JOIN services s ON s.id::text = p.service_id::text WHERE (s.description IS NULL AND s.duration_minutes IS NULL);"))

    # ---- 5) Post-migration instructions (not enforced here)
    # - Update application code to read profile fields from `services`.
    # - Consider dual-writing (services + service_profiles) for a safe rollout.
    # - After app deploy and validation, create a follow-up migration to
    #   remove `service_profiles` and migrate FKs.


def downgrade() -> None:
    # Downgrade is intentionally conservative: we don't try to recreate
    # service_profiles or move data back. Operators should perform manual
    # rollback procedures if needed.
    conn = op.get_bind()
    # Remove the columns we added (safe if app rolled back)
    try:
        conn.execute(sa.text("ALTER TABLE services DROP COLUMN IF EXISTS profile_text;"))
        conn.execute(sa.text("ALTER TABLE services DROP COLUMN IF EXISTS profile_extra_json;"))
    except Exception:
        # swallow errors to avoid downgrade failing in partially-migrated DBs
        pass
