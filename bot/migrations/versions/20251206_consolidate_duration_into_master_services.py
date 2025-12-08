"""Consolidate `duration_minutes` into `master_services` (backfill + audit)

Revision ID: 20251206_consolidate_duration_into_master_services
Revises: 20251206_add_master_services_duration_minutes
Create Date: 2025-12-05 12:00:00

This guarded migration backfills `master_services.duration_minutes` from
`service_profiles.duration_minutes` where available, then from
`services.duration_minutes` as a fallback. It writes audit rows for any
master_services rows that remain NULL afterwards and records potential
conflicts where multiple sources disagree.

The migration is idempotent and safe to run multiple times.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251206_consolidate_duration_into_master_services"
down_revision = "20251206_add_master_services_duration_minutes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # Ensure the column exists (idempotent)
    conn.execute(sa.text("ALTER TABLE master_services ADD COLUMN IF NOT EXISTS duration_minutes INTEGER;"))

    # 1) Backfill from service_profiles when available
    conn.execute(sa.text(
        """
        UPDATE master_services ms
        SET duration_minutes = sp.duration_minutes
        FROM service_profiles sp
        WHERE ms.service_id = sp.service_id
          AND ms.duration_minutes IS NULL
          AND sp.duration_minutes IS NOT NULL;
        """
    ))

    # 2) Backfill remaining NULLs from services
    conn.execute(sa.text(
        """
        UPDATE master_services ms
        SET duration_minutes = s.duration_minutes
        FROM services s
        WHERE ms.service_id = s.id
          AND ms.duration_minutes IS NULL
          AND s.duration_minutes IS NOT NULL;
        """
    ))

    # 3) Create an audit table to record any remaining NULLs / conflicts
    conn.execute(sa.text(
        """
        CREATE TABLE IF NOT EXISTS master_services_duration_audit (
            master_service_id text,
            master_id bigint,
            service_id text,
            ms_duration integer,
            sp_duration integer,
            svc_duration integer,
            issue text,
            inserted_at timestamptz DEFAULT now()
        );
        """
    ))

    # 4) Insert rows for master_services that are still NULL after backfill
    conn.execute(sa.text(
        """
        INSERT INTO master_services_duration_audit(master_service_id, master_id, service_id, ms_duration, sp_duration, svc_duration, issue)
        SELECT (ms.master_id::text || ':' || ms.service_id::text) as ms_key, ms.master_id, ms.service_id::text,
               ms.duration_minutes,
               (SELECT sp.duration_minutes FROM service_profiles sp WHERE sp.service_id = ms.service_id LIMIT 1),
               (SELECT s.duration_minutes FROM services s WHERE s.id = ms.service_id LIMIT 1),
               'missing_after_backfill'
        FROM master_services ms
        WHERE ms.duration_minutes IS NULL
        AND NOT EXISTS (
            SELECT 1 FROM master_services_duration_audit a WHERE a.master_service_id = (ms.master_id::text || ':' || ms.service_id::text) AND a.issue = 'missing_after_backfill'
        );
        """
    ))

    # 5) Insert rows for conflicts where ms has a value but differs from profile/service
    conn.execute(sa.text(
        """
         INSERT INTO master_services_duration_audit(master_service_id, master_id, service_id, ms_duration, sp_duration, svc_duration, issue)
         SELECT (ms.master_id::text || ':' || ms.service_id::text) as ms_key, ms.master_id, ms.service_id::text,
             ms.duration_minutes,
             sp.duration_minutes,
             s.duration_minutes,
             'conflict'
         FROM master_services ms
        LEFT JOIN service_profiles sp ON sp.service_id = ms.service_id
        LEFT JOIN services s ON s.id = ms.service_id
        WHERE ms.duration_minutes IS NOT NULL
          AND (
            (sp.duration_minutes IS NOT NULL AND sp.duration_minutes IS DISTINCT FROM ms.duration_minutes)
            OR (s.duration_minutes IS NOT NULL AND s.duration_minutes IS DISTINCT FROM ms.duration_minutes)
          )
        AND NOT EXISTS (
            SELECT 1 FROM master_services_duration_audit a WHERE a.master_service_id = (ms.master_id::text || ':' || ms.service_id::text) AND a.issue = 'conflict'
        );
        """
    ))


def downgrade() -> None:
    # Conservative downgrade: drop audit table only. Do not remove data.
    conn = op.get_bind()
    conn.execute(sa.text("DROP TABLE IF EXISTS master_services_duration_audit;"))
