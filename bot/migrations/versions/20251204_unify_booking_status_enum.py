"""Unify booking status enum types and normalize labels to lowercase (safe)

Revision ID: 20251204_unify_booking_status_enum
Revises: 20251204_migrate_settings_to_jsonb
Create Date: 2025-12-04 13:10:00

This guarded migration normalizes booking status values into a new
lowercase enum type without dropping any legacy types or helper objects.

It will:
 - create `booking_status_normalized` if missing
 - add `status_txt` and populate normalized values
 - audit and abort if unknown values are present
 - convert `bookings.status` to the new enum type

It intentionally does NOT drop legacy enum types, casts or helper
functions; that cleanup must be performed separately after verification.
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251204_unify_booking_status_enum"
down_revision = "20251204_migrate_settings_to_jsonb"
branch_labels = None
depends_on = None


def upgrade() -> None:
	conn = op.get_bind()

	# 1) Create normalized enum type if not exists
	conn.execute(
		sa.text(
			"""
			DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'booking_status_normalized') THEN
					CREATE TYPE booking_status_normalized AS ENUM (
						'reserved', 'pending_payment', 'confirmed', 'awaiting_cash',
						'paid', 'active', 'cancelled', 'done', 'no_show', 'expired'
					);
				END IF;
			END
			$$;
			"""
		)
	)

	# 2) Add temporary text column to hold normalized values
	try:
		op.add_column('bookings', sa.Column('status_txt', sa.Text(), nullable=True))
	except Exception:
		# column probably exists from a prior run
		pass

	# 3) Populate status_txt by normalizing existing status text
	conn.execute(
		sa.text(
			"""
			UPDATE bookings
			SET status_txt = CASE lower(status::text)
				WHEN 'reserved' THEN 'reserved'
				WHEN 'pending_payment' THEN 'pending_payment'
				WHEN 'pending-payment' THEN 'pending_payment'
				WHEN 'pendingpayment' THEN 'pending_payment'
				WHEN 'confirmed' THEN 'confirmed'
				WHEN 'awaiting_cash' THEN 'awaiting_cash'
				WHEN 'awaiting-cash' THEN 'awaiting_cash'
				WHEN 'awaitingcash' THEN 'awaiting_cash'
				WHEN 'paid' THEN 'paid'
				WHEN 'active' THEN 'active'
				WHEN 'cancelled' THEN 'cancelled'
				WHEN 'canceled' THEN 'cancelled'
				WHEN 'done' THEN 'done'
				WHEN 'no_show' THEN 'no_show'
				WHEN 'no-show' THEN 'no_show'
				WHEN 'noshow' THEN 'no_show'
				WHEN 'expired' THEN 'expired'
				ELSE NULL
			END
			WHERE status_txt IS NULL;
			"""
		)
	)

	# 4) If any rows have NULL status_txt now, preserve them into audit and abort
	unknown_count = conn.execute(sa.text("SELECT COUNT(*) FROM bookings WHERE status_txt IS NULL")).scalar()
	if unknown_count and int(unknown_count) > 0:
		conn.execute(
			sa.text(
				"""
				CREATE TABLE IF NOT EXISTS bookings_status_unknown_audit (
					booking_id bigint,
					raw_status text,
					inserted_at timestamptz DEFAULT now()
				);

				INSERT INTO bookings_status_unknown_audit(booking_id, raw_status)
				SELECT id, status::text FROM bookings WHERE status_txt IS NULL;
				"""
			)
		)
		raise RuntimeError(
			"Found unknown booking.status values; audit written to bookings_status_unknown_audit. Inspect before re-running migration."
		)

	# 5) Switch column type to new enum in a guarded way
	try:
		conn.execute(sa.text("ALTER TABLE bookings ALTER COLUMN status DROP DEFAULT"))
	except Exception:
		pass

	conn.execute(
		sa.text(
			"ALTER TABLE bookings ALTER COLUMN status TYPE booking_status_normalized USING status_txt::booking_status_normalized"
		)
	)

	conn.execute(sa.text("ALTER TABLE bookings ALTER COLUMN status SET DEFAULT 'reserved'::booking_status_normalized"))

	# 6) Remove temporary column
	conn.execute(sa.text("ALTER TABLE bookings DROP COLUMN IF EXISTS status_txt"))


def downgrade() -> None:
	# Downgrade intentionally not implemented; manual action required to revert.
	pass

