from datetime import datetime, time as _time, date as _date
from enum import Enum as _Enum
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    BigInteger,
    Time,
    Date,
    Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy declarative base class (explicit for mypy)."""

    pass


class BookingStatus(_Enum):  # Use lowercase values to match normalized DB enum labels
    RESERVED = "reserved"
    PENDING_PAYMENT = "pending_payment"
    CONFIRMED = "confirmed"
    PAID = "paid"
    CANCELLED = "cancelled"
    DONE = "done"
    NO_SHOW = "no_show"
    EXPIRED = "expired"


def normalize_booking_status(value: str | BookingStatus | None) -> BookingStatus | None:
    """Return a BookingStatus enum when possible (accepts strings/enum values).

    Accepts mixed-case and legacy variants (e.g. 'CONFIRMED', 'confirmed', 'no-show').
    """
    if isinstance(value, BookingStatus):
        return value
    if isinstance(value, str):
        v = value.strip()
        # Try exact match against member value
        for m in BookingStatus:
            if m.value == v:
                return m
        # Try case-insensitive match against name or value
        lv = v.lower()
        for m in BookingStatus:
            if m.name.lower() == lv or m.value.lower() == lv:
                return m
        # Try common legacy variants (dashes/underscores)
        normalized = lv.replace("-", "_")
        for m in BookingStatus:
            if m.value.lower() == normalized:
                return m
        return None
    return None


TERMINAL_STATUSES = frozenset(
    {
        BookingStatus.CANCELLED,
        BookingStatus.DONE,
        BookingStatus.NO_SHOW,
        BookingStatus.EXPIRED,
    }
)

ACTIVE_STATUSES = frozenset(
    {
        BookingStatus.RESERVED,
        BookingStatus.PENDING_PAYMENT,
        BookingStatus.CONFIRMED,
        BookingStatus.PAID,
    }
)

REVENUE_STATUSES = frozenset(
    {
        BookingStatus.PAID,
        BookingStatus.CONFIRMED,
        BookingStatus.DONE,
    }
)


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    # Optional Telegram username (without @). Nullable for backward compatibility.
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    locale: Mapped[str | None] = mapped_column(String(8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
    )
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)  # ✅ добавь это


class Master(Base):
    __tablename__ = "masters"
    # Surrogate primary key (added by migration). Kept as primary key in the model.
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # Legacy Telegram identifier (kept for compatibility). Marked unique/indexed.
    telegram_id: Mapped[int | None] = mapped_column(
        BigInteger, unique=True, index=True, nullable=True
    )
    name: Mapped[str] = mapped_column(String(120))
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # Optional JSON/text blob with profile info (about, durations, etc.)
    bio: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Time when master record was created (for analytics/history). Nullable for backward compatibility.
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
        nullable=True,
    )
    # Soft-delete flag: prefer setting this to False instead of physically deleting
    # rows so historical references (e.g. bookings) remain intact.
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class Service(Base):
    __tablename__ = "services"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    # Optional category for grouping services in admin/analytics (nullable for backward compatibility)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # Description migrated into services table in DB: keep on the model for direct access
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Duration in minutes stored on service row in DB (nullable for backward-compatibility)
    duration_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class MasterService(Base):
    __tablename__ = "master_services"
    # Junction table: now uses (master_id, service_id) where master_id references masters.id
    master_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("masters.id", ondelete="CASCADE"), primary_key=True
    )
    # NOTE: legacy `master_telegram_id` SQL expression removed for performance.
    # Use `master_id` (surrogate PK referencing `masters.id`) for joins and filters.
    service_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("services.id", ondelete="CASCADE"), primary_key=True
    )
    # Optional индивидуальная длительность услуги для конкретного мастера (в минутах).
    # Nullable для обратной совместимости: при NULL используется Service.duration_minutes
    # или глобальная длительность слота из настроек.
    duration_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    # Reference to `masters.id` (surrogate primary key).
    master_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("masters.id"))
    # Booking composition is canonicalized in `booking_items`.
    # Persist enum values (lowercase strings) into the existing Postgres enum type
    # named "booking_status" created by the initial migration. This avoids
    # generating a separate "bookingstatus" type and ensures values like
    # "reserved"/"confirmed" are stored instead of Enum names like "RESERVED".
    status: Mapped[BookingStatus] = mapped_column(
        Enum(
            BookingStatus,
            name="booking_status_normalized",  # use normalized Postgres enum
            values_callable=lambda e: [m.value for m in e],
            native_enum=True,
        ),
        default=BookingStatus.RESERVED,
    )
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # End timestamp for the booking interval. Added to support range-based
    # exclusion constraints and to match DB migrations that populate
    # `ends_at` from booking_items / service_profiles. Nullable for
    # backward-compatibility during migrations.
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
    )
    original_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # final (possibly discounted) price snapshot
    final_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cash_hold_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    payment_provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    payment_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Store applied discount identifier (nullable). Matches DB `discount_applied` varchar(64)
    discount_applied: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # reminder / notification flags (used by scheduler tests)
    remind_24h_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    remind_1h_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    # Flexible reminder tracking: timestamp of last reminder and lead (minutes)
    last_reminder_sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_reminder_lead_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)


class Setting(Base):
    __tablename__ = "settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(120), unique=True)
    value: Mapped[str] = mapped_column(String(400))
    value_json: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSONB, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
        nullable=False,
    )


# master_profiles table removed; bio now belongs to masters


class BookingRating(Base):
    __tablename__ = "booking_ratings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id"))
    rating: Mapped[int] = mapped_column(Integer)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)


class BookingItem(Base):
    __tablename__ = "booking_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id", ondelete="CASCADE"))
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id", ondelete="CASCADE"))
    position: Mapped[int] = mapped_column(Integer, default=0)
    # Snapshot of service price at the time of booking (in cents). Nullable
    # initially to allow a conservative rollout and backfill.
    price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)


class BookingStatusHistory(Base):
    __tablename__ = "booking_status_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    booking_id: Mapped[int] = mapped_column(ForeignKey("bookings.id", ondelete="CASCADE"))
    old_status: Mapped[BookingStatus | None] = mapped_column(
        Enum(
            BookingStatus,
            name="booking_status_normalized",
            values_callable=lambda e: [m.value for m in e],
            native_enum=True,
        ),
        nullable=True,
    )
    new_status: Mapped[BookingStatus] = mapped_column(
        Enum(
            BookingStatus,
            name="booking_status_normalized",
            values_callable=lambda e: [m.value for m in e],
            native_enum=True,
        ),
    )
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
    )


class MasterClientNote(Base):
    __tablename__ = "master_client_notes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("masters.id", ondelete="CASCADE")
    )
    # Legacy `master_telegram_id` expression removed; use `master_id` instead.
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    note: Mapped[str] = mapped_column(Text)


class MasterSchedule(Base):
    __tablename__ = "master_schedules"
    __table_args__ = (
        Index("ix_master_schedules_master_id_day_of_week", "master_id", "day_of_week"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("masters.id", ondelete="CASCADE")
    )
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    start_time: Mapped[_time] = mapped_column(Time, nullable=False)
    end_time: Mapped[_time] = mapped_column(Time, nullable=False)
    is_day_off: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
        nullable=False,
    )


class MasterScheduleException(Base):
    __tablename__ = "master_schedule_exceptions"
    __table_args__ = (
        Index("ix_master_schedule_exceptions_master_id_date", "master_id", "exception_date"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("masters.id", ondelete="CASCADE")
    )
    exception_date: Mapped[_date] = mapped_column(Date, nullable=False)
    start_time: Mapped[_time] = mapped_column(Time, nullable=False)
    end_time: Mapped[_time] = mapped_column(Time, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: __import__(
            "bot.app.services.shared_services", fromlist=["utc_now"]
        ).utc_now(),
        nullable=False,
    )


__all__ = [
    "Base",
    "User",
    "Master",
    "Service",
    "MasterService",
    "BookingStatus",
    "Booking",
    "Setting",
    "BookingRating",
    "BookingItem",
    "BookingStatusHistory",
    "MasterClientNote",
    "MasterSchedule",
    "MasterScheduleException",
    "normalize_booking_status",
    "TERMINAL_STATUSES",
    "ACTIVE_STATUSES",
    "REVENUE_STATUSES",
]
