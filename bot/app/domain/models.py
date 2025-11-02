from datetime import UTC, datetime
from enum import Enum as _Enum

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, String, Text, BigInteger, Column
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """SQLAlchemy declarative base class (explicit for mypy)."""

    pass


class BookingStatus(_Enum):  # Values match DB labels (Postgres enum)
    RESERVED = "RESERVED"
    PENDING_PAYMENT = "PENDING_PAYMENT"
    CONFIRMED = "CONFIRMED"
    AWAITING_CASH = "AWAITING_CASH"  # legacy alias, kept for compatibility
    PAID = "PAID"
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    DONE = "DONE"
    NO_SHOW = "NO_SHOW"
    EXPIRED = "EXPIRED"


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(120))
    # Optional Telegram username (without @). Nullable for backward compatibility.
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locale: Mapped[str | None] = mapped_column(String(8), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)  # ✅ добавь это



class Master(Base):
    __tablename__ = "masters"
    # Existing DB uses telegram_id as the primary key
    telegram_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String(120))


class Service(Base):
    __tablename__ = "services"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    # Optional category for grouping services in admin/analytics (nullable for backward compatibility)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)


class MasterService(Base):
    __tablename__ = "master_services"
    # Junction table in existing DB uses (master_telegram_id, service_id) as composite PK
    master_telegram_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("masters.telegram_id"), primary_key=True
    )
    service_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("services.id"), primary_key=True
    )


class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    # Reference masters by telegram_id in the existing schema
    master_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("masters.telegram_id"))
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"))
    # Persist enum values (lowercase strings) into the existing Postgres enum type
    # named "booking_status" created by the initial migration. This avoids
    # generating a separate "bookingstatus" type and ensures values like
    # "reserved"/"confirmed" are stored instead of Enum names like "RESERVED".
    status: Mapped[BookingStatus] = mapped_column(
        Enum(
            BookingStatus,
            name="bookingstatus",  # bind to existing Postgres enum type
            values_callable=lambda e: [m.value for m in e],  # persist uppercase labels
            native_enum=True,
        ),
        default=BookingStatus.RESERVED,
    )
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    original_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # final (possibly discounted) price snapshot
    final_price_cents: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cash_hold_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    paid_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    payment_provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    payment_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # reminder / notification flags (used by scheduler tests)
    remind_24h_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    remind_1h_sent: Mapped[bool] = mapped_column(Boolean, default=False)


class Setting(Base):
    __tablename__ = "settings"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(120), unique=True)
    value: Mapped[str] = mapped_column(String(400))


class MasterProfile(Base):
    __tablename__ = "master_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_telegram_id: Mapped[int] = mapped_column(ForeignKey("masters.telegram_id"))
    bio: Mapped[str | None] = mapped_column(Text, nullable=True)


class ServiceProfile(Base):
    __tablename__ = "service_profiles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Present since migration 0007; map for duration calculations
    duration_minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)


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
    service_id: Mapped[str] = mapped_column(ForeignKey("services.id"))
    position: Mapped[int] = mapped_column(Integer, default=0)


class MasterClientNote(Base):
    __tablename__ = "master_client_notes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_telegram_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("masters.telegram_id"))
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    note: Mapped[str] = mapped_column(Text)


__all__ = [
    "Base",
    "User",
    "Master",
    "Service",
    "MasterService",
    "BookingStatus",
    "Booking",
    "Setting",
    "MasterProfile",
    "ServiceProfile",
    "BookingRating",
    "BookingItem",
    "MasterClientNote",
]
