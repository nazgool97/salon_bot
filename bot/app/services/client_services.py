from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, UTC
from typing import Any, Dict, Iterable, List, Sequence, TypedDict

from sqlalchemy import select, and_, func, or_, String

from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from bot.app.domain.models import (
    Booking,
    BookingStatus,
    Master,
    Service,
    User,
    BookingRating,
    MasterSchedule,
    BookingItem,
    normalize_booking_status,
    TERMINAL_STATUSES,
    ACTIVE_STATUSES,
)
from bot.app.core.db import get_session
from bot.app.core.constants import REQUIRE_ROW_LOCK_STRICT, BOT_TOKEN, TELEGRAM_PROVIDER_TOKEN
from bot.app.services import master_services
from bot.app.services.master_services import MasterRepo

from zoneinfo import ZoneInfo
from aiogram import Bot
from bot.app.services.shared_services import (
    BookingInfo,
    booking_info_from_mapping,
    format_money_cents,
    status_to_emoji,
    safe_get_locale,
    default_language,
    format_booking_list_item,
    format_booking_details_text,
    format_date,
    utc_now,
    local_now,
    get_local_tz,
    get_service_duration,
    ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT,
    get_admin_ids,
    normalize_error_code,
)
from bot.app.core.notifications import send_booking_notification
from bot.app.services.admin_services import SettingsRepo
from bot.app.services.admin_services import ServiceRepo
from bot.app.telegram.common.status import ACTIVE_BLOCKING_STATUSES


async def _finalize_booking_payment(
    booking_id: int,
    *,
    target_status: BookingStatus,
    set_paid_at: bool,
) -> tuple[bool, str | None]:
    """Shared payment finalization with slot revalidation.

    Returns (ok, error_code): booking_not_found | booking_not_active | slot_unavailable | None
    """
    async with get_session() as session:
        from sqlalchemy import select
        from bot.app.domain.models import BookingStatusHistory

        booking_stmt = select(Booking).where(Booking.id == booking_id).with_for_update()
        booking = (await session.execute(booking_stmt)).scalar_one_or_none()
        if not booking:
            return False, "booking_not_found"

        status = getattr(booking, "status", None)
        if status in TERMINAL_STATUSES or status == target_status or status == BookingStatus.PAID:
            return False, "booking_not_active"

        try:
            hold_minutes = await SettingsRepo.get_reservation_hold_minutes()
        except Exception:
            hold_minutes = 5
        try:
            fallback_slot = await SettingsRepo.get_slot_duration()
        except Exception:
            fallback_slot = 60

        now_utc = utc_now()
        interval = _get_booking_interval(booking, fallback_slot)
        if not interval:
            return False, "booking_not_found"
        start, end = interval

        # If the hold already expired, mark the booking as expired and abort.
        if not is_booking_slot_blocked(booking, now_utc, hold_minutes):
            try:
                booking.status = BookingStatus.EXPIRED
                booking.cash_hold_expires_at = None
                await session.commit()
            except Exception:
                await session.rollback()
            return False, "slot_unavailable"

        # Fetch overlapping active bookings for the same master and lock them.
        try:
            candidate_stmt = (
                select(Booking)
                .where(
                    Booking.master_id == getattr(booking, "master_id", None),
                    Booking.id != booking.id,
                    Booking.status.in_(tuple(ACTIVE_STATUSES)),
                    Booking.starts_at < end,
                    Booking.starts_at >= start - timedelta(hours=12),
                )
                .with_for_update()
            )
            candidates = (await session.execute(candidate_stmt)).scalars().all()
        except Exception:
            candidates = []

        for other in candidates:
            other_interval = _get_booking_interval(other, fallback_slot)
            if not other_interval:
                continue
            if not is_booking_slot_blocked(other, now_utc, hold_minutes):
                continue
            o_start, o_end = other_interval
            if start < o_end and end > o_start:
                try:
                    booking.status = BookingStatus.EXPIRED
                    booking.cash_hold_expires_at = None
                    await session.commit()
                except Exception:
                    await session.rollback()
                return False, "slot_unavailable"

        old = getattr(booking, "status", None)
        booking.status = target_status
        try:
            booking.cash_hold_expires_at = None
            if set_paid_at:
                booking.paid_at = utc_now()
        except Exception:
            pass
        try:
            hist = BookingStatusHistory(booking_id=booking.id, old_status=old, new_status=target_status)
            session.add(hist)
        except Exception:
            pass
        await session.commit()
        return True, None

logger = logging.getLogger(__name__)

# Online payment discount setting key (percent). Default value centralized
# in `shared_services.ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT` so there's a
# single source of truth for the fallback percent used across the codebase.
ONLINE_PAYMENT_DISCOUNT_SETTING = "online_payment_discount_percent"


@dataclass
class BookingConflictRow:
    booking_id: int
    starts_at: datetime | None
    status: Any | None
    master_id: int | None
    user_id: int | None
    user_name: str | None
    username: str | None

# ---------------- Formatting helpers (moved from client_keyboards) ----------------


# --- Client formatters ------------------------------------------------------
def format_client_booking_row(fields: dict[str, str]) -> str:
    """Format booking row for client-facing compact lists.

    Fields is a mapping with keys produced by `format_booking_list_item`.
    """
    status_label = str(fields.get("status_label") or "")
    st = str(fields.get("st") or "")
    dt = str(fields.get("dt") or "")
    master_name = str(fields.get("master_name") or "")
    service_name = str(fields.get("service_name") or "")
    price_txt = str(fields.get("price_txt") or "")
    # Keep status label at the front, then compact parts separated by bullets
    datetime_part = f"{dt} {st}".strip()
    service_part = f"{service_name[:24]} {price_txt}".strip()
    parts = [datetime_part, master_name[:20].strip(), service_part]
    parts = [p for p in parts if p]
    body = " • ".join(parts)
    return (f"{status_label} " + body).strip()



async def calculate_booking_permissions(obj: dict | Any, lock_r_minutes: int | None = None, lock_c_minutes: int | None = None, settings: Any | None = None) -> tuple[bool, bool]:
    """Calculate (can_cancel, can_reschedule) for a booking-like object.

    Args:
        obj: mapping or object with a `starts_at` and optional `status`.
        settings: optional object providing `get_client_reschedule_lock_minutes` and
            `get_client_cancel_lock_minutes` callables; falls back to `SettingsRepo`.
    Returns:
        (can_cancel, can_reschedule)
    """
    can_cancel = False
    can_reschedule = False
    try:
        starts_at_dt = obj.get("starts_at") if isinstance(obj, dict) else getattr(obj, "starts_at", None)
        if starts_at_dt:
            now_utc = utc_now()
            try:
                starts_utc = starts_at_dt.astimezone(UTC)
            except Exception:
                starts_utc = starts_at_dt
            delta_seconds = (starts_utc - now_utc).total_seconds()
            # resolve lock settings: explicit args take precedence, then provided
            # settings object, then SettingsRepo, then default 3 hours
            if lock_r_minutes is not None:
                lock_r = lock_r_minutes
            else:
                try:
                    if settings and hasattr(settings, "get_client_reschedule_lock_minutes"):
                        lock_r = settings.get_client_reschedule_lock_minutes()
                        if asyncio.iscoroutine(lock_r):
                            lock_r = await lock_r
                    else:
                        lock_r = await SettingsRepo.get_client_reschedule_lock_minutes()
                except Exception:
                    lock_r = 180

            if lock_c_minutes is not None:
                lock_c = lock_c_minutes
            else:
                try:
                    if settings and hasattr(settings, "get_client_cancel_lock_minutes"):
                        lock_c = settings.get_client_cancel_lock_minutes()
                        if asyncio.iscoroutine(lock_c):
                            lock_c = await lock_c
                    else:
                        lock_c = await SettingsRepo.get_client_cancel_lock_minutes()
                except Exception:
                    lock_c = 60
            can_reschedule = delta_seconds >= (lock_r * 60)
            can_cancel = delta_seconds >= (lock_c * 60)
    except Exception:
        can_cancel = False
        can_reschedule = False

    # suppress permissions for terminal statuses
    try:
        status_val_local = obj.get("status") if isinstance(obj, dict) else getattr(obj, "status", None)
        status_enum = normalize_booking_status(status_val_local)
        if status_enum in TERMINAL_STATUSES:
            can_cancel = False
            can_reschedule = False
    except Exception:
        pass

    return bool(can_cancel), bool(can_reschedule)


async def format_bookings_for_ui(rows: Iterable[Any], lang: str) -> list[tuple[str, int]]:
    """Format a sequence of DB rows into a list of (text, booking_id) pairs.

    This helper is intended for handlers to call before invoking keyboard builders.
    """
    out: list[tuple[str, int]] = []
    for r in rows:
        try:
            text, bid = format_booking_list_item(r, role="client", lang=lang)
            if bid is not None:
                out.append((text, int(bid)))
        except Exception:
            continue
    return out


# Wrapper `get_client_bookings` removed; call `BookingRepo.get_paginated_list` directly.


# Legacy wrappers for role-based booking formatting were removed.
# Use `format_booking_list_item(row, role="master"|"admin"|"client")` directly.


# Lightweight DTO so handlers can consume service metadata without extra DB calls
class ServiceDTO:
    def __init__(self, id: str, name: str, duration_minutes: int | None = None, price_cents: int | None = None):
        self.id = id
        self.name = name
        self.duration_minutes = duration_minutes
        self.price_cents = price_cents


async def get_filtered_services() -> list[ServiceDTO]:
    """Return list of ServiceDTO for services that have at least one master.

    Each DTO contains basic display fields so handlers can avoid extra
    DB roundtrips when building menus.
    """
    out: list[ServiceDTO] = []
    try:
        from bot.app.core.db import get_session
        from bot.app.domain.models import Service, MasterService
        from sqlalchemy import select, join, outerjoin
        async with get_session() as session:
            # Join Service <- MasterService to ensure only services that have at least
            # one master are returned; legacy ServiceProfile join removed
            # fetch duration metadata in the same query. Use GROUP BY to avoid
            # DISTINCT + ORDER BY portability issues across DB engines.
            # Build FROM/JOINs using join()/outerjoin() to avoid overwriting
            # the FROM clause when calling select_from() multiple times.
            stmt = (
                select(
                    Service.id,
                    Service.name,
                        Service.duration_minutes,
                        Service.price_cents,
                )
                .join(MasterService, MasterService.service_id == Service.id)
                    .group_by(Service.id, Service.name, Service.duration_minutes, Service.price_cents)
            )
            rows = (await session.execute(stmt)).all()

            # Currency is a global setting; ServiceDTO no longer carries it.

            for r in rows:
                sid = str(r[0])
                name = str(r[1] or sid)
                try:
                    dur = int(r[2]) if r[2] is not None else None
                except Exception:
                    dur = None
                try:
                    pc = int(r[3]) if r[3] is not None else None
                except Exception:
                    pc = None
                out.append(ServiceDTO(id=sid, name=name, duration_minutes=dur, price_cents=pc))
            return out
    except Exception:
        logger.exception("get_filtered_services failed")
        return []

    # (Duplicate legacy definition removed during consolidation.)


# Thin wrapper `format_booking_details_text` removed; use the shared
# implementation imported at module top (`format_booking_details_text`).


# --- Booking presentation and list helpers moved from shared_services ---




@dataclass(slots=True)
class BookingDetails:
    booking_id: int
    service_name: str | None = None
    master_name: str | None = None
    price_cents: int = 0
    currency: str | None = None
    starts_at: datetime | None = None
    ends_at: datetime | None = None
    date_str: str | None = None
    client_id: int | None = None
    duration_minutes: int | None = None
    raw: Any | None = None
    status: str | None = None
    client_name: str | None = None
    client_phone: str | None = None
    client_telegram_id: int | None = None
    client_username: str | None = None
    can_cancel: bool = False
    can_reschedule: bool = False


class BookingResult(TypedDict, total=False):
    ok: bool
    booking_id: int | None
    status: str | None
    starts_at: str | None
    ends_at: str | None
    cash_hold_expires_at: str | None
    original_price_cents: int | None
    final_price_cents: int | None
    discount_amount_cents: int | None
    currency: str | None
    duration_minutes: int | None
    master_id: int | None
    master_name: str | None
    payment_method: str | None
    invoice_url: str | None
    text: str | None
    error: str | None

# ---------------- Calendar computation (moved from client_keyboards) ----------------
def compute_calendar_day_states(
    year: int,
    month: int,
    *,
    today: datetime | None = None,
    allowed_weekdays: list[int] | None = None,
    available_days: set[int] | None = None,
) -> list[list[tuple[int, str]]]:
    """Return structured calendar week/day state data for a given month.

    Each week is a list of tuples (day, state) where state is one of:
      - 'empty' (padding cell from monthcalendar -> 0)
      - 'past' (date < today)
      - 'not_allowed' (weekday not in allowed_weekdays)
      - 'available' (day in available_days)
      - 'full' (day not in available_days but selectable weekday)

    Business logic was extracted from client_keyboards to keep UI builders dumb.
    """
    from calendar import monthcalendar
    if today is None:
        from datetime import date as _date
        today_date = _date.today()
    else:
        try:
            today_date = today.date()
        except Exception:
            from datetime import date as _date
            today_date = _date.today()

    if allowed_weekdays is None:
        allowed_weekdays = []
    if available_days is None:
        available_days = set()

    weeks_states: list[list[tuple[int, str]]] = []
    for week in monthcalendar(year, month):
        w_states: list[tuple[int, str]] = []
        for day in week:
            if day == 0:
                w_states.append((0, 'empty'))
                continue
            from datetime import date as _date
            day_date = _date(year, month, day)
            if day_date < today_date:
                w_states.append((day, 'past'))
                continue
            if day_date.weekday() not in allowed_weekdays:
                w_states.append((day, 'not_allowed'))
                continue
            if day in available_days:
                w_states.append((day, 'available'))
            else:
                w_states.append((day, 'full'))
        weeks_states.append(w_states)
    return weeks_states
    

def compute_month_label(year: int, month: int, lang: str) -> str:
    """Return localized month label (e.g. 'Березень 2025')."""
    # 1) Prefer explicit translation list (month_names_full)
    try:
        from bot.app.translations import tr as _tr
        months = _tr("month_names_full", lang=lang)
        if isinstance(months, list) and len(months) >= month and months[month - 1]:
            return f"{months[month - 1]} {year}"
    except Exception:
        pass

    # 2) Fallback to Babel month names if available
    try:
        from babel.dates import get_month_names  # type: ignore

        locale = (lang or "").replace("-", "_") or "en"
        names = get_month_names(width="wide", context="format", locale=locale)
        month_name = names.get(month)
        if month_name:
            return f"{month_name} {year}"
    except Exception:
        pass

    # 3) Final fallback: numeric month/year
    try:
        return f"{int(month):02d}.{year}"
    except Exception:
        return f"{month}/{year}"


# ---------------------------------------------------------------------------
# BookingRepo: repository for booking-related DB operations (single canonical)
# ---------------------------------------------------------------------------
class BookingRepo:
    """Repository for Booking-related DB operations. All methods are async
    and will open a DB session when needed.
    """

    @staticmethod
    async def get(booking_id: int):
        async with get_session() as session:
            from bot.app.domain.models import Booking
            return await session.get(Booking, booking_id)

    @staticmethod
    async def find_conflicting_booking(
        session,
        client_id: int | None,
        master_id: int | None,
        new_start: datetime,
        new_end: datetime,
        service_ids: Sequence[str] | None = None,
        window_back: timedelta | None = None,
    ) -> str | None:
        """Return conflict code string if a conflicting booking exists for the
        given client or master in the provided time interval, otherwise None.
        """
        from bot.app.domain.models import Booking, BookingItem, BookingStatus, Service

        if window_back is None:
            window_back = timedelta(hours=12)
        window_start = new_start - window_back

        active_statuses = tuple(ACTIVE_STATUSES)

        # Load candidate bookings for master and client
        master_rows = []
        user_rows = []
        if master_id is not None:
            master_stmt = select(Booking).where(
                Booking.master_id == master_id,
                Booking.status.in_(active_statuses),
                Booking.starts_at < new_end,
                Booking.starts_at >= window_start,
            ).order_by(Booking.starts_at)
            master_rows = (await session.execute(master_stmt)).scalars().all()
        if client_id is not None:
            user_stmt = select(Booking).where(
                Booking.user_id == client_id,
                Booking.status.in_(active_statuses),
                Booking.starts_at < new_end,
                Booking.starts_at >= window_start,
            ).order_by(Booking.starts_at)
            user_rows = (await session.execute(user_stmt)).scalars().all()

        # Build booking -> service_ids map
        booking_ids = [b.id for b in list(master_rows) + list(user_rows) if getattr(b, "id", None)]
        booking_service_map: dict[int, list[str]] = {}
        if booking_ids:
            bi_rows = (await session.execute(select(BookingItem.booking_id, BookingItem.service_id).where(BookingItem.booking_id.in_(booking_ids)))).all()
            for bid, sid in bi_rows:
                booking_service_map.setdefault(int(bid), []).append(str(sid))

        for b in list(master_rows) + list(user_rows):
            # If no booking_items exist for this booking, treat as empty list.
            if int(getattr(b, "id", 0)) not in booking_service_map:
                booking_service_map[int(getattr(b, "id", 0))] = []

        svc_ids = {sid for sids in booking_service_map.values() for sid in sids if sid}
        svc_durations: dict[str, int] = {}
        if svc_ids:
            svc_rows = await session.execute(select(Service).where(Service.id.in_(list(svc_ids))))
            for s in svc_rows.scalars().all():
                try:
                    svc_durations[str(s.id)] = int(getattr(s, "duration_minutes", 0) or 0)
                except Exception:
                    svc_durations[str(s.id)] = 0

        default_slot = await SettingsRepo.get_slot_duration()

        def compute_end(b_obj):
            # Prefer stored ends_at if present (populated during booking creation).
            try:
                stored_end = getattr(b_obj, "ends_at", None)
                if stored_end:
                    return stored_end
            except Exception:
                pass
            # Fallback legacy duration accumulation.
            bids = booking_service_map.get(int(getattr(b_obj, "id", 0)), [])
            total = 0
            for ss in bids:
                total += svc_durations.get(str(ss), 0) or default_slot
            if total <= 0:
                total = default_slot
            try:
                return getattr(b_obj, "starts_at") + timedelta(minutes=total)
            except Exception:
                return getattr(b_obj, "starts_at")

        # check user overlaps first
        for ub in user_rows:
            try:
                ub_start = getattr(ub, "starts_at")
                ub_end = compute_end(ub)
                if new_start < ub_end and new_end > ub_start:
                    return "client_already_has_booking_at_this_time"
            except Exception:
                continue

        for mb in master_rows:
            try:
                mb_start = getattr(mb, "starts_at")
                mb_end = compute_end(mb)
                if new_start < mb_end and new_end > mb_start:
                    return "slot_unavailable"
            except Exception:
                continue

        return None

    @staticmethod
    async def get_conflicting_bookings_ids(
        master_id: int,
        windows: Sequence[tuple[int, int, int]],
        start: datetime,
        end: datetime,
        *,
        excluded_statuses: Sequence[Any] | None = None,
        return_ids_only: bool = False,
    ) -> list[int] | list[BookingConflictRow]:
        """Return bookings that overlap configured windows (by master and datetime range).
        
        Args:
            master_id: The ID of the master.
            windows: A sequence of tuples containing day, start minute, and end minute.
            start: The start datetime for the query.
            end: The end datetime for the query.
            excluded_statuses: Optional list of statuses to exclude from the results.
            return_ids_only: If True, return only the booking IDs.
        
        Returns:
            A list of booking IDs or BookingConflictRow objects.
        """
        if not windows:
            return []
        from bot.app.domain.models import Booking, BookingStatus, User
        from sqlalchemy import select, and_, or_, func

        if excluded_statuses is None:
            excluded_statuses = tuple(TERMINAL_STATUSES)

        minute_of_day = func.date_part('hour', Booking.starts_at) * 60 + func.date_part('minute', Booking.starts_at)
        day_expr = func.date_part('dow', Booking.starts_at)
        clauses = [
            and_(
                day_expr == day,
                minute_of_day >= start_min,
                minute_of_day < end_min,
            )
            for day, start_min, end_min in windows
        ]
        if not clauses:
            return []

        filters = [
            Booking.master_id == master_id,
            Booking.starts_at >= start,
            Booking.starts_at < end,
            Booking.status.notin_(excluded_statuses),
            or_(*clauses),
        ]

        async with get_session() as session:
            if return_ids_only:
                stmt = select(Booking.id).where(*filters).order_by(Booking.starts_at)
                res = await session.execute(stmt)
                return [int(r) for r in res.scalars().all()]

            stmt = (
                select(
                    Booking.id,
                    Booking.starts_at,
                    Booking.status,
                    Booking.master_id,
                    Booking.user_id,
                    User.name,
                    User.username,
                )
                .join(User, User.id == Booking.user_id)
                .where(*filters)
                .order_by(Booking.starts_at)
            )
            res = await session.execute(stmt)
            rows: list[BookingConflictRow] = []
            for row in res.fetchall():
                try:
                    bid, starts_at, status, master_id, user_id, user_name, username = row
                    rows.append(
                        BookingConflictRow(
                            booking_id=int(bid),
                            starts_at=starts_at,
                            status=status,
                            master_id=int(master_id) if master_id is not None else None,
                            user_id=int(user_id) if user_id is not None else None,
                            user_name=str(user_name) if user_name is not None else None,
                            username=str(username) if username is not None else None,
                        )
                    )
                except Exception:
                    continue
            return rows

    @staticmethod
    async def update_status(booking_id: int, new_status) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatusHistory
            booking = await session.get(Booking, booking_id)
            if not booking:
                return False
            old = getattr(booking, "status", None)
            booking.status = new_status  # Update the booking status
            try:
                hist = BookingStatusHistory(booking_id=booking.id, old_status=old, new_status=new_status)
                session.add(hist)
            except Exception:
                # best-effort: do not fail status update if history insert has issues
                pass
            await session.commit()
            return True

    @staticmethod
    async def recent_by_user(user_id: int, limit: int = 10):
        """Return recent Booking objects for given internal user id (limit newest first)."""
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import Booking
            res = await session.execute(select(Booking).where(Booking.user_id == int(user_id)).order_by(Booking.starts_at.desc()).limit(int(limit)))
            return res.scalars().all()

    @staticmethod
    async def recent_by_master(master_id: int, limit: int = 10):
        """Return recent Booking objects for given master telegram id (limit newest first)."""
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import Booking
            res = await session.execute(select(Booking).where(Booking.master_id == int(master_id)).order_by(Booking.starts_at.desc()).limit(int(limit)))
            return res.scalars().all()

    @staticmethod
    async def confirm_cash(booking_id: int) -> tuple[bool, str | None]:
        """Confirm a cash payment only if the slot is still available."""
        return await _finalize_booking_payment(booking_id, target_status=BookingStatus.CONFIRMED, set_paid_at=False)

    @staticmethod
    async def reschedule(booking_id: int, new_starts_at: datetime) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking
            b = await session.get(Booking, booking_id)
            if not b:
                return False

            # Keep end time consistent with existing duration
            duration = timedelta(minutes=60)
            if b.ends_at and b.starts_at:
                duration = b.ends_at - b.starts_at
            elif getattr(b, "duration_minutes", None):
                duration = timedelta(minutes=int(b.duration_minutes))

            b.starts_at = new_starts_at
            b.ends_at = new_starts_at + duration

            try:
                b.cash_hold_expires_at = None
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def mark_paid(booking_id: int) -> tuple[bool, str | None]:
        """Mark booking as paid if the slot is still valid and not taken.

        Returns (ok, error_code). error_code is one of:
        - booking_not_found
        - booking_not_active
        - slot_unavailable
        """
        return await _finalize_booking_payment(booking_id, target_status=BookingStatus.PAID, set_paid_at=True)

    @staticmethod
    async def set_cancelled(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus, BookingStatusHistory
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            old = getattr(b, "status", None)
            b.status = BookingStatus.CANCELLED
            try:
                hist = BookingStatusHistory(booking_id=b.id, old_status=old, new_status=BookingStatus.CANCELLED)
                session.add(hist)
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def delete_booking(booking_id: int) -> bool:
        """Permanently delete a booking row (and cascade delete related items).

        Intended for lightweight cleanup when a client abandons payment.
        Returns True if deletion occurred, False if booking not found.
        """
        async with get_session() as session:
            from bot.app.domain.models import Booking
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            try:
                await session.delete(b)
                await session.commit()
                return True
            except Exception:
                await session.rollback()
                return False

    @staticmethod
    async def list_active_by_user(user_id: int) -> list[Booking]:
        """Return upcoming/active Booking objects for a given user in a single query.

        Active = starts_at >= now and status not in terminal.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Booking, BookingStatus
                now = utc_now()
                stmt = (
                    select(Booking)
                    .where(
                        Booking.user_id == int(user_id),
                        Booking.starts_at >= now,
                        Booking.status.notin_(tuple(TERMINAL_STATUSES)),
                    )
                    .order_by(Booking.starts_at)
                )
                res = await session.execute(stmt)
                return list(res.scalars().all())
        except Exception as e:
            logger.exception("BookingRepo.list_active_by_user failed for %s: %s", user_id, e)
            return []

    @staticmethod
    async def list_history_by_user(user_id: int, limit: int = 50) -> list[Booking]:
        """Return past or terminal bookings for a user (newest first).

        History = starts_at < now OR status is terminal (cancelled/done/no_show/etc.).
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select, or_
                from bot.app.domain.models import Booking

                now = utc_now()
                from bot.app.domain.models import BookingStatus

                # Only include explicit history statuses: cancelled, done, no_show
                history_statuses = (BookingStatus.CANCELLED, BookingStatus.DONE, BookingStatus.NO_SHOW)
                stmt = (
                    select(Booking)
                    .where(
                        Booking.user_id == int(user_id),
                        Booking.status.in_(history_statuses),
                    )
                    .order_by(Booking.starts_at.desc())
                    .limit(int(limit))
                )
                res = await session.execute(stmt)
                return list(res.scalars().all())
        except Exception as e:
            logger.exception("BookingRepo.list_history_by_user failed for %s: %s", user_id, e)
            return []

    @staticmethod
    async def set_pending_payment(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus, BookingStatusHistory
            # Lock the row to avoid reviving an already-expired booking concurrently.
            stmt = select(Booking).where(Booking.id == booking_id).with_for_update()
            b = (await session.execute(stmt)).scalar_one_or_none()
            if not b:
                return False

            now_utc = utc_now()
            try:
                hold_min = await SettingsRepo.get_reservation_hold_minutes()
            except Exception:
                hold_min = 5

            # Do not revive terminal/confirmed/paid bookings.
            status = getattr(b, "status", None)
            if status in TERMINAL_STATUSES or status in {BookingStatus.CONFIRMED, BookingStatus.PAID}:
                return False

            # If hold already expired, refuse to extend and leave for rebooking.
            hold_expires = getattr(b, "cash_hold_expires_at", None)
            created_at = getattr(b, "created_at", None)
            if hold_expires and hold_expires <= now_utc:
                return False
            if not hold_expires:
                cutoff = now_utc - timedelta(minutes=max(1, int(hold_min or 0)))
                if created_at is None or created_at <= cutoff:
                    return False

            old = status
            b.status = BookingStatus.PENDING_PAYMENT
            # Extend hold window to give the user time to finish payment/confirmation
            try:
                b.cash_hold_expires_at = now_utc + timedelta(minutes=max(1, int(hold_min or 0)))
            except Exception:
                pass
            try:
                hist = BookingStatusHistory(booking_id=b.id, old_status=old, new_status=BookingStatus.PENDING_PAYMENT)
                session.add(hist)
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def ensure_owner(user_id: int, booking_id: int):
        async with get_session() as session:
            from bot.app.domain.models import Booking
            booking = await session.get(Booking, booking_id)
            if booking and getattr(booking, "user_id", None) == user_id:
                return booking
        return None

    @staticmethod
    async def query_bookings_range(start: datetime, end: datetime, mode: str | None = "all") -> list[Any]:
        """Return Booking rows in [start, end) filtered by mode (paid/awaiting/upcoming/cancelled/done/no_show).

        start/end are expected to be timezone-aware datetimes already converted to UTC when passed.
        """
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import Booking, BookingStatus
            stmt = select(Booking).order_by(Booking.starts_at.desc()).where(
                Booking.starts_at >= start,
                Booking.starts_at < end,
            )
            if mode == "paid":
                stmt = stmt.where(Booking.status == BookingStatus.PAID)
            elif mode == "awaiting":
                # 'awaiting' groups pending/payment-like statuses. Legacy
                # 'awaiting_cash' has been normalized to 'pending_payment'.
                stmt = stmt.where(Booking.status.in_(
                    [BookingStatus.CONFIRMED, BookingStatus.PENDING_PAYMENT, BookingStatus.RESERVED]
                ))
            elif mode == "upcoming":
                from bot.app.services.shared_services import utc_now
                now_utc = utc_now()
                stmt = stmt.where(Booking.starts_at >= now_utc)
            elif mode == "cancelled":
                stmt = stmt.where(Booking.status == BookingStatus.CANCELLED)
            elif mode == "done":
                stmt = stmt.where(Booking.status == BookingStatus.DONE)
            elif mode == "no_show":
                stmt = stmt.where(Booking.status == BookingStatus.NO_SHOW)

            rows = (await session.execute(stmt)).scalars().all()
            return list(rows)

    @staticmethod
    async def get_booking_service_names(booking_id: int) -> str:
        """Return service display name for a booking, combining multiple items if present."""
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingItem, Service
            b = await session.get(Booking, booking_id)
            if not b:
                return str(booking_id)
            rows = list((await session.execute(
                select(BookingItem.service_id, Service.name)
                .join(Service, Service.id == BookingItem.service_id)
                .where(BookingItem.booking_id == booking_id)
            )).all())
            if rows:
                return " + ".join([r[1] or str(r[0]) for r in rows])
            # No booking_items: return booking id as a fallback display.
            return str(booking_id)

    @staticmethod
    async def _prepare_pagination_context(
        session,
        Booking,
        BookingStatus,
        base_where: list[Any],
        mode: str,
        page: int,
        page_size: int | None,
        start: datetime | None,
        end: datetime | None,
        now: datetime,
        *,
        completed_statuses: Iterable[BookingStatus] | None = None,
    ) -> tuple[list[Any], Any, dict[str, int], int, int, int]:
        """Build canonical where/order/pagination metadata shared by client and master paging."""
        done_count = int((await session.execute(select(func.count()).select_from(Booking).where(*base_where, Booking.status == BookingStatus.DONE))).scalar() or 0)
        cancelled_count = int((await session.execute(select(func.count()).select_from(Booking).where(*base_where, Booking.status == BookingStatus.CANCELLED))).scalar() or 0)
        noshow_count = int((await session.execute(select(func.count()).select_from(Booking).where(*base_where, Booking.status == BookingStatus.NO_SHOW))).scalar() or 0)
        upcoming_count = int((await session.execute(
            select(func.count()).select_from(Booking).where(
                *base_where,
                Booking.starts_at >= now,
                Booking.status.notin_(tuple(TERMINAL_STATUSES)),
            )
        )).scalar() or 0)

        if completed_statuses is None:
            completed_statuses = tuple(TERMINAL_STATUSES)
        else:
            completed_statuses = tuple(completed_statuses)

        if mode == "completed":
            where_clause = [*base_where, Booking.status.in_(tuple(completed_statuses))]
            order_expr = Booking.starts_at.desc()
        elif mode == "done":
            where_clause = [*base_where, Booking.status == BookingStatus.DONE]
            order_expr = Booking.starts_at.desc()
        elif mode == "no_show":
            where_clause = [*base_where, Booking.status == BookingStatus.NO_SHOW]
            order_expr = Booking.starts_at.desc()
        elif mode == "cancelled":
            where_clause = [*base_where, Booking.status == BookingStatus.CANCELLED]
            order_expr = Booking.starts_at.desc()
        elif mode == "all":
            where_clause = [*base_where, Booking.starts_at >= now]
            order_expr = Booking.starts_at
        else:
            where_clause = [*base_where, Booking.starts_at >= now, Booking.status.notin_(tuple(TERMINAL_STATUSES))]
            order_expr = Booking.starts_at

        if start is not None:
            where_clause.append(Booking.starts_at >= start)
        if end is not None:
            where_clause.append(Booking.starts_at < end)

        total = int((await session.execute(select(func.count()).select_from(Booking).where(*where_clause))).scalar() or 0)

        if page_size:
            total_pages = max(1, (total + int(page_size) - 1) // int(page_size))
            p = max(1, min(int(page or 1), total_pages))
            offset = (p - 1) * int(page_size)
        else:
            total_pages = 1
            p = 1
            offset = 0

        meta = {
            "upcoming_count": upcoming_count,
            "done_count": done_count,
            "cancelled_count": cancelled_count,
            "noshow_count": noshow_count,
            "total": total,
            "total_pages": total_pages,
            "page": p,
        }
        meta["completed_count"] = done_count + cancelled_count + noshow_count
        return where_clause, order_expr, meta, total_pages, p, offset

    @staticmethod
    async def get_client_bookings_paginated(
        *,
        user_id: int,
        mode: str = "upcoming",
        page: int = 1,
        page_size: int | None = 5,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[list[BookingInfo], dict[str, Any]]:
        """Return rows and metadata for client-facing booking lists."""
        from bot.app.domain.models import Booking, BookingStatus, Master, BookingItem, Service

        now = utc_now()
        service_items_subq = (
            select(
                BookingItem.booking_id.label("booking_id"),
                func.string_agg(
                    func.coalesce(Service.name, func.cast(BookingItem.service_id, String)),
                    " + ",
                ).label("service_name"),
            )
            .join(Service, Service.id == BookingItem.service_id)
            .group_by(BookingItem.booking_id)
        ).subquery()
        # Also provide a canonical representative service_id per booking (first/min)
        service_first_subq = (
            select(
                BookingItem.booking_id.label("booking_id"),
                func.min(BookingItem.service_id).label("service_id"),
            )
            .group_by(BookingItem.booking_id)
        ).subquery()

        # Prefer aggregated booking item names; fall back to empty string when missing.
        service_name_expr = func.coalesce(service_items_subq.c.service_name, "").label("service_name")
        service_first_expr = func.coalesce(service_first_subq.c.service_id, "").label("service_id")
        async with get_session() as session:
            base_where = [Booking.user_id == user_id]
            where_clause, order_expr, meta, total_pages, p, offset = await BookingRepo._prepare_pagination_context(
                session,
                Booking,
                BookingStatus,
                base_where,
                mode,
                page,
                page_size,
                start,
                end,
                now,
                completed_statuses=(BookingStatus.DONE, BookingStatus.CANCELLED, BookingStatus.NO_SHOW),
            )

            stmt = (
                select(
                    Booking.id,
                    Booking.master_id,
                    Booking.status,
                    Booking.starts_at,
                    Booking.original_price_cents,
                    Booking.final_price_cents,
                    Master.name.label("master_name"),
                    service_name_expr,
                    service_first_expr,
                )
                .join(Master, Master.id == Booking.master_id, isouter=True)
                .outerjoin(service_items_subq, service_items_subq.c.booking_id == Booking.id)
                .outerjoin(service_first_subq, service_first_subq.c.booking_id == Booking.id)
                # Do not join Service by the removed Booking.service_id column; aggregated names
                # are provided by `service_items_subq` and representative id by `service_first_subq`.
                .where(*where_clause)
                .order_by(order_expr)
            )

            if page_size:
                stmt = stmt.limit(page_size).offset(offset)

            result = await session.execute(stmt)
            raw_rows = result.all()
            # Resolve global currency once for the mapper
            try:
                global_currency = await SettingsRepo.get_currency()
            except Exception:
                from bot.app.services.shared_services import _default_currency
                global_currency = _default_currency()

            booking_infos: list[BookingInfo] = []
            for (
                booking_id,
                master_id,
                status,
                starts_at,
                original_price_cents,
                final_price_cents,
                master_name,
                service_name,
                service_first_id,
            ) in raw_rows:
                booking_infos.append(
                    booking_info_from_mapping(
                        {
                            "id": booking_id,
                            "master_id": master_id,
                            "service_id": service_first_id or None,
                            "status": status,
                            "starts_at": starts_at,
                            "original_price_cents": original_price_cents,
                            "final_price_cents": final_price_cents,
                            "currency": global_currency,
                            "master_name": master_name,
                            "service_name": service_name,
                            "client_id": user_id,
                        }
                    )
                )
            meta["total_pages"] = total_pages
            meta["page"] = p
            return booking_infos, meta

    @staticmethod
    async def get_master_bookings_paginated(
        *,
        master_id: int,
        mode: str = "upcoming",
        page: int = 1,
        page_size: int | None = 5,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[list[BookingInfo], dict[str, Any]]:
        """Return normalized rows and metadata for master-facing booking lists."""
        from bot.app.domain.models import Booking, BookingStatus, User, BookingItem, Service

        now = utc_now()
        service_items_subq = (
            select(
                BookingItem.booking_id.label("booking_id"),
                func.string_agg(
                    func.coalesce(Service.name, func.cast(BookingItem.service_id, String)),
                    " + ",
                ).label("service_name"),
            )
            .join(Service, Service.id == BookingItem.service_id)
            .group_by(BookingItem.booking_id)
        ).subquery()
        service_name_expr = func.coalesce(service_items_subq.c.service_name, "").label("service_name")
        async with get_session() as session:
            base_where = [Booking.master_id == master_id]
            where_clause, order_expr, meta, total_pages, p, offset = await BookingRepo._prepare_pagination_context(
                session,
                Booking,
                BookingStatus,
                base_where,
                mode,
                page,
                page_size,
                start,
                end,
                now,
            )

            stmt = (
                select(Booking, User.name.label("client_name"), service_name_expr)
                .where(*where_clause)
                .order_by(order_expr)
                .join(User, User.id == Booking.user_id, isouter=True)
                .outerjoin(service_items_subq, service_items_subq.c.booking_id == Booking.id)
                # Do not join Service by Booking.service_id (column removed).
            )
            if page_size:
                stmt = stmt.limit(page_size).offset(offset)
            result = await session.execute(stmt)
            raw_rows = list(result.all())
            # Resolve global currency once to avoid hardcoded fallbacks in multiple rows
            try:
                from bot.app.services.shared_services import get_global_currency

                global_currency = await get_global_currency()
            except Exception:
                from bot.app.services.shared_services import _default_currency

                global_currency = _default_currency()

            booking_infos: list[BookingInfo] = []
            for b, client_name, svc_name in raw_rows:
                booking_infos.append(
                    booking_info_from_mapping(
                        {
                            "id": getattr(b, "id", None),
                            "master_id": getattr(b, "master_id", None),
                            "service_id": getattr(b, "service_id", None),
                            "status": getattr(b, "status", None),
                            "starts_at": getattr(b, "starts_at", None),
                            "original_price_cents": getattr(b, "original_price_cents", None),
                            "final_price_cents": getattr(b, "final_price_cents", None),
                            "currency": getattr(b, "currency", None) or global_currency,
                            "master_name": None,
                            "client_name": client_name,
                            "client_id": getattr(b, "user_id", None),
                            "service_name": svc_name,
                        }
                    )
                )
            meta["total_pages"] = total_pages
            meta["page"] = p
            return booking_infos, meta

    @staticmethod
    async def get_paginated_list(
        *,
        user_id: int | None = None,
        master_id: int | None = None,
        mode: str = "upcoming",
        page: int = 1,
        page_size: int | None = 5,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> tuple[list[BookingInfo], dict[str, Any]]:
        """Facade that delegates to client or master specific paging helpers."""
        if user_id is None and master_id is None:
            from bot.app.services.admin_services import ServiceRepo
            return await ServiceRepo.get_admin_bookings(mode=mode, page=page, page_size=page_size, start=start, end=end)
        if user_id is not None:
            return await BookingRepo.get_client_bookings_paginated(user_id=user_id, mode=mode, page=page, page_size=page_size, start=start, end=end)
        if master_id is None:
            raise ValueError("master_id is required when user_id is not provided")
        return await BookingRepo.get_master_bookings_paginated(master_id=master_id, mode=mode, page=page, page_size=page_size, start=start, end=end)



def is_booking_slot_blocked(booking: Booking, now_utc: datetime, hold_minutes: int | None) -> bool:
    """Return True if the booking is currently blocking a slot."""
    try:
        status = getattr(booking, "status", None)
        if status in {BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT}:
            hold_expires = getattr(booking, "cash_hold_expires_at", None)
            created_at = getattr(booking, "created_at", None)
            if hold_expires and hold_expires > now_utc:
                return True
            if not hold_expires:
                cutoff = now_utc - timedelta(minutes=max(1, int(hold_minutes or 0)))
                if created_at is not None and created_at > cutoff:
                    return True
        elif status in ACTIVE_BLOCKING_STATUSES:
            return True
    except Exception:
        return False
    return False


def _get_booking_interval(booking: Booking, fallback_minutes: int) -> tuple[datetime, datetime] | None:
    """Return the UTC interval ([start, end]) that the booking occupies (fallback to slot minutes)."""
    try:
        start = getattr(booking, "starts_at", None)
        if not start:
            return None
        end = getattr(booking, "ends_at", None)
        if not end:
            duration = getattr(booking, "duration_minutes", None)
            fallback = int(duration) if isinstance(duration, (int, float)) and duration > 0 else fallback_minutes or 0
            fallback = max(1, fallback)
            end = start + timedelta(minutes=fallback)
        return start, end
    except Exception:
        return None
async def build_booking_details(
    booking: object,
    service_name: str | None = None,
    master_name: str | None = None,
    user_id: int | None = None,
    date: str | None = None,
    lang: str | None = None,
) -> BookingDetails:
    # Resolve language
    try:
        if not lang and user_id:
            lang = await safe_get_locale(user_id)
    except Exception:
        lang = default_language()

    data: dict | None = None
    try:
        # import here to avoid cycles
        from bot.app.services import master_services as _ms

        if isinstance(booking, dict):
            data = booking
        else:
            bid = getattr(booking, "id", None) or (booking if isinstance(booking, int) else None)
            if bid is not None:
                data = await _ms.MasterRepo.get_booking_display_data(int(bid))
    except Exception:
        data = None

    if not data:
        try:
            from bot.app.services.shared_services import get_global_currency

            global_currency = await get_global_currency()
        except Exception:
            from bot.app.services.shared_services import _default_currency

            global_currency = _default_currency()

        data = {
            "booking_id": getattr(booking, "id", booking if isinstance(booking, int) else 0),
            "service_name": service_name,
            "master_name": master_name,
            "price_cents": getattr(booking, "final_price_cents", None)
            or getattr(booking, "original_price_cents", None)
            or 0,
            "currency": getattr(booking, "currency", None) or global_currency,
            "starts_at": getattr(booking, "starts_at", None),
            "client_id": user_id,
        }

    if service_name:
        data["service_name"] = service_name
    if master_name:
        data["master_name"] = master_name
    if date:
        data["date_str"] = date

    status_val = data.get("status")
    client_name = data.get("client_name")
    client_phone = data.get("client_phone")
    client_tg = data.get("client_telegram_id") or data.get("client_tid") or data.get("client_tg_id")
    client_username = data.get("client_username")

    # Try to enrich with Booking/User models when available
    b = None
    try:
        b = await BookingRepo.get(int(data.get("booking_id") or 0))
    except Exception:
        b = None

    if b is not None:
        try:
            status_val = getattr(b.status, "value", str(b.status))
        except Exception:
            status_val = str(getattr(b, "status", ""))
        if not data.get("starts_at") and getattr(b, "starts_at", None):
            data["starts_at"] = b.starts_at
        if not data.get("price_cents"):
            data["price_cents"] = getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", None) or 0
        try:
            if getattr(b, "user_id", None):
                u = await UserRepo.get_by_id(int(b.user_id))
                if u:
                    client_name = client_name or getattr(u, "name", None)
                    client_tg = client_tg or getattr(u, "telegram_id", None)
                    client_username = client_username or getattr(u, "username", None)
        except Exception:
            pass
    # Calculate permissions using module-level helper. Prefer explicit values
    # from SettingsRepo (passed here) to avoid hidden cfg dependencies and
    # improve testability.
    try:
        lock_r_val = await SettingsRepo.get_client_reschedule_lock_minutes()
        lock_c_val = await SettingsRepo.get_client_cancel_lock_minutes()
    except Exception:
        lock_r_val = None
        lock_c_val = None
    can_cancel, can_reschedule = await calculate_booking_permissions(data, lock_r_minutes=lock_r_val, lock_c_minutes=lock_c_val)

    try:
        from bot.app.services.shared_services import get_global_currency
        global_currency = await get_global_currency()
    except Exception:
        from bot.app.services.shared_services import _default_currency

        global_currency = _default_currency()

    return BookingDetails(
        booking_id=int(data.get("booking_id", 0) or 0),
        service_name=data.get("service_name"),
        master_name=data.get("master_name"),
        price_cents=int(data.get("price_cents", 0) or 0),
        currency=data.get("currency") or global_currency,
        starts_at=data.get("starts_at"),
        ends_at=data.get("ends_at"),
        duration_minutes=(int(data.get("duration_minutes")) if data.get("duration_minutes") is not None else None),
        date_str=data.get("date_str"),
        client_id=data.get("client_id"),
        raw=data,
        status=status_val,
        client_name=client_name,
        client_phone=client_phone,
        client_telegram_id=int(client_tg) if client_tg else None,
        client_username=client_username,
        can_cancel=bool(can_cancel),
        can_reschedule=bool(can_reschedule),
    )


# Wrapper `get_bookings_list` removed; use `BookingRepo.get_paginated_list`.


# ---------------- Repositories moved from shared_services -----------------
class UserRepo:
    """Repository for User-related lookups (moved from shared_services)."""

    @staticmethod
    async def get_by_id(user_id: int) -> User | None:
        async with get_session() as session:
            return await session.get(User, user_id)

    @staticmethod
    async def get_by_telegram_id(telegram_id: int) -> User | None:
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(User).where(User.telegram_id == telegram_id))
            return result.scalar_one_or_none()

    @staticmethod
    async def get_locale(telegram_id: int) -> str | None:
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(User.locale).where(User.telegram_id == telegram_id))
            return result.scalar_one_or_none()

    @staticmethod
    async def get_locale_by_telegram_id(telegram_id: int) -> str | None:
        """Alias for clarity: returns locale string for a Telegram user id or None."""
        return await UserRepo.get_locale(telegram_id)

    @staticmethod
    async def get_or_create(telegram_id: int, name: str | None = None, username: str | None = None):
        async with get_session() as session:
            from sqlalchemy import select
            user = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if user:
                changed = False
                if username and getattr(user, "username", None) != username:
                    try:
                        user.username = username
                        changed = True
                    except Exception:
                        pass
                if name and getattr(user, "name", None) != name:
                    try:
                        user.name = name
                        changed = True
                    except Exception:
                        pass
                if changed:
                    await session.commit()
                return user

            new_user = User(telegram_id=telegram_id, name=name or (username or str(telegram_id)))
            try:
                new_user.username = username
            except Exception:
                pass
            session.add(new_user)
            await session.commit()
            await session.refresh(new_user)
            return new_user

    @staticmethod
    async def set_locale(telegram_id: int, locale: str) -> bool:
        async with get_session() as session:
            from sqlalchemy import select
            user = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if not user:
                user = User(telegram_id=telegram_id, name=str(telegram_id), locale=locale)
                session.add(user)
            else:
                try:
                    user.locale = locale
                except Exception:
                    pass
            await session.commit()
        return True

    @staticmethod
    async def get_by_ids(ids: set[int]) -> dict[int, "User"]:
        if not ids:
            return {}
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(User).where(User.id.in_(ids)))
            rows = result.scalars().all()
            return {u.id: u for u in rows}

    @staticmethod
    async def get_by_telegram_ids(tids: set[int]) -> dict[int, "User"]:
        if not tids:
            return {}
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(User).where(User.telegram_id.in_(tids)))
            rows = result.scalars().all()
            return {u.telegram_id: u for u in rows}


# Use the canonical SettingsRepo from admin_services (imported above).



# Thin wrapper `get_or_create_user` removed; call `UserRepo.get_or_create` directly.


async def get_available_time_slots_for_services(
    date: datetime,
    master_id: int,
    service_durations: list[int],
    *,
    exclude_booking_id: int | None = None,
) -> List[dtime]:
    """
    Calculates available slots based on 'Gap' logic:
    1. Get work windows.
    2. Get busy intervals (bookings).
    3. Subtract busy intervals from windows to find free gaps.
    4. A slot is available at the START of each gap if the gap is long enough.
    """
    total_duration = sum(service_durations)
    if total_duration <= 0:
        return []

    try:
        # Normalize input `date` into an aware local datetime representing
        # the target day in the business/local timezone. Callers typically
        # pass a naive ISO date (e.g. 2025-12-10) — treat those as local-day
        # references rather than guessing UTC.
        local_tz = get_local_tz() or UTC
        if isinstance(date, datetime):
            if date.tzinfo is None:
                ref_local = date.replace(tzinfo=local_tz)
            else:
                ref_local = date.astimezone(local_tz)
        else:
            # If a plain date was passed, construct a midnight-local datetime
            ref_local = datetime.combine(date, dtime()).replace(tzinfo=local_tz)

        # 1. Get Work Windows (Local Time)
        # Returns list of (start_time, end_time) as time objects
        windows_local = await master_services.get_work_windows_for_day(master_id, ref_local)
    except Exception:
        windows_local = [(dtime(hour=9), dtime(hour=18))]
    
    if not windows_local:
        return []

    # 2. Get Bookings (UTC)
    # Compute the local-day boundaries (aware datetimes) and convert them
    # to UTC for DB-range queries. All DB timestamps are stored in UTC.
    local_day_start = ref_local.replace(hour=0, minute=0, second=0, microsecond=0)
    local_day_end = local_day_start + timedelta(days=1)
    day_start_utc = local_day_start.astimezone(UTC)
    day_end_utc = local_day_end.astimezone(UTC)

    async with get_session() as session:
        stmt = select(Booking).where(
            Booking.master_id == master_id,
            Booking.starts_at >= day_start_utc,
            Booking.starts_at < day_end_utc,
        )
        if exclude_booking_id is not None:
            stmt = stmt.where(Booking.id != int(exclude_booking_id))
        stmt = stmt.order_by(Booking.starts_at)
        result = await session.execute(stmt)
        bookings_objs = result.scalars().all()

    hold_minutes = await SettingsRepo.get_reservation_hold_minutes()
    now_utc = utc_now()
    
    busy_intervals = []
    for b in bookings_objs:
        if is_booking_slot_blocked(b, now_utc, hold_minutes):
            # Use a default fallback if duration is missing, though usually it should be there.
            # We use 1 minute as absolute minimum fallback to avoid zero-length blocks if something is wrong.
            interval = _get_booking_interval(b, 60) 
            if interval:
                busy_intervals.append(interval)

    # Merge overlapping busy intervals
    # Sort by start time
    busy_intervals.sort(key=lambda x: x[0])
    merged_busy = []
    for b_start, b_end in busy_intervals:
        if not merged_busy:
            merged_busy.append((b_start, b_end))
        else:
            last_start, last_end = merged_busy[-1]
            if b_start < last_end:
                # Overlap or adjacent
                merged_busy[-1] = (last_start, max(last_end, b_end))
            else:
                merged_busy.append((b_start, b_end))
    
    # 3. Calculate Gaps
    # Convert windows (local times) into UTC-aware intervals for gap
    # calculations. Use the `ref_local` date so we map times to the
    # correct calendar day in local timezone.
    window_intervals_utc = []
    for ws, we in windows_local:
        w_start = datetime.combine(ref_local.date(), ws).replace(tzinfo=local_tz).astimezone(UTC)
        w_end = datetime.combine(ref_local.date(), we).replace(tzinfo=local_tz).astimezone(UTC)
        window_intervals_utc.append((w_start, w_end))

    free_gaps = []
    for w_start, w_end in window_intervals_utc:
        current_start = w_start
        for b_start, b_end in merged_busy:
            # If booking ends before current window position, skip it
            if b_end <= current_start:
                continue
            # If booking starts after this window, we are done with bookings for this window
            if b_start >= w_end:
                break
            
            # If there is a gap before the booking
            if b_start > current_start:
                free_gaps.append((current_start, b_start))
            
            # Advance current_start to the end of the booking
            current_start = max(current_start, b_end)
        
        # If there is space left after the last booking in this window
        if current_start < w_end:
            free_gaps.append((current_start, w_end))

    # 4. Filter and Collect Slots
    slots = []
    lead_min = await SettingsRepo.get_same_day_lead_minutes()
    now_utc = utc_now()
    now_local = now_utc.astimezone(local_tz)
    is_today = (local_day_start.date() == now_local.date())

    for gap_start, gap_end in free_gaps:
        # Calculate duration in minutes
        gap_duration_minutes = (gap_end - gap_start).total_seconds() / 60
        
        # Check if the gap is large enough for the total service duration
        if gap_duration_minutes >= total_duration:
            # Instead of only returning the gap start, generate stepped slots
            # across the gap so clients can pick any available start time.
            # Determine tick step for generating candidate starts.
            # Prefer explicit admin setting `slot_tick_minutes` if available,
            # otherwise fall back to 5 minutes which provides fine-grained
            # selection for clients.
            try:
                slot_step_min = await SettingsRepo.get_slot_tick_minutes()
                slot_step_min = int(slot_step_min or 0)
            except Exception:
                slot_step_min = 0

            if not slot_step_min or slot_step_min <= 0:
                # default to a 15-minute grid to match the main bot UX
                # and avoid client-side adjustments. Administrators can
                # still override via `slot_tick_minutes` setting.
                slot_step_min = 15

            current = gap_start
            # walk the gap in steps and add each candidate that fits total_duration
            while (current + timedelta(minutes=total_duration)) <= gap_end:
                # Check lead time if it's today (compare in local timezone)
                if is_today and lead_min:
                    try:
                        candidate_local = current.astimezone(local_tz)
                        if (candidate_local - now_local).total_seconds() / 60 < lead_min:
                            current = current + timedelta(minutes=slot_step_min)
                            continue
                    except Exception:
                        pass

                # Return time objects in local tz for UI (buttons expect local times)
                slots.append(current.astimezone(local_tz).time())
                current = current + timedelta(minutes=slot_step_min)

    logger.debug("Slots (Gap-based) for master %s on %s: %s", master_id, date, slots)
    return slots



async def get_available_days_for_month(master_id: int, year: int, month: int, service_duration_min: int = 60) -> set[int]:
    """
    Возвращает набор дней (числа месяца), в которые у мастера есть свободные слоты.
    Использует SQL-таблицу master_schedules для получения графика работы.
    """
    try:
        from calendar import monthrange
        from sqlalchemy import select, and_

        _, days_in_month = monthrange(year, month)

        # 1. Определяем границы месяца (в локальном часовом поясе бизнеса)
        local_tz = get_local_tz() or UTC
        month_start_local = datetime(year, month, 1, tzinfo=local_tz)
        if month == 12:
            next_month_local = datetime(year + 1, 1, 1, tzinfo=local_tz)
        else:
            next_month_local = datetime(year, month + 1, 1, tzinfo=local_tz)

        month_start_utc = month_start_local.astimezone(UTC)
        next_month_utc = next_month_local.astimezone(UTC)
        now_utc = utc_now()

        async with get_session() as session:
            # 2. Загружаем все бронирования мастера за этот месяц (одним запросом)
            # Используем master_id как есть (предполагаем, что это суррогатный ID, если нет - резолвим)
            
            # Резолвинг ID (на всякий случай, если передан telegram_id)
            real_master_id = master_id
            master_obj = await session.execute(select(Master.id).where(Master.telegram_id == master_id))
            mid_row = master_obj.scalar_one_or_none()
            if mid_row:
                real_master_id = mid_row
            
            # Получаем брони
            bookings_stmt = select(Booking).where(
                Booking.master_id == real_master_id,
                Booking.starts_at >= month_start_utc,
                Booking.starts_at < next_month_utc,
                Booking.status.notin_(tuple(TERMINAL_STATUSES)) # Игнорируем отмененные
            ).order_by(Booking.starts_at)
            
            bookings_result = await session.execute(bookings_stmt)
            bookings_raw_objs = bookings_result.scalars().all()

            # 3. Загружаем расписание из SQL (master_schedules)
            # Теперь расписание привязано напрямую к мастеру по master_id
            schedule_stmt = (
                select(MasterSchedule)
                .where(MasterSchedule.master_id == real_master_id)
            )
            schedule_result = await session.execute(schedule_stmt)
            schedule_rows = schedule_result.scalars().all()

        # 4. Преобразуем расписание в удобный словарь: {день_недели (0-6): [(start, end), ...]}
        weekly_schedule = {}
        for row in schedule_rows:
            if row.start_time and row.end_time:
                # В базе хранятся time объекты (напр. 09:00:00)
                weekly_schedule.setdefault(row.day_of_week, []).append((row.start_time, row.end_time))

        # 5. Подготавливаем интервалы занятости (брони)
        blocked_intervals: list[tuple[datetime, datetime]] = []
        hold_minutes = await SettingsRepo.get_reservation_hold_minutes()
        
        for b in bookings_raw_objs:
            if is_booking_slot_blocked(b, now_utc, hold_minutes):
                interval = _get_booking_interval(b, service_duration_min)
                if interval:
                    blocked_intervals.append(interval)

        available_days: set[int] = set()
        step = timedelta(minutes=service_duration_min)
        lead_min = await SettingsRepo.get_same_day_lead_minutes()
        now_local = now_utc.astimezone(local_tz)

        # 6. Проходим по каждому дню месяца
        for day in range(1, days_in_month + 1):
            day_date_local = datetime(year, month, day, tzinfo=local_tz)
            weekday = day_date_local.weekday()

            # Получаем окна работы для этого дня недели из словаря
            # Если для дня нет записей в БД -> день выходной
            windows_time = weekly_schedule.get(weekday, [])
            
            if not windows_time:
                continue # Выходной

            # Конвертируем окна (time) в полные datetime для текущего дня
            windows_local = []
            for start_t, end_t in windows_time:
                w_start = datetime.combine(day_date_local.date(), start_t).replace(tzinfo=local_tz)
                w_end = datetime.combine(day_date_local.date(), end_t).replace(tzinfo=local_tz)
                windows_local.append((w_start, w_end))

            # Фильтруем брони, относящиеся к этому дню (в UTC)
            day_start_utc = day_date_local.astimezone(UTC)
            day_end_utc = (day_date_local + timedelta(days=1)).astimezone(UTC)
            
            bookings_for_day = [
                (b_start, b_end)
                for b_start, b_end in blocked_intervals
                if b_start < day_end_utc and b_end > day_start_utc
            ]

            is_today = (day_date_local.date() == now_local.date())
            any_slot_found = False

            # Проверяем наличие слотов
            for w_start_local, w_end_local in windows_local:
                # Переводим окно работы в UTC для сравнения с бронями
                current_utc = w_start_local.astimezone(UTC)
                w_end_utc = w_end_local.astimezone(UTC)

                while current_utc + step <= w_end_utc:
                    slot_start = current_utc
                    slot_end = current_utc + step

                    # Проверка lead time (если сегодня)
                    if lead_min and is_today:
                        try:
                            # Сравниваем в локальном времени
                            local_slot_dt = slot_start.astimezone(local_tz)
                            if (local_slot_dt - now_local).total_seconds() / 60 < lead_min:
                                current_utc += step
                                continue
                        except Exception:
                            pass
                    
                    # Проверка на прошедшее время
                    if slot_start < now_utc:
                         current_utc += step
                         continue

                    # Проверка пересечений с бронями
                    overlap = False
                    for b_start, b_end in bookings_for_day:
                        # Стандартная проверка пересечения интервалов: max(start1, start2) < min(end1, end2)
                        if slot_start < b_end and slot_end > b_start:
                            overlap = True
                            break
                    
                    if not overlap:
                        any_slot_found = True
                        break # Нашли хотя бы один слот в этот день
                    
                    current_utc += step
                
                if any_slot_found:
                    break

            if any_slot_found:
                available_days.add(day)

        return available_days

    except Exception as e:
        logger.exception("Ошибка получения доступных дней (SQL) для мастера %s %04d-%02d: %s", master_id, year, month, e)
        return set()




async def create_booking(client_id: int, master_id: int, service_id: str, slot: datetime, *, hold_minutes: int | None = None) -> Booking:
    """Создает новую запись (бронирование).

    Args:
        client_id: ID клиента.
        master_id: Telegram ID мастера.
        service_id: ID услуги.
        slot: Время начала записи.
        hold_minutes: Время удержания резерва в минутах (опционально).

    Returns:
        Объект Booking.
    """
    try:
        async with get_session() as session:
            # Normalize provided master identifier into canonical surrogate id
            # (Master.id). Callers may pass either the surrogate id or the
            # legacy telegram_id — prefer surrogate id and fall back to
            # telegram_id for compatibility. If resolution fails, raise so
            # the calling flow can handle it.
            try:
                from sqlalchemy import select
                from bot.app.domain.models import Master
                mid = await session.scalar(select(Master.id).where(Master.id == int(master_id)))
                if not mid:
                    mid = await session.scalar(select(Master.id).where(Master.telegram_id == int(master_id)))
                if not mid:
                    raise ValueError("master_not_found")
                resolved_master_id = int(mid)
            except ValueError:
                raise
            except Exception:
                # If resolution failed due to DB error, log and re-raise to
                # surface the failure to callers.
                logger.exception("Failed to resolve master id for %s", master_id)
                raise

            # Acquire advisory lock for the specific (master_id, slot) pair to
            # avoid races with the expiration worker. Use the two-int advisory
            # lock variant; reduce the surrogate id and epoch to 32-bit space
            # deterministically to avoid overflow.
            try:
                from sqlalchemy import text
                k1 = int(resolved_master_id) % 2147483647
                k2 = int(new_start.timestamp()) % 2147483647 if 'new_start' in locals() else int(slot.timestamp()) % 2147483647
                await session.execute(text("SELECT pg_advisory_xact_lock(:k1, :k2)"), {"k1": k1, "k2": k2})
            except Exception:
                # Advisory locks are best-effort; if unavailable, continue and
                # rely on existing row-level locking and unique-index checks.
                pass
            # Application-level guard: delegate interval-overlap checks to BookingRepo
            try:
                # Determine duration for the new booking (minutes) so we can compute new_end
                try:
                    totals = await get_services_duration_and_price([service_id], online_payment=False, master_id=resolved_master_id)
                    new_dur = int(totals.get("total_minutes") or 0)
                except Exception:
                    new_dur = 0
                if not new_dur:
                    new_dur = await SettingsRepo.get_slot_duration()

                new_start = slot
                new_end = new_start + timedelta(minutes=new_dur)

                conflict = await BookingRepo.find_conflicting_booking(session, client_id, resolved_master_id, new_start, new_end, service_ids=[service_id])
                if conflict:
                    raise ValueError(conflict)
            except ValueError:
                raise
            except Exception:
                # on DB error (rare), continue to attempt create and let the
                # commit surface the exception
                pass

            # Optional lightweight locking: lock potentially overlapping bookings for master and client
            try:
                from bot.app.domain.models import Booking as _B
                await session.execute(
                    select(_B.id).where(
                        _B.master_id == resolved_master_id,
                        _B.starts_at < new_end,
                        _B.ends_at > new_start,
                    ).with_for_update()
                )
                await session.execute(
                    select(_B.id).where(
                        _B.user_id == client_id,
                        _B.starts_at < new_end,
                        _B.ends_at > new_start,
                    ).with_for_update()
                )
            except Exception as e:
                # If locking fails (e.g., DB backend limitations), log the failure.
                # In some deployments row-locking is critical to avoid races; enable
                # strict behavior via `REQUIRE_ROW_LOCK` flag in constants.
                logger.exception("Row-level locking failed while creating booking: %s", e)
                if REQUIRE_ROW_LOCK_STRICT:
                    raise

            # Snapshot service price at booking time and create booking via helper
            svc = await session.get(Service, service_id)
            svc_price = int(getattr(svc, "price_cents", 0) or 0)
            booking = await _create_booking_base(session, client_id, resolved_master_id, slot, price_cents=svc_price, hold_minutes=hold_minutes, service_id=service_id, duration_minutes=new_dur)

            # Backfill booking_items for single-service bookings so booking_items
            # remains the canonical composition source.
            try:
                await session.flush()
                # Persist price snapshot for this booking item
                session.add(BookingItem(booking_id=booking.id, service_id=str(service_id), price_cents=svc_price, position=0))
            except IntegrityError as ie:
                # Integrity errors during flush (e.g. exclusion constraint violation)
                # leave the session in a rolled-back state and surface a friendly
                # `slot_unavailable` error so callers can handle it.
                try:
                    await session.rollback()
                except Exception:
                    pass
                logger.info("IntegrityError on flush while creating booking (slot likely taken): %s", ie)
                raise ValueError("slot_unavailable") from ie
            except Exception:
                # If flush/add fails for non-integrity reasons, let commit surface the error.
                pass

            try:
                await session.commit()
            except IntegrityError as ie:
                await session.rollback()
                logger.info("IntegrityError on commit while creating booking (slot likely taken): %s", ie)
                raise ValueError("slot_unavailable") from ie
            await session.refresh(booking)
            logger.info("Создана запись №%s: client_id=%s, master_id=%s (resolved=%s), slot=%s, expires_at=%s", booking.id, client_id, master_id, resolved_master_id, slot, booking.cash_hold_expires_at)
            return booking
    except SQLAlchemyError as e:
        logger.error("Ошибка создания записи: client_id=%s, master_id=%s, service_id=%s, slot=%s, error=%s", client_id, master_id, service_id, slot, e)
        raise


async def get_services_duration_and_price(service_ids: Sequence[str], online_payment: bool = False, master_id: int | None = None) -> dict[str, int | str]:
    """Return total duration (minutes) and total price_cents for selected services without N+1 queries.

    - Loads all Services in a single query.
    Uses `Service.duration_minutes` as the canonical duration; falls back to 60 per service.
    """
    total_minutes = 0
    total_price = 0
    # Resolve global currency once (single source of truth).
    from bot.app.services.shared_services import _default_currency
    try:
        from bot.app.services.admin_services import SettingsRepo

        try:
            currency = await SettingsRepo.get_currency()
        except Exception:
            currency = _default_currency()
    except Exception:
        currency = _default_currency()
    try:
        if not service_ids:
            return {"total_minutes": 0, "total_price_cents": 0, "currency": currency}
        async with get_session() as session:
            # Bulk load services
            svc_rows = await session.execute(select(Service).where(Service.id.in_(list(service_ids))))
            services = {str(s.id): s for s in svc_rows.scalars().all()}
            # ServiceProfile removed; durations come from Service or MasterService overrides
            overrides: dict[str, int] = {}
            if master_id is not None:
                from bot.app.domain.models import MasterService, Master
                # Normalize master_id param: accept either surrogate id or telegram id
                mid = await session.scalar(select(Master.id).where(Master.id == int(master_id)))
                if not mid:
                    mid = await session.scalar(select(Master.id).where(Master.telegram_id == int(master_id)))
                if mid:
                    ms_rows = await session.execute(
                        select(MasterService.service_id, MasterService.duration_minutes).where(
                            MasterService.master_id == int(mid),
                            MasterService.service_id.in_(list(service_ids)),
                        )
                    )
                else:
                    ms_rows = []
                for sid, dur in ms_rows.all():
                    try:
                        if dur and int(dur) > 0:
                            overrides[str(sid)] = int(dur)
                    except Exception:
                        continue

            for sid in service_ids:
                svc = services.get(str(sid))
                if svc:
                    if isinstance(getattr(svc, "price_cents", None), int):
                        total_price += int(svc.price_cents or 0)
                    # Per-service currency column is ignored; global env/default is authoritative
                    pass
                if str(sid) in overrides:
                    dur = overrides[str(sid)]
                else:
                    if svc:
                        try:
                            dur = int(getattr(svc, "duration_minutes", 0) or 0)
                        except Exception:
                            dur = 0
                    else:
                        dur = 0
                total_minutes += dur if dur > 0 else 60
        if online_payment and total_price > 0:
            try:
                # Allow override of the discount percent via SettingsRepo; fall back to module default.
                dp = await SettingsRepo.get_setting(ONLINE_PAYMENT_DISCOUNT_SETTING, ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT)
                try:
                    discount_percent = float(dp)
                except Exception:
                    discount_percent = float(ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT)
                multiplier = max(0.0, 1.0 - (discount_percent / 100.0))
                total_price = int(total_price * multiplier)
            except Exception:
                # On any error, fall back to the previous fixed 5% discount multiplier
                total_price = int(total_price * (1.0 - (ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT / 100.0)))
        return {"total_minutes": total_minutes, "total_price_cents": total_price, "currency": currency}
    except Exception as e:
        logger.warning("Ошибка расчета суммы длительности/цены для %s: %s", service_ids, e)
        return {"total_minutes": total_minutes, "total_price_cents": 0, "currency": currency}


## Removed: legacy pagination wrappers — callers should use BookingRepo.get_paginated_list.


# render_bookings_page removed — use shared_services.render_bookings_list_page(role, user_id, mode, page, lang)


async def create_composite_booking(client_id: int, master_id: int, service_ids: Sequence[str], slot: datetime, *, hold_minutes: int | None = None) -> Booking:
    """Create a booking with multiple services snapshot into booking_items and total price snapshot on Booking.

    The Booking.service_id will be set to the first service id for backward compatibility; detailed list stored in BookingItem rows.
    """
    if not service_ids:
        raise ValueError("service_ids must not be empty")
    try:
        totals = await get_services_duration_and_price(service_ids, online_payment=False, master_id=master_id)
        async with get_session() as session:
            # Compute proposed interval (use aggregated duration; exclusion constraints or commit will enforce uniqueness)
            new_dur = int(totals.get("total_minutes") or 0) or await SettingsRepo.get_slot_duration()
            new_start = slot
            new_end = new_start + timedelta(minutes=new_dur)

            # Acquire advisory lock for this (master_id, starts_at) pair to
            # reduce race with expiration worker and other creators.
            try:
                from sqlalchemy import text
                k1 = int(master_id) % 2147483647
                k2 = int(new_start.timestamp()) % 2147483647
                await session.execute(text("SELECT pg_advisory_xact_lock(:k1, :k2)"), {"k1": k1, "k2": k2})
            except Exception:
                # Best-effort only.
                pass

            # Optional lightweight locking: lock existing potentially overlapping rows to reduce race window.
            try:
                from bot.app.domain.models import Booking as _B
                await session.execute(
                    select(_B.id).where(
                        _B.master_id == master_id,
                        _B.starts_at < new_end,
                        _B.ends_at > new_start,
                    ).with_for_update()
                )
                await session.execute(
                    select(_B.id).where(
                        _B.user_id == client_id,
                        _B.starts_at < new_end,
                        _B.ends_at > new_start,
                    ).with_for_update()
                )
            except Exception as e:
                logger.exception("Row-level locking failed while creating composite booking: %s", e)
                if REQUIRE_ROW_LOCK_STRICT:
                    raise

            price_cents = int(totals.get("total_price_cents", 0) or 0) or None
            booking = await _create_booking_base(session, client_id, master_id, slot, price_cents=price_cents, hold_minutes=hold_minutes, service_id=str(service_ids[0]), duration_minutes=new_dur)
            await session.flush()
            # Add items with per-item price snapshot. Load current service prices
            svc_rows = await session.execute(select(Service.id, Service.price_cents).where(Service.id.in_(list(service_ids))))
            svc_map = {str(r[0]): int(r[1] or 0) for r in svc_rows.all()}
            pos = 0
            for sid in service_ids:
                item_price = svc_map.get(str(sid), 0)
                session.add(BookingItem(booking_id=booking.id, service_id=str(sid), price_cents=item_price, position=pos))
                pos += 1
            try:
                await session.commit()
            except IntegrityError as ie:
                await session.rollback()
                # Map constraint violation to user-friendly codes (generic fallback)
                msg = str(ie).lower()
                if "client" in msg:
                    raise ValueError("client_already_has_booking_at_this_time") from ie
                raise ValueError("slot_unavailable") from ie
            await session.refresh(booking)
            logger.info("Создана композитная запись №%s: client=%s master=%s services=%s", booking.id, client_id, master_id, list(service_ids))
            return booking
    except SQLAlchemyError as e:
        logger.error("Ошибка создания композитной записи: client_id=%s, master_id=%s, services=%s, slot=%s, error=%s", client_id, master_id, service_ids, slot, e)
        raise


async def book_slot(
    user_telegram_id: int,
    master_id: int,
    service_id: str,
    date_str: str,
    time_compact: str,
    locale: str,
) -> dict[str, Any]:
    """High-level booking creator invoked by the time selection handler.

    Consolidates parsing, user creation, single vs composite booking logic and
    pending-payment transition. Returns a dict:
      { 'ok': bool, 'error': <code>|None, 'booking': Booking|None,
        'service_name': str|None, 'master_name': str|None, 'date': str|None }

    Error codes map to translation keys or fallbacks:
      - client_already_has_booking_at_this_time
      - slot_unavailable
      - booking_failed (generic fallback)
      - invalid_data (parsing failures)
    """
    try:
        # Parse time HHMM -> HH:MM
        if not time_compact or len(time_compact) < 3:
            return {"ok": False, "error": "invalid_data"}
        hh = time_compact[:2]
        mm = time_compact[2:]
        time_str = f"{hh}:{mm}"
        try:
            local_dt = datetime.fromisoformat(f"{date_str}T{time_str}")
        except ValueError:
            return {"ok": False, "error": "invalid_data"}
        # Use the same local timezone as display/rendering to avoid shifts
        # Resolve business/local timezone at runtime
        biz_tz = get_local_tz() or UTC
        local_dt = local_dt.replace(tzinfo=biz_tz)
        slot_dt = local_dt.astimezone(UTC)

        # Ensure user exists (call repo directly)
        user_full_name = None
        booking_user = await UserRepo.get_or_create(user_telegram_id, name=user_full_name)

        # Short initial hold to reduce zombie reservations before payment
        try:
            short_hold_value = await SettingsRepo.get_reservation_hold_minutes()
            short_hold = min(int(short_hold_value or 5), 3)
        except Exception:
            short_hold = 3

        composite = "+" in str(service_id)
        if composite:
            ids = [s for s in str(service_id).split("+") if s]
            if not ids:
                return {"ok": False, "error": "invalid_data"}
        # Resolve booking creation
        try:
            if composite:
                booking = await create_composite_booking(booking_user.id, int(master_id), ids, slot_dt, hold_minutes=short_hold)
                names = [await ServiceRepo.get_service_name(sid) for sid in ids]
                service_name = " + ".join([n for n in names if n])
            else:
                booking = await create_booking(booking_user.id, int(master_id), service_id, slot_dt, hold_minutes=short_hold)
                # Try to load name from repo (state caching handled in handler side earlier)
                service_name = await ServiceRepo.get_service_name(service_id)
        except ValueError as ve:
            code = str(ve) or "booking_failed"
            if code not in {"client_already_has_booking_at_this_time", "slot_unavailable"}:
                code = "booking_failed"
            return {"ok": False, "error": code}

        # Transition to PENDING_PAYMENT optimistically
        try:
            await BookingRepo.set_pending_payment(int(getattr(booking, "id", 0)))
        except Exception:
            pass

        master_name = await MasterRepo.get_master_name(int(master_id))
        try:
            formatted_date = format_date(datetime.fromisoformat(date_str), "%d.%m.%Y")
        except Exception:
            formatted_date = date_str

        return {
            "ok": True,
            "error": None,
            "booking": booking,
            "service_name": service_name,
            "master_name": master_name,
            "date": formatted_date,
        }
    except Exception as e:
        logger.exception("book_slot failed: user=%s master=%s service=%s date=%s time=%s error=%s", user_telegram_id, master_id, service_id, date_str, time_compact, e)
        return {"ok": False, "error": "booking_failed"}

async def get_client_active_bookings(user_id: int) -> List[Booking]:
    """Возвращает активные и будущие записи клиента.

    Args:
        user_id: ID клиента.

    Returns:
        Список объектов Booking.
    """
    try:
        bookings = await BookingRepo.list_active_by_user(int(user_id))
        logger.info("Получено %d активных записей для клиента %s", len(bookings), user_id)
        return bookings
    except Exception as e:
        logger.error("Ошибка получения активных записей для клиента %s: %s", user_id, e)
        return []


async def _create_booking_base(
    session,
    client_id: int,
    master_id: int,
    slot: datetime,
    *,
    price_cents: int | None = None,
    hold_minutes: int | None = None,
    service_id: str | None = None,
    duration_minutes: int | None = None,
) -> Booking:
    """Internal helper: populate Booking object, add to session, but do not commit outer changes.

    The caller is responsible for committing if needed. This centralizes setting created_at,
    cash_hold_expires_at, and price snapshot fields.
    """
    booking = Booking(
        user_id=client_id,
        master_id=master_id,
        starts_at=slot,
        status=BookingStatus.RESERVED,
        created_at=utc_now(),
    )
    try:
        if price_cents is not None and price_cents > 0:
            booking.original_price_cents = int(price_cents)
            booking.final_price_cents = int(price_cents)
    except Exception:
        pass
    _hold = hold_minutes if hold_minutes is not None else await SettingsRepo.get_reservation_hold_minutes()
    booking.cash_hold_expires_at = utc_now() + timedelta(minutes=max(1, _hold))
    # Determine and set ends_at for exclusion constraint correctness.
    try:
        # Prefer explicit caller-provided duration; otherwise resolve via helper
        if duration_minutes and int(duration_minutes) > 0:
            duration_min = int(duration_minutes)
        else:
            duration_min = await get_service_duration(session, service_id, master_id)
        booking.ends_at = slot + timedelta(minutes=int(duration_min))
    except Exception:
        # best-effort: if we can't compute ends_at, leave it None and rely
        # on DB-level checks (commit may fail). Prefer not to crash here.
        pass
    session.add(booking)
    return booking


async def calculate_price(service_id: str, online_payment: bool) -> Dict[str, Any]:
    """Рассчитывает стоимость услуги с учетом скидки за онлайн-оплату.

    Args:
        service_id: ID услуги.
        online_payment: Флаг онлайн-оплаты.

    Returns:
        Словарь с final_price_cents и currency.
    """
    try:
        async with get_session() as session:
            service = await session.get(Service, service_id)
            if not service or service.price_cents is None:
                logger.warning("Услуга %s не найдена или цена отсутствует", service_id)
                # Use global currency (single source of truth)
                from bot.app.services.shared_services import _default_currency
                try:
                    from bot.app.services.admin_services import SettingsRepo

                    try:
                        cur = await SettingsRepo.get_currency()
                    except Exception:
                        cur = _default_currency()
                except Exception:
                    cur = _default_currency()
                return {"final_price_cents": 0, "currency": cur}

            price = service.price_cents
            if online_payment:
                try:
                    dp = await SettingsRepo.get_setting(ONLINE_PAYMENT_DISCOUNT_SETTING, ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT)
                    try:
                        discount_percent = float(dp)
                    except Exception:
                        discount_percent = float(ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT)
                    multiplier = max(0.0, 1.0 - (discount_percent / 100.0))
                    price = int(price * multiplier)
                except Exception:
                    # conservative fallback to default 5% discount
                    price = int(price * (1.0 - (ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT / 100.0)))
            # Resolve global currency once (avoid per-service currency fields).
            from bot.app.services.shared_services import _default_currency
            try:
                from bot.app.services.admin_services import SettingsRepo

                try:
                    cur = await SettingsRepo.get_currency()
                except Exception:
                    cur = _default_currency()
            except Exception:
                cur = _default_currency()
            result = {"final_price_cents": price, "currency": cur}
            logger.debug("Рассчитана цена для услуги %s (онлайн=%s): %s", service_id, online_payment, result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета цены для услуги %s: %s", service_id, e)
        try:
            from bot.app.services.shared_services import get_global_currency

            cur = await get_global_currency()
        except Exception:
            from bot.app.services.shared_services import _default_currency

            cur = _default_currency()
        return {"final_price_cents": 0, "currency": cur}

async def process_successful_payment(booking_id: int, charge_id: str) -> tuple[bool, str | None]:
    """Обрабатывает успешный платеж онлайн с повторной проверкой слота.

    Возвращает (ok, error_code) аналогично mark_paid.
    """
    try:
        ok, reason = await BookingRepo.mark_paid(booking_id)
        if ok:
            try:
                async with get_session() as session:
                    booking = await session.get(Booking, booking_id)
                    if booking:
                        try:
                            booking.payment_id = charge_id
                            booking.payment_provider = "online"
                            await session.commit()
                        except Exception:
                            await session.rollback()
            except Exception:
                logger.exception("process_successful_payment: failed to persist payment metadata for %s", booking_id)
            logger.info("Платеж обработан для записи №%s, charge_id=%s", booking_id, charge_id)
        return ok, reason
    except SQLAlchemyError as e:
        logger.error("Ошибка обработки платежа для записи №%s: %s", booking_id, e)
        return False, "payment_failed"

# ---------------- Client-side booking action guards -----------------

async def cancel_client_booking(booking_id: int, user_telegram_id: int, *, bot=None, lang: str | None = None) -> tuple[bool, str, dict]:
    """Cancel a client's booking with business checks centralized.

    Returns (ok, message_key, params). On success notifications are sent when bot provided.
    """
    try:
        # Resolve internal user and ensure ownership (call repo directly)
        user = await UserRepo.get_or_create(user_telegram_id, name=None)
        b = await BookingRepo.ensure_owner(int(user.id), int(booking_id))
        if not b:
            return False, "booking_not_found", {}

        # Terminal/expired checks
        status_val = getattr(b, "status", None)
        status_enum = normalize_booking_status(status_val)
        if status_enum in TERMINAL_STATUSES:
            return False, "booking_not_active", {}

        starts_at = getattr(b, "starts_at", None)
        if starts_at and starts_at <= utc_now():
            return False, "cannot_cancel_past", {}

        # Lock window
        try:
            lock_m = int(await SettingsRepo.get_client_cancel_lock_minutes())
        except Exception:
            lock_m = 60
        if starts_at and (starts_at - utc_now()).total_seconds() < lock_m * 60:
            return False, "cancel_too_close", {"minutes": lock_m}

        # Perform cancellation
        ok = await BookingRepo.set_cancelled(int(booking_id))
        if not ok:
            return False, "error_retry", {}

        # Optional notifications: only include master recipient when present
        if bot and b:
            try:
                from bot.app.services.shared_services import get_admin_ids
                recipients: list[int] = []
                master_rec = getattr(b, "master_id", None)
                if master_rec is not None:
                    try:
                        recipients.append(int(master_rec))
                    except Exception:
                        # ignore invalid master id
                        pass
                recipients.extend(get_admin_ids())
                # Only send if we have at least one valid recipient
                if recipients:
                    from bot.app.core.notifications import send_booking_notification

                    await send_booking_notification(bot, int(booking_id), "cancelled", recipients)
            except Exception:
                logger.exception("cancel_client_booking: notification failed for %s", booking_id)
        return True, "booking_cancelled_success", {}
    except Exception:
        logger.exception("cancel_client_booking failed for booking=%s user=%s", booking_id, user_telegram_id)
        return False, "error_retry", {}


async def can_client_reschedule(booking_id: int, user_telegram_id: int) -> tuple[bool, str | None]:
    """Return whether the user can start rescheduling the booking.

    Only centralizes permission/lock checks; actual calendar assembly remains in handler.
    """
    try:
        user = await UserRepo.get_or_create(user_telegram_id, name=None)
        b = await BookingRepo.ensure_owner(int(user.id), int(booking_id))
        if not b:
            return False, "booking_not_found"
        status_enum = normalize_booking_status(getattr(b, "status", None))
        if status_enum in TERMINAL_STATUSES:
            return False, "booking_not_active"
        # Reschedule lock window
        try:
            lock_m = int(await SettingsRepo.get_client_reschedule_lock_minutes())
        except Exception:
            lock_m = 180
        starts_at = getattr(b, "starts_at", None)
        if starts_at and (starts_at - utc_now()).total_seconds() < lock_m * 60:
            return False, "reschedule_too_close"
        return True, None
    except Exception:
        logger.exception("can_client_reschedule failed for booking=%s user=%s", booking_id, user_telegram_id)
        return False, "error_retry"

async def record_booking_rating(booking_id: int, rating: int) -> Dict[str, Any]:
    """Записывает оценку для завершенной записи.

    Args:
        booking_id: ID записи.
        rating: Оценка (1-5).

    Returns:
        Словарь с результатом операции (status: ok/invalid/not_found/not_done/already).
    """
    try:
        if not (1 <= rating <= 5):
            logger.warning("Недопустимая оценка для записи №%s: %s", booking_id, rating)
            return {"status": "invalid"}

        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not booking:
                logger.warning("Запись не найдена для оценки: id=%s", booking_id)
                return {"status": "not_found"}
            if booking.status != BookingStatus.DONE:
                logger.warning("Запись №%s не завершена, оценка невозможна", booking_id)
                return {"status": "not_done"}
            if await session.scalar(select(BookingRating).where(BookingRating.booking_id == booking_id)):
                logger.warning("Оценка для записи №%s уже существует", booking_id)
                return {"status": "already"}

            new_rating = BookingRating(booking_id=booking.id, rating=rating)
            session.add(new_rating)
            await session.commit()
            logger.info("Оценка %d записана для записи #%s", rating, booking_id)
            return {"status": "ok"}
    except SQLAlchemyError as e:
        logger.error("Ошибка записи оценки для записи #%s: %s", booking_id, e)
        return {"status": "error"}


# ---------------- WebApp-facing processors -----------------

async def process_booking_hold(
    user_id: int,
    user_telegram_id: int,
    service_ids: Sequence[str],
    slot: datetime,
    *,
    master_id: int | None,
    payment_method: str | None = None,
    client_name: str | None = None,
    client_username: str | None = None,
) -> BookingResult:
    """Create a short-term hold booking while keeping app layer thin."""
    try:
        user = await UserRepo.get_by_id(user_id)
        if not user:
            user = await UserRepo.get_or_create(
                telegram_id=user_telegram_id,
                name=client_name,
                username=client_username,
            )
    except Exception as exc:
        logger.exception("process_booking_hold: failed to resolve user: %s", exc)
        return {"ok": False, "error": "booking_failed", "booking_id": None}

    try:
        hold_minutes = int(await SettingsRepo.get_reservation_hold_minutes())
    except Exception as exc:
        logger.exception("process_booking_hold: failed to read hold minutes: %s", exc)
        hold_minutes = 5

    slot_val = slot
    if slot_val.tzinfo is None:
        try:
            local_tz = get_local_tz() or UTC
        except Exception as exc:
            logger.exception("process_booking_hold: failed to resolve local timezone: %s", exc)
            local_tz = UTC
        slot_val = slot_val.replace(tzinfo=local_tz).astimezone(UTC)
    else:
        slot_val = slot_val.astimezone(UTC)

    master_val = master_id
    try:
        if master_val is not None:
            master_val = int(master_val)
            if master_val < 0:
                master_val = None
    except Exception as exc:
        logger.exception("process_booking_hold: invalid master_id %s: %s", master_id, exc)
        master_val = None

    if master_val is None:
        return {"ok": False, "error": "no_master_available", "booking_id": None}

    pay_method = payment_method or "cash"

    try:
        base_totals = await get_services_duration_and_price(service_ids, online_payment=False, master_id=master_val)
        base_price = int(base_totals.get("total_price_cents") or 0)
        currency = str(base_totals.get("currency") or "UAH")
        final_price = base_price
        discount_amount = 0
        if pay_method == "online":
            online_totals = await get_services_duration_and_price(service_ids, online_payment=True, master_id=master_val)
            final_price = int(online_totals.get("total_price_cents") or base_price)
            currency = str(online_totals.get("currency") or currency)
            discount_amount = base_price - final_price if base_price > final_price else 0
    except Exception as exc:
        logger.exception("process_booking_hold: pricing failed for services %s: %s", service_ids, exc)
        base_price = 0
        final_price = 0
        discount_amount = 0
        currency = "UAH"

    try:
        if len(service_ids) == 1:
            booking = await create_booking(
                client_id=int(user.id),
                master_id=master_val,
                service_id=service_ids[0],
                slot=slot_val,
                hold_minutes=hold_minutes,
            )
        else:
            booking = await create_composite_booking(
                client_id=int(user.id),
                master_id=master_val,
                service_ids=service_ids,
                slot=slot_val,
                hold_minutes=hold_minutes,
            )
    except ValueError as exc:
        return {"ok": False, "error": normalize_error_code(exc, "booking_failed"), "booking_id": None}
    except Exception as exc:
        logger.exception("process_booking_hold: booking creation failed: %s", exc)
        return {"ok": False, "error": "booking_failed", "booking_id": None}

    booking_id = int(getattr(booking, "id", 0) or 0)
    starts_at_val = getattr(booking, "starts_at", None)
    cash_hold_expires = getattr(booking, "cash_hold_expires_at", None)

    return {
        "ok": True,
        "booking_id": booking_id,
        "status": str(getattr(booking, "status", "")),
        "starts_at": starts_at_val.isoformat() if starts_at_val else None,
        "cash_hold_expires_at": cash_hold_expires.isoformat() if cash_hold_expires else None,
        "original_price_cents": base_price,
        "final_price_cents": final_price,
        "discount_amount_cents": discount_amount,
        "currency": currency,
        "duration_minutes": getattr(booking, "duration_minutes", None),
        "master_id": master_val,
        "payment_method": pay_method,
        "error": None,
    }

async def process_booking_rating(user_id: int, booking_id: int, rating: int) -> BookingResult:
    """Thin service wrapper for WebApp rating submissions."""
    booking = await BookingRepo.ensure_owner(user_id, booking_id)
    if not booking:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}

    res = await record_booking_rating(booking_id, rating)
    status = res.get("status") if isinstance(res, dict) else None
    ok = status in {None, "ok"}
    return {
        "ok": ok,
        "booking_id": booking_id if ok else None,
        "error": None if ok else (status or "rating_failed"),
    }


async def process_booking_cancellation(user_id: int, user_telegram_id: int, booking_id: int) -> BookingResult:
    """Cancel booking with ownership/status handling hidden behind the service layer."""
    booking = await BookingRepo.ensure_owner(user_id, booking_id)
    if not booking:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}

    status = normalize_booking_status(getattr(booking, "status", None))
    if status in {BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT}:
        deleted = await BookingRepo.delete_booking(booking_id)
        if deleted:
            return {"ok": True, "booking_id": booking_id, "status": BookingStatus.CANCELLED.value}
        return {"ok": False, "error": "cancel_failed", "booking_id": None}

    ok, code, _ = await cancel_client_booking(booking_id, user_telegram_id)
    return {
        "ok": ok,
        "booking_id": booking_id if ok else None,
        "status": BookingStatus.CANCELLED.value if ok else None,
        "error": None if ok else code,
    }


async def process_booking_reschedule(
    user_id: int,
    user_telegram_id: int,
    booking_id: int,
    new_slot: datetime,
    *,
    language: str | None = None,
) -> BookingResult:
    """Handle reschedule flow for WebApp (permissions, ownership, notifications)."""
    can_res, code = await can_client_reschedule(booking_id, user_telegram_id)
    if not can_res:
        return {"ok": False, "error": code or "reschedule_not_allowed", "booking_id": None}

    booking = await BookingRepo.ensure_owner(user_id, booking_id)
    if not booking:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}

    slot = new_slot
    if slot.tzinfo is None:
        try:
            local_tz = get_local_tz() or UTC
        except Exception:
            local_tz = UTC
        slot = slot.replace(tzinfo=local_tz).astimezone(UTC)
    else:
        slot = slot.astimezone(UTC)

    ok = await BookingRepo.reschedule(booking_id, slot)
    if ok:
        await _send_reschedule_notifications(booking_id, user_id, user_telegram_id, language)
    return {
        "ok": ok,
        "booking_id": booking_id if ok else None,
        "error": None if ok else "reschedule_failed",
    }


async def _send_reschedule_notifications(booking_id: int, user_id: int, user_telegram_id: int, lang: str | None) -> None:
    """Notify master/admins and client about successful reschedule (best-effort)."""
    try:
        recipients: list[int] = []
        try:
            master_rec = getattr(await BookingRepo.ensure_owner(user_id, booking_id), "master_id", None)
        except Exception:
            master_rec = None
        if master_rec is not None:
            try:
                recipients.append(int(master_rec))
            except Exception:
                pass
        try:
            recipients.extend(get_admin_ids())
        except Exception:
            pass

        if recipients:
            bot = Bot(BOT_TOKEN)
            try:
                await send_booking_notification(bot, booking_id, "rescheduled_by_client", recipients)
            except Exception:
                logger.exception("reschedule: send_booking_notification failed for %s", booking_id)

            try:
                lang_resolved = lang if lang else await safe_get_locale(user_telegram_id)
            except Exception:
                lang_resolved = lang

            try:
                bd = await build_booking_details(await BookingRepo.ensure_owner(user_id, booking_id), user_id=user_telegram_id, lang=lang_resolved)
                body = format_booking_details_text(bd, lang=lang_resolved)
                await bot.send_message(chat_id=user_telegram_id, text=body, parse_mode="HTML")
            except Exception:
                logger.exception("Failed to send client confirmation after reschedule for %s", booking_id)

            try:
                await bot.session.close()
            except Exception:
                pass
    except Exception:
        logger.exception("reschedule: notification block failed for %s", booking_id)


async def process_booking_finalization(user_id: int, user_telegram_id: int, booking_id: int, payment_method: str) -> BookingResult:
    """Finalize held booking including payments and notifications."""
    booking = await BookingRepo.ensure_owner(user_id, booking_id)
    if not booking:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}

    payment_method = payment_method or "cash"

    if payment_method == "online":
        pending_ok = await BookingRepo.set_pending_payment(booking_id)
        if not pending_ok:
            return {"ok": False, "error": "finalize_failed", "booking_id": None}

        amt = getattr(booking, "final_price_cents", None) or getattr(booking, "original_price_cents", None)
        try:
            amount_cents = int(amt) if amt is not None else 0
        except Exception:
            amount_cents = 0
        if amount_cents <= 0:
            return {"ok": False, "error": "invalid_amount", "booking_id": None}

        invoice_url = None
        bot = Bot(BOT_TOKEN)
        try:
            from aiogram.types import LabeledPrice

            prices = [LabeledPrice(label=f"Booking #{booking_id}", amount=int(amount_cents))]
        except Exception:
            prices = []

        currency = getattr(booking, "currency", None) or "USD"
        try:
            provider_token = TELEGRAM_PROVIDER_TOKEN or None
            link = await bot.create_invoice_link(
                title=f"Booking #{booking_id}",
                description=f"Payment for booking {booking_id}",
                payload=f"booking_{booking_id}",
                provider_token=provider_token,
                currency=str(currency).upper(),
                prices=prices,
            )
            invoice_url = str(link) if link is not None else None
        except Exception:
            logger.exception("create_invoice_link failed for booking=%s", booking_id)
            try:
                await bot.session.close()
            except Exception:
                pass
            return {"ok": False, "error": "invoice_failed", "booking_id": None}

        try:
            await bot.session.close()
        except Exception:
            pass

        return {"ok": True, "booking_id": booking_id, "status": "pending_payment", "invoice_url": invoice_url}

    ok, err = await BookingRepo.confirm_cash(booking_id)
    if not ok:
        return {"ok": False, "error": err or "finalize_failed", "booking_id": None}

    await _notify_after_cash_confirmation(booking_id, user_telegram_id)
    starts_at = getattr(booking, "starts_at", None)
    return {
        "ok": True,
        "booking_id": booking_id,
        "status": str(getattr(booking, "status", "")),
        "starts_at": starts_at.isoformat() if starts_at else None,
    }


async def _notify_after_cash_confirmation(booking_id: int, client_tid: int) -> None:
    """Send cash confirmation notifications (best-effort)."""
    try:
        booking = await BookingRepo.get(booking_id)
        recipients: list[int] = []
        master_rec = getattr(booking, "master_id", None) if booking else None
        if master_rec is not None:
            try:
                recipients.append(int(master_rec))
            except Exception:
                pass
        try:
            recipients.extend(get_admin_ids())
        except Exception:
            pass
        if not recipients:
            return
        bot = Bot(BOT_TOKEN)
        try:
            await send_booking_notification(bot, booking_id, "cash_confirmed", recipients)
        except Exception:
            logger.exception("finalize: notification failed for booking=%s", booking_id)

        try:
            lang = await safe_get_locale(client_tid)
        except Exception:
            lang = None

        try:
            bd = await build_booking_details(booking, user_id=client_tid, lang=lang)
            body = format_booking_details_text(bd, lang=lang)
            await bot.send_message(chat_id=client_tid, text=body, parse_mode="HTML")
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
    except Exception:
        logger.exception("finalize: notification block failed for booking=%s", booking_id)


async def process_invoice_link(user_id: int, booking_id: int) -> BookingResult:
    """Create invoice link for an owned booking (WebApp)."""
    b = await BookingRepo.ensure_owner(user_id, booking_id)
    if not b:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}

    amt = getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", None)
    try:
        amount_cents = int(amt) if amt is not None else 0
    except Exception:
        amount_cents = 0
    if amount_cents <= 0:
        return {"ok": False, "error": "invalid_amount", "booking_id": None}

    try:
        from aiogram.types import LabeledPrice
    except Exception:
        LabeledPrice = None

    prices = []
    if LabeledPrice is not None:
        try:
            prices = [LabeledPrice(label=f"Booking #{booking_id}", amount=int(amount_cents))]
        except Exception:
            prices = []

    currency = getattr(b, "currency", None) or "USD"
    bot = Bot(BOT_TOKEN)
    try:
        provider_token = TELEGRAM_PROVIDER_TOKEN or None
        link = await bot.create_invoice_link(
            title=f"Booking #{booking_id}",
            description=f"Payment for booking {booking_id}",
            payload=f"booking_{booking_id}",
            provider_token=provider_token,
            currency=str(currency).upper(),
            prices=prices,
        )
        invoice_url = str(link) if link is not None else None
    except Exception:
        logger.exception("create_invoice_link failed for booking=%s", booking_id)
        try:
            await bot.session.close()
        except Exception:
            pass
        return {"ok": False, "error": "invoice_failed", "booking_id": None}

    try:
        await bot.session.close()
    except Exception:
        pass

    return {"ok": True, "booking_id": booking_id, "invoice_url": invoice_url}


async def process_booking_details(user_id: int, booking_id: int) -> BookingResult:
    """Return booking details text for owned booking (WebApp)."""
    b = await BookingRepo.ensure_owner(user_id, booking_id)
    if not b:
        return {"ok": False, "error": "booking_not_found", "booking_id": None}
    try:
        text = format_booking_details_text(b, role="client")
        return {"ok": True, "booking_id": booking_id, "text": text}
    except Exception as exc:
        logger.exception("process_booking_details failed: %s", exc)
        return {"ok": False, "error": "details_failed", "booking_id": None}


# Public exports for improved import clarity
__all__ = [
    "BookingRepo",
    "BookingResult",
    "is_booking_slot_blocked",
    "process_booking_hold",
    "process_booking_rating",
    "process_booking_cancellation",
    "process_booking_reschedule",
    "process_booking_finalization",
    "process_invoice_link",
    "process_booking_details",
]
