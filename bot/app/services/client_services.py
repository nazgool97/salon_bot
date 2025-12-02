from __future__ import annotations
import logging
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, UTC
from typing import Any, Dict, Iterable, List, Sequence

from sqlalchemy import select, and_, func, or_, String

from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from bot.app.domain.models import (
    Booking,
    BookingStatus,
    Master,
    Service,
    ServiceProfile,
    User,
    BookingRating,
    MasterProfile,
    BookingItem,
    normalize_booking_status,
    TERMINAL_STATUSES,
    ACTIVE_STATUSES,
)
import asyncio
from bot.app.core.db import get_session
from bot.app.services import master_services
from bot.app.services.master_services import MasterRepo

from zoneinfo import ZoneInfo
from aiogram import Bot
from bot.app.services.shared_services import (
    LOCAL_TZ,
    BookingInfo,
    booking_info_from_mapping,
    format_money_cents,
    status_to_emoji,
    safe_get_locale,
    default_language,
    format_booking_list_item,
    format_booking_details_text,
    format_date,
)
from bot.app.services.admin_services import SettingsRepo
from bot.app.services.admin_services import ServiceRepo
from bot.app.telegram.common.status import ACTIVE_BLOCKING_STATUSES

logger = logging.getLogger(__name__)


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


async def calculate_booking_permissions(obj: dict | Any, lock_r_hours: int | None = None, lock_c_hours: int | None = None, settings: Any | None = None) -> tuple[bool, bool]:
    """Calculate (can_cancel, can_reschedule) for a booking-like object.

    Args:
        obj: mapping or object with a `starts_at` and optional `status`.
        settings: optional object providing `get_client_reschedule_lock_hours` and
            `get_client_cancel_lock_hours` callables; falls back to `SettingsRepo`.
    Returns:
        (can_cancel, can_reschedule)
    """
    can_cancel = False
    can_reschedule = False
    try:
        starts_at_dt = obj.get("starts_at") if isinstance(obj, dict) else getattr(obj, "starts_at", None)
        if starts_at_dt:
            now_utc = datetime.now(UTC)
            try:
                starts_utc = starts_at_dt.astimezone(UTC)
            except Exception:
                starts_utc = starts_at_dt
            delta_seconds = (starts_utc - now_utc).total_seconds()
            # resolve lock settings: explicit args take precedence, then provided
            # settings object, then SettingsRepo, then default 3 hours
            if lock_r_hours is not None:
                lock_r = lock_r_hours
            else:
                try:
                    if settings and hasattr(settings, "get_client_reschedule_lock_hours"):
                        lock_r = settings.get_client_reschedule_lock_hours()
                        if asyncio.iscoroutine(lock_r):
                            lock_r = await lock_r
                    else:
                        lock_r = await SettingsRepo.get_client_reschedule_lock_hours()
                except Exception:
                    lock_r = 3

            if lock_c_hours is not None:
                lock_c = lock_c_hours
            else:
                try:
                    if settings and hasattr(settings, "get_client_cancel_lock_hours"):
                        lock_c = settings.get_client_cancel_lock_hours()
                        if asyncio.iscoroutine(lock_c):
                            lock_c = await lock_c
                    else:
                        lock_c = await SettingsRepo.get_client_cancel_lock_hours()
                except Exception:
                    lock_c = 3
            can_reschedule = delta_seconds >= (lock_r * 3600)
            can_cancel = delta_seconds >= (lock_c * 3600)
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
    def __init__(self, id: str, name: str, duration_minutes: int | None = None, price_cents: int | None = None, currency: str | None = None):
        self.id = id
        self.name = name
        self.duration_minutes = duration_minutes
        self.price_cents = price_cents
        self.currency = currency


async def get_filtered_services() -> list[ServiceDTO]:
    """Return list of ServiceDTO for services that have at least one master.

    Each DTO contains basic display fields so handlers can avoid extra
    DB roundtrips when building menus.
    """
    out: list[ServiceDTO] = []
    try:
        from bot.app.core.db import get_session
        from bot.app.domain.models import Service, ServiceProfile, MasterService
        from sqlalchemy import select, join, outerjoin
        async with get_session() as session:
            # Join Service <- MasterService to ensure only services that have at least
            # one master are returned, and left-outer-join to ServiceProfile to
            # fetch duration metadata in the same query. Use GROUP BY to avoid
            # DISTINCT + ORDER BY portability issues across DB engines.
            # Build FROM/JOINs using join()/outerjoin() to avoid overwriting
            # the FROM clause when calling select_from() multiple times.
            stmt = (
                select(
                    Service.id,
                    Service.name,
                    ServiceProfile.duration_minutes,
                    Service.price_cents,
                    Service.currency,
                )
                .join(MasterService, MasterService.service_id == Service.id)
                .outerjoin(ServiceProfile, ServiceProfile.service_id == Service.id)
                .group_by(Service.id, Service.name, ServiceProfile.duration_minutes, Service.price_cents, Service.currency)
            )
            rows = (await session.execute(stmt)).all()
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
                cur = r[4] if r[4] is not None else None
                out.append(ServiceDTO(id=sid, name=name, duration_minutes=dur, price_cents=pc, currency=cur))
            return out
    except Exception:
        return []

    # (Duplicate legacy definition removed during consolidation.)


def format_booking_details_text(data: dict | Any, lang: str | None = None, role: str = "client") -> str:
    """Thin wrapper that delegates to shared formatter (single source of truth)."""
    from bot.app.services.shared_services import format_booking_details_text as _fmt
    return _fmt(data, lang, role)


# --- Booking presentation and list helpers moved from shared_services ---
from dataclasses import dataclass
from typing import Sequence, Any


@dataclass
class BookingDetails:
    booking_id: int
    service_name: str | None = None
    master_name: str | None = None
    price_cents: int = 0
    currency: str = "UAH"
    starts_at: datetime | None = None
    date_str: str | None = None
    client_id: int | None = None
    raw: Any | None = None
    status: str | None = None
    client_name: str | None = None
    client_phone: str | None = None
    client_telegram_id: int | None = None
    client_username: str | None = None
    can_cancel: bool = False
    can_reschedule: bool = False

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
    try:
        from bot.app.translations import tr as _tr
        months = _tr("month_names_full", lang=lang)
        if isinstance(months, list) and len(months) >= month:
            return f"{months[month - 1]} {year}"
    except Exception:
        pass
    # fallback abbreviated month names reused from keyboard module
    fallback = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    try:
        return f"{fallback[month - 1]} {year}"
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
        from bot.app.domain.models import Booking, BookingItem, ServiceProfile, BookingStatus

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
            if int(getattr(b, "id", 0)) not in booking_service_map:
                try:
                    booking_service_map[int(b.id)] = [str(getattr(b, "service_id"))]
                except Exception:
                    booking_service_map[int(b.id)] = []

        svc_ids = {sid for sids in booking_service_map.values() for sid in sids if sid}
        svc_durations: dict[str, int] = {}
        if svc_ids:
            prof_rows = (await session.execute(select(ServiceProfile).where(ServiceProfile.service_id.in_(list(svc_ids))))).scalars().all()
            for p in prof_rows:
                try:
                    svc_durations[str(p.service_id)] = int(getattr(p, "duration_minutes", 0) or 0)
                except Exception:
                    svc_durations[str(p.service_id)] = 0

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
        """Return bookings that overlap configured windows (by master and datetime range)."""
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
            from bot.app.domain.models import Booking
            booking = await session.get(Booking, booking_id)
            if not booking:
                return False
            booking.status = new_status
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
    async def confirm_cash(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus
            booking = await session.get(Booking, booking_id)
            if not booking:
                return False
            booking.status = BookingStatus.CONFIRMED
            try:
                booking.cash_hold_expires_at = None
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def reschedule(booking_id: int, new_starts_at: datetime) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            b.starts_at = new_starts_at
            try:
                b.cash_hold_expires_at = None
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def mark_paid(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            b.status = BookingStatus.PAID
            try:
                b.paid_at = datetime.now(UTC)
                b.cash_hold_expires_at = None
            except Exception:
                pass
            await session.commit()
            return True

    @staticmethod
    async def set_cancelled(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            b.status = BookingStatus.CANCELLED
            await session.commit()
            return True

    @staticmethod
    async def list_active_by_user(user_id: int) -> list[Booking]:
        """Return upcoming/active Booking objects for a given user in a single query.

        Active = starts_at >= now and status not in terminal.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Booking, BookingStatus
                now = datetime.now(UTC)
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
    async def set_pending_payment(booking_id: int) -> bool:
        async with get_session() as session:
            from bot.app.domain.models import Booking, BookingStatus
            b = await session.get(Booking, booking_id)
            if not b:
                return False
            b.status = BookingStatus.PENDING_PAYMENT
            # Extend hold window to give the user time to finish payment/confirmation
            try:
                hold_min = await SettingsRepo.get_reservation_hold_minutes()
            except Exception:
                hold_min = 5
            try:
                b.cash_hold_expires_at = datetime.now(UTC) + timedelta(minutes=max(1, int(hold_min or 0)))
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
                stmt = stmt.where(Booking.status.in_([
                    getattr(BookingStatus, "AWAITING_CASH", BookingStatus.CONFIRMED),
                    BookingStatus.PENDING_PAYMENT,
                    BookingStatus.RESERVED,
                ]))
            elif mode == "upcoming":
                from zoneinfo import ZoneInfo
                now_utc = datetime.now().astimezone(ZoneInfo("UTC"))
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
            svc = await session.get(Service, getattr(b, 'service_id', ''))
            return getattr(svc, 'name', None) or str(getattr(b, 'service_id', ''))

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

        now = datetime.now(UTC)
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
        service_name_expr = func.coalesce(
            service_items_subq.c.service_name,
            Service.name,
            func.cast(Booking.service_id, String),
        ).label("service_name")
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
                    Booking.service_id,
                    Booking.status,
                    Booking.starts_at,
                    Booking.original_price_cents,
                    Booking.final_price_cents,
                    Service.currency.label("currency"),
                    Master.name.label("master_name"),
                    service_name_expr,
                )
                .join(Master, Master.telegram_id == Booking.master_id, isouter=True)
                .outerjoin(service_items_subq, service_items_subq.c.booking_id == Booking.id)
                .outerjoin(Service, Service.id == Booking.service_id)
                .where(*where_clause)
                .order_by(order_expr)
            )

            if page_size:
                stmt = stmt.limit(page_size).offset(offset)

            result = await session.execute(stmt)
            raw_rows = result.all()
            booking_infos: list[BookingInfo] = []
            for (
                booking_id,
                master_id,
                service_id,
                status,
                starts_at,
                original_price_cents,
                final_price_cents,
                currency_val,
                master_name,
                service_name,
            ) in raw_rows:
                booking_infos.append(
                    booking_info_from_mapping(
                        {
                            "id": booking_id,
                            "master_id": master_id,
                            "service_id": service_id,
                            "status": status,
                            "starts_at": starts_at,
                            "original_price_cents": original_price_cents,
                            "final_price_cents": final_price_cents,
                            "currency": currency_val or "UAH",
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

        now = datetime.now(UTC)
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
        service_name_expr = func.coalesce(
            service_items_subq.c.service_name,
            Service.name,
            func.cast(Booking.service_id, String),
        ).label("service_name")
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
                .outerjoin(Service, Service.id == Booking.service_id)
            )
            if page_size:
                stmt = stmt.limit(page_size).offset(offset)
            result = await session.execute(stmt)
            raw_rows = list(result.all())
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
                            "currency": getattr(b, "currency", "UAH"),
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
        data = {
            "booking_id": getattr(booking, "id", booking if isinstance(booking, int) else 0),
            "service_name": service_name,
            "master_name": master_name,
            "price_cents": getattr(booking, "final_price_cents", None)
            or getattr(booking, "original_price_cents", None)
            or 0,
            "currency": getattr(booking, "currency", "UAH"),
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
        lock_r_val = await SettingsRepo.get_client_reschedule_lock_hours()
        lock_c_val = await SettingsRepo.get_client_cancel_lock_hours()
    except Exception:
        lock_r_val = None
        lock_c_val = None
    can_cancel, can_reschedule = await calculate_booking_permissions(data, lock_r_hours=lock_r_val, lock_c_hours=lock_c_val)

    return BookingDetails(
        booking_id=int(data.get("booking_id", 0) or 0),
        service_name=data.get("service_name"),
        master_name=data.get("master_name"),
        price_cents=int(data.get("price_cents", 0) or 0),
        currency=data.get("currency", "UAH"),
        starts_at=data.get("starts_at"),
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


async def send_booking_notification(
    bot: Bot,
    booking_id: int,
    event_type: str,
    recipients: Sequence[int],
    *,
    previous_starts_at: datetime | None = None,
) -> None:
    from bot.app.translations import tr

    try:
        booking = await BookingRepo.get(booking_id)
        if not booking:
            return
        logger.info("send_booking_notification: booking=%s event=%s recipients=%s", booking_id, event_type, recipients)
        for rid in recipients:
            try:
                rid_int = int(rid)
            except Exception:
                logger.warning("send_booking_notification: invalid recipient id, skipping: %r", rid)
                continue
            lang = await safe_get_locale(rid_int)
            try:
                bd = await build_booking_details(booking, user_id=rid_int, lang=lang)
            except Exception as be:
                logger.exception("send_booking_notification: build_booking_details failed: %s", be)
                continue

            svc_names = bd.service_name or ""
            starts = bd.starts_at
            dt_txt = format_date(starts) if starts else ""
            client_line = bd.client_name or ""
            master_id_val = getattr(booking, "master_id", None)
            client_tg_id = bd.client_telegram_id

            try:
                if event_type == "paid":
                    title = tr("notif_paid_confirmed", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "cash_confirmed":
                    title = tr("notif_cash_confirmed", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "cancelled":
                    title = tr("notif_client_cancelled", lang=lang).format(id=booking_id, user=client_line)
                elif event_type == "rescheduled_by_client":
                    if int(rid_int) == int(master_id_val or 0):
                        title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                    else:
                        title = tr("notif_client_rescheduled", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "rescheduled_by_master":
                    if client_tg_id and int(rid_int) == int(client_tg_id):
                        title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                    else:
                        title = tr("notif_master_rescheduled_admin", lang=lang).format(master=master_id_val or "", id=booking_id, service=svc_names, dt=dt_txt)
                else:
                    title = f"#{booking_id}: {svc_names} {dt_txt}".strip()
            except Exception:
                title = f"#{booking_id}"

            body = format_booking_details_text(bd, lang)
            try:
                await bot.send_message(chat_id=rid_int, text=f"{title}\n\n{body}".strip())
                logger.info("send_booking_notification: sent to %s", rid_int)
            except Exception as se:
                logger.warning("Failed to send notification to %s: %s", rid_int, se)
    except Exception as e:
        logger.exception("send_booking_notification failed: %s", e)


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



async def get_or_create_user(telegram_id: int, name: str | None = None, username: str | None = None) -> User:
    """Delegate user lookup/creation to UserRepo."""
    try:
        return await UserRepo.get_or_create(telegram_id, name=name, username=username)
    except Exception as e:
        logger.error("get_or_create_user(repo) failed for %s: %s", telegram_id, e)
        raise


async def get_available_time_slots_for_services(
    date: datetime,
    master_id: int,
    service_durations: list[int]
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
        # 1. Get Work Windows (Local Time)
        # Returns list of (start_time, end_time)
        windows_local = await master_services.get_work_windows_for_day(master_id, date)
    except Exception:
        windows_local = [(dtime(hour=9), dtime(hour=18))]
    
    if not windows_local:
        return []

    # 2. Get Bookings (UTC)
    # We need the full day in UTC to catch all relevant bookings
    local_day_start = date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=LOCAL_TZ)
    local_day_end = local_day_start + timedelta(days=1)
    day_start_utc = local_day_start.astimezone(UTC)
    day_end_utc = local_day_end.astimezone(UTC)

    async with get_session() as session:
        result = await session.execute(
            select(Booking).where(
                Booking.master_id == master_id,
                Booking.starts_at >= day_start_utc,
                Booking.starts_at < day_end_utc,
            ).order_by(Booking.starts_at)
        )
        bookings_objs = result.scalars().all()

    hold_minutes = await SettingsRepo.get_reservation_hold_minutes()
    now_utc = datetime.now(UTC)
    
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
    # Convert windows to UTC intervals for calculation
    window_intervals_utc = []
    for ws, we in windows_local:
        w_start = datetime.combine(date.date(), ws).replace(tzinfo=LOCAL_TZ).astimezone(UTC)
        w_end = datetime.combine(date.date(), we).replace(tzinfo=LOCAL_TZ).astimezone(UTC)
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
    now_local = datetime.now(LOCAL_TZ)
    is_today = (local_day_start.date() == now_local.date())

    for gap_start, gap_end in free_gaps:
        # Calculate duration in minutes
        gap_duration_minutes = (gap_end - gap_start).total_seconds() / 60
        
        # Check if the gap is large enough for the total service duration
        if gap_duration_minutes >= total_duration:
            # Instead of only returning the gap start, generate stepped slots
            # across the gap so clients can pick any available start time.
            try:
                slot_step_min = await SettingsRepo.get_slot_duration()
                slot_step_min = int(slot_step_min or 0)
            except Exception:
                slot_step_min = 0

            # Fallback to reasonable tick: if slot_step is missing or zero,
            # prefer a 15-minute grid (or total_duration if larger).
            if not slot_step_min or slot_step_min <= 0:
                slot_step_min = 15 if total_duration < 15 else int(total_duration)

            current = gap_start
            # walk the gap in steps and add each candidate that fits total_duration
            while (current + timedelta(minutes=total_duration)) <= gap_end:
                # Check lead time if it's today (compare in local timezone)
                if is_today and lead_min:
                    try:
                        candidate_local = current.astimezone(LOCAL_TZ)
                        if (candidate_local - now_local).total_seconds() / 60 < lead_min:
                            current = current + timedelta(minutes=slot_step_min)
                            continue
                    except Exception:
                        pass

                slots.append(current.astimezone(LOCAL_TZ).time())
                current = current + timedelta(minutes=slot_step_min)

    logger.debug("Slots (Gap-based) for master %s on %s: %s", master_id, date, slots)
    return slots



async def get_available_days_for_month(master_id: int, year: int, month: int, service_duration_min: int = 60) -> set[int]:
    """Return set of day numbers (1..31) within the month that have at least one available slot.

    This function loads the master's schedule and all bookings for the month in a single
    DB query, then simulates slot generation in memory per day to decide whether the day
    has at least one free slot. The goal is to avoid calling DB per-day.
    """
    try:
        from calendar import monthrange

        _, days_in_month = monthrange(year, month)

        # Month start/end in local timezone -> convert to UTC for DB query
        month_start_local = datetime(year, month, 1).replace(tzinfo=LOCAL_TZ)
        if month == 12:
            next_month_local = datetime(year + 1, 1, 1).replace(tzinfo=LOCAL_TZ)
        else:
            next_month_local = datetime(year, month + 1, 1).replace(tzinfo=LOCAL_TZ)
        month_start_utc = month_start_local.astimezone(UTC)
        next_month_utc = next_month_local.astimezone(UTC)

        now_utc = datetime.now(UTC)

        # Load bookings for the whole month in one query
        # Load bookings for the whole month in one query using canonical provider
        
        # Load bookings for the whole month directly for the master to avoid
        # Avoid legacy client-focused wrapper; load master bookings directly.
        async with get_session() as session:
            result = await session.execute(
                select(Booking).where(
                    Booking.master_id == master_id,
                    Booking.starts_at >= month_start_utc,
                    Booking.starts_at < next_month_utc,
                ).order_by(Booking.starts_at)
            )
            bookings_raw_objs = result.scalars().all()

        # Normalize bookings to intervals that still block slots
        blocked_intervals: list[tuple[datetime, datetime]] = []
        hold_minutes = await SettingsRepo.get_reservation_hold_minutes()
        for b in bookings_raw_objs:
            try:
                starts_at = getattr(b, "starts_at", None)
                if not starts_at:
                    continue

                if is_booking_slot_blocked(b, now_utc, hold_minutes):
                    interval = _get_booking_interval(b, service_duration_min)
                    if interval:
                        blocked_intervals.append(interval)
            except Exception:
                continue

        # Load master profile once for schedule/windows
        from typing import Any
        data: dict[str, Any] = {}
        try:
            async with get_session() as session:
                prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_id))
                if prof and getattr(prof, 'bio', None):
                    try:
                        import json
                        data = json.loads(prof.bio or "{}") or {}
                    except Exception:
                        data = {}
        except Exception:
            data = {}

        available_days: set[int] = set()

        step = timedelta(minutes=service_duration_min)

        # Same-day lead minutes
        lead_min = await SettingsRepo.get_same_day_lead_minutes()
        now_local = datetime.now(LOCAL_TZ)

        # Evaluate each day in month
        for day in range(1, days_in_month + 1):
            day_date_local = datetime(year, month, day).replace(tzinfo=LOCAL_TZ)
            wd = day_date_local.weekday()

            # Determine windows for this day using centralized parser from master_services.
            windows_local = master_windows = master_services._parse_windows_from_bio(data, day_date_local)

            # Gather bookings relevant to this day for overlap checks
            # bookings are in UTC
            day_start_local = day_date_local
            day_end_local = (day_date_local + timedelta(days=1))
            day_start_utc = day_start_local.astimezone(UTC)
            day_end_utc = day_end_local.astimezone(UTC)
            bookings_for_day = [
                (b_start, b_end)
                for b_start, b_end in blocked_intervals
                if b_start < day_end_utc and b_end > day_start_utc
            ]

            # If same-day lead applied and day is today, compute now_local
            is_today = (day_date_local.date() == now_local.date())

            any_slot = False
            for ws, we in windows_local:
                work_start_local = datetime.combine(day_date_local.date(), ws).replace(tzinfo=LOCAL_TZ)
                work_end_local = datetime.combine(day_date_local.date(), we).replace(tzinfo=LOCAL_TZ)
                work_start_utc = work_start_local.astimezone(UTC)
                work_end_utc = work_end_local.astimezone(UTC)

                current = work_start_utc
                while current + step <= work_end_utc:
                    slot_start = current
                    slot_end = current + step

                    # same-day lead filter (use local now)
                    if lead_min and is_today:
                        try:
                            local_slot_dt = slot_start.astimezone(LOCAL_TZ)
                            if (local_slot_dt - now_local) < timedelta(minutes=lead_min):
                                current += step
                                continue
                        except Exception:
                            pass

                    overlap = False
                    for b_start, b_end in bookings_for_day:
                        if slot_start < b_end and slot_end > b_start:
                            overlap = True
                            break
                    if not overlap:
                        any_slot = True
                        break
                    current += step
                if any_slot:
                    break

            if any_slot:
                available_days.add(day)

        return available_days
    except Exception as e:
        logger.exception("Ошибка получения доступных дней для мастера %s %04d-%02d: %s", master_id, year, month, e)
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
            # Application-level guard: delegate interval-overlap checks to BookingRepo
            try:
                # Determine duration for the new booking (minutes) so we can compute new_end
                try:
                    totals = await get_services_duration_and_price([service_id], online_payment=False, master_id=master_id)
                    new_dur = int(totals.get("total_minutes") or 0)
                except Exception:
                    new_dur = 0
                if not new_dur:
                    new_dur = await SettingsRepo.get_slot_duration()

                new_start = slot
                new_end = new_start + timedelta(minutes=new_dur)

                conflict = await BookingRepo.find_conflicting_booking(session, client_id, master_id, new_start, new_end, service_ids=[service_id])
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
                # If locking fails (e.g., DB backend limitations), log the failure.
                # In some deployments row-locking is critical to avoid races; enable
                # strict behavior by setting env `REQUIRE_ROW_LOCK=1` to raise.
                import os
                logger.exception("Row-level locking failed while creating booking: %s", e)
                if os.getenv("REQUIRE_ROW_LOCK", "0").lower() in ("1", "true", "yes"):
                    raise

            # Snapshot service price at booking time and create booking via helper
            svc = await session.get(Service, service_id)
            svc_price = int(getattr(svc, "price_cents", 0) or 0)
            booking = await _create_booking_base(session, client_id, master_id, slot, price_cents=svc_price, hold_minutes=hold_minutes, service_id=service_id, duration_minutes=new_dur)
            try:
                await session.commit()
            except IntegrityError as ie:
                # Likely a DB-level unique index violation due to race; translate
                # to a friendly error for callers. This can happen if two users
                # attempt to reserve the same slot concurrently. We keep a short
                # INFO-level log here because this is an expected collision and
                # is handled by the application logic (user sees a friendly alert).
                await session.rollback()
                logger.info("IntegrityError on commit while creating booking (slot likely taken): %s", ie)
                raise ValueError("slot_unavailable") from ie
            await session.refresh(booking)
            logger.info("Создана запись №%s: client_id=%s, master_id=%s, service_id=%s, slot=%s, expires_at=%s", booking.id, client_id, master_id, service_id, slot, booking.cash_hold_expires_at)
            return booking
    except SQLAlchemyError as e:
        logger.error("Ошибка создания записи: client_id=%s, master_id=%s, service_id=%s, slot=%s, error=%s", client_id, master_id, service_id, slot, e)
        raise


async def get_services_duration_and_price(service_ids: Sequence[str], online_payment: bool = False, master_id: int | None = None) -> dict[str, int | str]:
    """Return total duration (minutes) and total price_cents for selected services without N+1 queries.

    - Loads all Services in a single query.
    - Loads all ServiceProfiles in a single query.
    If ServiceProfile.duration_minutes is missing, falls back to 60 per service.
    """
    total_minutes = 0
    total_price = 0
    currency = "UAH"
    try:
        if not service_ids:
            return {"total_minutes": 0, "total_price_cents": 0, "currency": currency}
        async with get_session() as session:
            # Bulk load services
            svc_rows = await session.execute(select(Service).where(Service.id.in_(list(service_ids))))
            services = {str(s.id): s for s in svc_rows.scalars().all()}
            # Bulk load profiles
            prof_rows = await session.execute(select(ServiceProfile).where(ServiceProfile.service_id.in_(list(service_ids))))
            profiles = {str(p.service_id): p for p in prof_rows.scalars().all()}
            overrides: dict[str, int] = {}
            if master_id is not None:
                from bot.app.domain.models import MasterService
                ms_rows = await session.execute(
                    select(MasterService.service_id, MasterService.duration_minutes).where(
                        MasterService.master_telegram_id == int(master_id),
                        MasterService.service_id.in_(list(service_ids)),
                    )
                )
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
                    if getattr(svc, "currency", None):
                        currency = svc.currency or currency
                prof = profiles.get(str(sid))
                if str(sid) in overrides:
                    dur = overrides[str(sid)]
                else:
                    dur = int(getattr(prof, "duration_minutes", 0) or 0) if prof else 0
                total_minutes += dur if dur > 0 else 60
        if online_payment and total_price > 0:
            total_price = int(total_price * 0.95)
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
                import os
                logger.exception("Row-level locking failed while creating composite booking: %s", e)
                if os.getenv("REQUIRE_ROW_LOCK", "0").lower() in ("1", "true", "yes"):
                    raise

            price_cents = int(totals.get("total_price_cents", 0) or 0) or None
            booking = await _create_booking_base(session, client_id, master_id, slot, price_cents=price_cents, hold_minutes=hold_minutes, service_id=str(service_ids[0]), duration_minutes=new_dur)
            await session.flush()
            # add items
            pos = 0
            for sid in service_ids:
                session.add(BookingItem(booking_id=booking.id, service_id=str(sid), position=pos))
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
        try:
            from bot.app.services.shared_services import LOCAL_TZ
            biz_tz = LOCAL_TZ
        except Exception:
            biz_tz = None
        if biz_tz is None:
            # Fallback to UTC only if local timezone is not resolvable
            biz_tz = UTC
        local_dt = local_dt.replace(tzinfo=biz_tz)
        slot_dt = local_dt.astimezone(UTC)

        # Ensure user exists
        user_full_name = None
        booking_user = await get_or_create_user(user_telegram_id, user_full_name)

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
            formatted_date = datetime.fromisoformat(date_str).strftime("%d.%m.%Y")
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
        service_id=service_id,
        starts_at=slot,
        status=BookingStatus.RESERVED,
        created_at=datetime.now(UTC),
    )
    try:
        if price_cents is not None and price_cents > 0:
            booking.original_price_cents = int(price_cents)
            booking.final_price_cents = int(price_cents)
    except Exception:
        pass
    _hold = hold_minutes if hold_minutes is not None else await SettingsRepo.get_reservation_hold_minutes()
    booking.cash_hold_expires_at = datetime.now(UTC) + timedelta(minutes=max(1, _hold))
    # Determine and set ends_at for exclusion constraint correctness.
    try:
        duration_min = duration_minutes
        if not duration_min:
            if service_id:
                from bot.app.domain.models import ServiceProfile, MasterService
                # Master-specific override first
                try:
                    ms_row = await session.scalar(
                        select(MasterService).where(
                            MasterService.master_telegram_id == int(master_id),
                            MasterService.service_id == service_id,
                        )
                    )
                    if ms_row and getattr(ms_row, "duration_minutes", None):
                        duration_min = int(getattr(ms_row, "duration_minutes") or 0)
                except Exception:
                    duration_min = None
                if not duration_min:
                    sp = await session.scalar(select(ServiceProfile).where(ServiceProfile.service_id == service_id))
                    duration_min = int(getattr(sp, "duration_minutes", 0) or 0)
        if not duration_min:
            duration_min = await SettingsRepo.get_slot_duration()
        booking.ends_at = slot + timedelta(minutes=duration_min)
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
                return {"final_price_cents": 0, "currency": "UAH"}

            price = service.price_cents
            if online_payment:
                price = int(price * 0.95)  # 5% скидка
            result = {"final_price_cents": price, "currency": service.currency or "UAH"}
            logger.debug("Рассчитана цена для услуги %s (онлайн=%s): %s", service_id, online_payment, result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета цены для услуги %s: %s", service_id, e)
        return {"final_price_cents": 0, "currency": "UAH"}

async def process_successful_payment(booking_id: int, charge_id: str) -> bool:
    """Обрабатывает успешный платеж и обновляет статус записи.

    Args:
        booking_id: ID записи.
        charge_id: ID платежа.

    Returns:
        True, если обработка успешна, иначе False.
    """
    try:
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not booking:
                logger.warning("Запись не найдена для обработки платежа: id=%s", booking_id)
                return False
            booking.status = BookingStatus.PAID
            await session.commit()
            logger.info("Платеж обработан для записи №%s, charge_id=%s", booking_id, charge_id)
            return True
    except SQLAlchemyError as e:
        logger.error("Ошибка обработки платежа для записи №%s: %s", booking_id, e)
        return False

# ---------------- Client-side booking action guards -----------------

async def cancel_client_booking(booking_id: int, user_telegram_id: int, *, bot=None, lang: str | None = None) -> tuple[bool, str, dict]:
    """Cancel a client's booking with business checks centralized.

    Returns (ok, message_key, params). On success notifications are sent when bot provided.
    """
    try:
        # Resolve internal user and ensure ownership
        user = await get_or_create_user(user_telegram_id, None)
        b = await BookingRepo.ensure_owner(int(user.id), int(booking_id))
        if not b:
            return False, "booking_not_found", {}

        # Terminal/expired checks
        status_val = getattr(b, "status", None)
        status_enum = normalize_booking_status(status_val)
        if status_enum in TERMINAL_STATUSES:
            return False, "booking_not_active", {}

        starts_at = getattr(b, "starts_at", None)
        if starts_at and starts_at <= datetime.now(UTC):
            return False, "cannot_cancel_past", {}

        # Lock window
        try:
            lock_h = int(await SettingsRepo.get_client_cancel_lock_hours())
        except Exception:
            lock_h = 3
        if starts_at and (starts_at - datetime.now(UTC)).total_seconds() < lock_h * 3600:
            return False, "cancel_too_close", {"hours": lock_h}

        # Perform cancellation
        ok = await BookingRepo.set_cancelled(int(booking_id))
        if not ok:
            return False, "error_retry", {}

        # Optional notifications
        if bot and b:
            try:
                from bot.app.services.shared_services import get_admin_ids
                recipients = [int(getattr(b, "master_id", 0))] + get_admin_ids()
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
        user = await get_or_create_user(user_telegram_id, None)
        b = await BookingRepo.ensure_owner(int(user.id), int(booking_id))
        if not b:
            return False, "booking_not_found"
        status_enum = normalize_booking_status(getattr(b, "status", None))
        if status_enum in TERMINAL_STATUSES:
            return False, "booking_not_active"
        # Reschedule lock window
        try:
            lock_h = int(await SettingsRepo.get_client_reschedule_lock_hours())
        except Exception:
            lock_h = 3
        starts_at = getattr(b, "starts_at", None)
        if starts_at and (starts_at - datetime.now(UTC)).total_seconds() < lock_h * 3600:
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


# Public exports for improved import clarity
__all__ = [
    "BookingRepo",
    "is_booking_slot_blocked",
]
