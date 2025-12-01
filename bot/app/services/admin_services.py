from __future__ import annotations
import csv
import io
import logging
import os
import re
import time
from datetime import UTC, datetime, timedelta
import json
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from sqlalchemy import func, select, String, and_
from sqlalchemy.exc import SQLAlchemyError


from bot.app.domain.models import Booking, Master, Service, User, BookingStatus, REVENUE_STATUSES
from bot.app.core.db import get_session
from bot.app.services.shared_services import (
    BookingInfo,
    LOCAL_TZ,
    booking_info_from_mapping,
    format_booking_list_item,
    format_money_cents,
    format_user_display_name,
    default_language,
)
from bot.app.translations import tr, t

logger = logging.getLogger(__name__)

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r, defaulting to %s", name, raw, default)
        return default


def _parse_setting_value(raw: Any) -> Any:
    """Parse a Setting.value string into bool/int/float when reasonable."""
    if raw is None:
        return raw
    try:
        s = str(raw).strip()
    except Exception:
        return raw
    low = s.lower()
    if low in {"true", "yes", "on", "1"}:
        return True
    if low in {"false", "no", "off", "0"}:
        return False
    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    except Exception:
        pass
    try:
        if "." in s:
            return float(s)
    except Exception:
        pass
    return s


def validate_contact_phone(value: str, lang: str) -> tuple[str | None, str | None]:
    """Validate phone input coming from admin wizard forms."""
    cleaned = re.sub(r"[\s\-\(\)]+", "", value or "")
    trimmed = cleaned.strip()
    if not trimmed or not re.match(r"^\+?\d{7,15}$", trimmed):
        return None, "invalid_phone"
    return trimmed, None


def validate_instagram_handle(value: str, lang: str) -> tuple[str | None, str | None]:
    """Validate Instagram handle input coming from admin wizard forms."""
    handle = value.strip().lstrip("@")
    if not handle:
        return None, None
    if re.fullmatch(r"[A-Za-z0-9._]{2,30}", handle) and not (".." in handle or handle.startswith(".") or handle.endswith(".")):
        return handle, None
    return None, "invalid_instagram"


DEFAULT_DAILY_SLOTS = _env_int("DEFAULT_DAILY_SLOTS", 8)
DEFAULT_SAME_DAY_LEAD_MINUTES = _env_int("SAME_DAY_LEAD_MINUTES", 0)
DEFAULT_CALENDAR_MAX_DAYS_AHEAD = _env_int("CALENDAR_MAX_DAYS_AHEAD", 365)
DEFAULT_REMINDER_LEAD_MINUTES = _env_int("DEFAULT_REMINDER_LEAD_MINUTES", 60)


# NOTE: Payments/provider helpers live in `shared_services`.
# We intentionally do not re-export them here to avoid namespace proxying.
# Callers should import payment helpers directly from
# `bot.app.services.shared_services`.


# Backwards-compatible lazy accessor for stats rendering. Importing the
# implementation at call-time avoids a circular import (shared_services
# imports AdminRepo at module import time).
def render_stats_overview(data: Mapping[str, Any], *, title_key: str = "stats_overview", lang: str = "uk") -> str:
    """Render a simple stats overview with a localized title and k:v pairs.

    Canonical admin implementation. This was moved here from
    `shared_services` to keep admin analytics helpers colocated with
    `AdminRepo` and avoid circular imports.
    """
    try:
        title = tr(title_key, lang=lang)
        lines = [title]
        lines.extend(f"{k}: {v}" for k, v in data.items())
        return "\n".join(lines)
    except Exception:
        try:
            return "\n".join([title_key] + [f"{k}: {v}" for k, v in data.items()])
        except Exception:
            return title_key


async def delete_master_with_checks(master_tid: int) -> tuple[bool, int]:
    """Delete a master if no active/future bookings reference it.

    Returns (ok, blocking_count). If blocking_count > 0, deletion was not
    performed because there are active/future bookings.
    """
    try:
        remaining = await AdminRepo.get_active_future_booking_ids_for_master(master_tid)
    except Exception as e:
        logger.exception("delete_master_with_checks: failed to check bookings for %s: %s", master_tid, e)
        return False, -1
    if remaining:
        return False, len(remaining)
    try:
        # Import here to avoid circular imports at module load time
        from bot.app.services.master_services import MasterRepo
        deleted = await MasterRepo.delete_master(master_tid)
        return (True, 0) if deleted else (False, 0)
    except Exception as e:
        logger.exception("delete_master_with_checks: delete failed for %s: %s", master_tid, e)
        return False, 0


# Minimal cache/store globals used by ServiceRepo implementation
_services_cache_store: dict[str, str] | None = None

# Settings cache used by SettingsRepo when copied into this module
# DB-only strategy: legacy long-lived process-wide settings cache replaced by
# a very short-lived (<=5s) snapshot to preserve synchronous call sites without
# cross-process divergence. Consider migrating to fully async access for high
# traffic keys.
_settings_cache: dict[str, Any] | None = None
_settings_last_checked: datetime | None = None

DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "uk")
DEFAULT_SLOT_DURATION = _env_int("SLOT_DURATION", 60)
DEFAULT_RESERVATION_HOLD_MINUTES = _env_int("RESERVATION_HOLD_MINUTES", 5)
DEFAULT_CLIENT_RESCHEDULE_LOCK_HOURS = _env_int("CLIENT_RESCHEDULE_LOCK_HOURS", 3)
DEFAULT_CLIENT_CANCEL_LOCK_HOURS = _env_int("CLIENT_CANCEL_LOCK_HOURS", 3)


def invalidate_services_cache() -> None:
    """Invalidate services cache (useful after CRUD)."""
    global _services_cache_store
    _services_cache_store = None


class ServiceRepo:
    """Repository for Service-related lookups and caches.

    Moved here from shared_services to decentralize repo responsibilities.
    """

    @staticmethod
    async def services_cache() -> dict[str, str]:
        global _services_cache_store
        if _services_cache_store is not None:
            return _services_cache_store
        try:
            try:
                async with get_session() as session:
                    from bot.app.domain.models import Service
                    res = await session.execute(select(Service.id, Service.name))
                    rows = res.all()
                    if rows:
                        _services_cache_store = {str(r[0]): str(r[1]) for r in rows}
            except Exception:
                _services_cache_store = None

            if not _services_cache_store:
                logger.info("ServiceRepo.services_cache: DB empty/unavailable; returning empty services mapping")
                _services_cache_store = {}
        except Exception:
            _services_cache_store = {}
        return _services_cache_store or {}

    # --- Pagination helpers (avoid storing full list in FSM) ---
    @staticmethod
    async def count_services() -> int:
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                from sqlalchemy import select, func
                return int((await session.execute(select(func.count()).select_from(Service))).scalar() or 0)
        except Exception as e:
            logger.warning("ServiceRepo.count_services failed: %s", e)
            return 0

    @staticmethod
    async def get_services_page(page: int = 1, page_size: int = 10) -> list[tuple[str, str]]:
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 10
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                from sqlalchemy import select
                offset = (page - 1) * page_size
                stmt = select(Service.id, Service.name).order_by(Service.id).offset(offset).limit(page_size)
                rows = (await session.execute(stmt)).all()
                return [(str(r[0]), str(r[1]) if r[1] is not None else "") for r in rows]
        except Exception as e:
            logger.warning("ServiceRepo.get_services_page failed (page=%s): %s", page, e)
            return []


    # Role-based booking formatting now uses shared `format_booking_list_item(..., role="admin")`.


    @staticmethod
    async def get_admin_bookings(
        *,
        mode: str = "upcoming",
        page: int = 1,
        page_size: int | None = 5,
        start: datetime | None = None,
        end: datetime | None = None,
        optimized: bool = False,
    ) -> tuple[list[BookingInfo], dict[str, Any]]:
        """Возвращает страницу записей для админа.

        Если optimized=True, использует двухфазный запрос (IDs -> детали -> агрегирование
        услуг) чтобы уменьшить нагрузку string_agg/outer join на больших таблицах.
        """
        from bot.app.domain.models import Booking, BookingStatus, BookingItem, Service
        from bot.app.core.db import get_session

        now = datetime.now(UTC)
        async with get_session() as session:
            base_where: list[Any] = []  # Админ видит всё

            # Логика подсчета вкладок (такая же, как у мастера, но без base_where)
            try:
                done_count = int((await session.execute(select(func.count()).select_from(Booking).where(Booking.status == BookingStatus.DONE))).scalar() or 0)
                cancelled_count = int((await session.execute(select(func.count()).select_from(Booking).where(Booking.status == BookingStatus.CANCELLED))).scalar() or 0)
                noshow_count = int((await session.execute(select(func.count()).select_from(Booking).where(Booking.status == BookingStatus.NO_SHOW))).scalar() or 0)
                upcoming_count = int((await session.execute(
                    select(func.count()).select_from(Booking).where(
                        Booking.starts_at >= now,
                        Booking.status.notin_([
                            BookingStatus.CANCELLED,
                            BookingStatus.DONE,
                            BookingStatus.NO_SHOW,
                            BookingStatus.EXPIRED,
                        ]),
                    )
                )).scalar() or 0)
            except Exception:
                done_count = cancelled_count = noshow_count = upcoming_count = 0

            # Логика фильтрации по 'mode'
            if mode == "done":
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
            else:  # upcoming
                where_clause = [*base_where, Booking.starts_at >= now, Booking.status.notin_([
                    BookingStatus.CANCELLED,
                    BookingStatus.DONE,
                    BookingStatus.NO_SHOW,
                    BookingStatus.EXPIRED,
                ])]
                order_expr = Booking.starts_at

            if start is not None:
                where_clause.append(Booking.starts_at >= start)
            if end is not None:
                where_clause.append(Booking.starts_at < end)

            # Подсчет total
            try:
                total = int((await session.execute(select(func.count()).select_from(Booking).where(*where_clause))).scalar() or 0)
            except Exception:
                total = 0

            # Логика пагинации
            if page_size:
                total_pages = max(1, (total + page_size - 1) // page_size)
                p = max(1, min(int(page or 1), total_pages))
                offset = (p - 1) * page_size
            else:
                total_pages = 1
                p = 1
                offset = 0

            if not optimized:
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
                stmt = (
                    select(
                        Booking,
                        User.name.label("client_name"),
                        Master.name.label("master_name"),
                        service_name_expr,
                        Service.currency.label("currency"),
                    )
                    .where(*where_clause)
                    .order_by(order_expr)
                    .join(User, User.id == Booking.user_id, isouter=True)
                    .join(Master, Master.telegram_id == Booking.master_id, isouter=True)
                    .outerjoin(service_items_subq, service_items_subq.c.booking_id == Booking.id)
                    .outerjoin(Service, Service.id == Booking.service_id)
                )
                if page_size:
                    stmt = stmt.limit(page_size).offset(offset)
                result = await session.execute(stmt)
                raw_rows = list(result.all())
                norm_rows: list[dict[str, Any]] = []
                for b, client_name, master_name, service_name, currency_val in raw_rows:
                    norm_rows.append({
                        "id": getattr(b, "id", None),
                        "master_id": getattr(b, "master_id", None),
                        "service_id": getattr(b, "service_id", None),
                        "status": getattr(b, "status", None),
                        "starts_at": getattr(b, "starts_at", None),
                        "original_price_cents": getattr(b, "original_price_cents", None),
                        "final_price_cents": getattr(b, "final_price_cents", None),
                        "master_name": master_name,
                        "client_name": client_name,
                        "currency": currency_val or getattr(b, "currency", "UAH"),
                        "user_id": getattr(b, "user_id", None),
                        "service_name": service_name,
                    })
            else:
                id_stmt = select(Booking.id).where(*where_clause).order_by(order_expr)
                if page_size:
                    id_stmt = id_stmt.limit(page_size).offset(offset)
                id_rows = await session.execute(id_stmt)
                booking_ids = [int(x) for x in id_rows.scalars().all()]
                norm_rows = []
                if booking_ids:
                    core_stmt = (
                        select(
                            Booking,
                            User.name.label("client_name"),
                            Master.name.label("master_name"),
                            Service.currency.label("currency"),
                        )
                        .join(User, User.id == Booking.user_id, isouter=True)
                        .join(Master, Master.telegram_id == Booking.master_id, isouter=True)
                        .outerjoin(Service, Service.id == Booking.service_id)
                        .where(Booking.id.in_(booking_ids))
                    )
                    core_rows = await session.execute(core_stmt)
                    core_map: dict[int, dict[str, Any]] = {}
                    for b, client_name, master_name, currency_val in core_rows.all():
                        bid_raw = getattr(b, "id", 0)
                        try:
                            bid = int(bid_raw)
                        except Exception:
                            bid = 0
                        core_map[bid] = {
                            "id": bid,
                            "master_id": getattr(b, "master_id", None),
                            "service_id": getattr(b, "service_id", None),
                            "status": getattr(b, "status", None),
                            "starts_at": getattr(b, "starts_at", None),
                            "original_price_cents": getattr(b, "original_price_cents", None),
                            "final_price_cents": getattr(b, "final_price_cents", None),
                            "master_name": master_name,
                            "client_name": client_name,
                            "currency": currency_val or getattr(b, "currency", "UAH"),
                            "user_id": getattr(b, "user_id", None),
                            "service_name": None,
                        }
                    svc_stmt = (
                        select(
                            BookingItem.booking_id,
                            func.string_agg(func.coalesce(Service.name, func.cast(BookingItem.service_id, String)), " + ").label("svc_names"),
                        )
                        .join(Service, Service.id == BookingItem.service_id)
                        .where(BookingItem.booking_id.in_(booking_ids))
                        .group_by(BookingItem.booking_id)
                    )
                    svc_rows = await session.execute(svc_stmt)
                    for bid, svc_names in svc_rows.all():
                        try:
                            if int(bid) in core_map:
                                core_map[int(bid)]["service_name"] = svc_names
                        except Exception:
                            continue
                    missing = [bid for bid, data in core_map.items() if data.get("service_name") is None and data.get("service_id")]
                    if missing:
                        try:
                            base_ids = [core_map[bid]["service_id"] for bid in missing if core_map[bid].get("service_id")]
                            base_stmt = select(Service.id, Service.name).where(Service.id.in_(base_ids))
                            base_rows = await session.execute(base_stmt)
                            base_map = {str(r[0]): str(r[1]) for r in base_rows.all()}
                            for bid in missing:
                                sid = core_map[bid].get("service_id")
                                if sid and str(sid) in base_map:
                                    core_map[bid]["service_name"] = base_map[str(sid)]
                                elif sid:
                                    core_map[bid]["service_name"] = str(sid)
                        except Exception:
                            pass
                    norm_rows = [core_map[bid] for bid in booking_ids if bid in core_map]

            booking_infos = [booking_info_from_mapping(row) for row in norm_rows]
            return booking_infos, {
                "total": total,
                "total_pages": total_pages,
                "page": p,
                "done_count": done_count,
                "cancelled_count": cancelled_count,
                "noshow_count": noshow_count,
                "upcoming_count": upcoming_count,
            }

    @staticmethod
    async def get_service_name(service_id: str) -> str:
        try:
            all_services = await ServiceRepo.services_cache()
            name = all_services.get(str(service_id))
            if name:
                logger.debug("ServiceRepo.get_service_name: cache hit for %s -> %s", service_id, name)
                return name
        except Exception:
            pass
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                svc = await session.get(Service, service_id)
                if svc and getattr(svc, 'name', None):
                    logger.info("ServiceRepo.get_service_name: db fallback for %s -> %s", service_id, svc.name)
                    return svc.name
        except Exception:
            pass
        logger.warning("ServiceRepo.get_service_name: service %s not found, returning id", service_id)
        return str(service_id)

    @staticmethod
    async def get(service_id: str):
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                return await session.get(Service, service_id)
        except Exception:
            return None

    @staticmethod
    async def add_service(service_id: str, name: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                if await session.get(Service, service_id):
                    return False
                session.add(Service(id=service_id, name=name))
                await session.commit()
            invalidate_services_cache()
            return True
        except Exception as e:
            logger.exception("ServiceRepo.add_service failed for %s: %s", service_id, e)
            return False

    @staticmethod
    async def delete_service(service_id: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                svc = await session.get(Service, service_id)
                if not svc:
                    return False
                await session.delete(svc)
                await session.commit()
            invalidate_services_cache()
            return True
        except Exception as e:
            logger.exception("ServiceRepo.delete_service failed for %s: %s", service_id, e)
            return False

    @staticmethod
    async def get_services_by_ids(ids: set[int]) -> dict[int, str]:
        if not ids:
            return {}
        try:
            all_services = await ServiceRepo.services_cache()
            found = {int(k): v for k, v in all_services.items() if int(k) in ids}
            missing = set(ids) - set(found.keys())
            if missing:
                async with get_session() as session:
                    from sqlalchemy import select
                    from bot.app.domain.models import Service
                    res = await session.execute(select(Service.id, Service.name).where(Service.id.in_(missing)))
                    for sid, name in res.all():
                        found[int(sid)] = str(name)
            return found
        except Exception:
            return {}

    @staticmethod
    async def update_currency(service_id: str, currency: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                svc = await session.get(Service, service_id)
                if not svc:
                    return False
                try:
                    svc.currency = currency
                except Exception:
                    pass
                await session.commit()
            invalidate_services_cache()
            return True
        except Exception as e:
            logger.exception("ServiceRepo.update_currency failed for %s: %s", service_id, e)
            return False

    @staticmethod
    async def update_price_cents(service_id: int | str, new_cents: int):
        """Update price_cents (and final_price_cents if present) for a Service.

        Returns the updated Service instance or None on error/not found.
        """
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                svc = await session.get(Service, service_id)
                if not svc:
                    logger.debug("ServiceRepo.update_price_cents: service not found %s", service_id)
                    return None
                svc.price_cents = int(new_cents)
                try:
                    if hasattr(svc, "final_price_cents"):
                        setattr(svc, "final_price_cents", int(new_cents))
                except Exception:
                    logger.debug("Could not set final_price_cents for service %s", service_id)
                await session.commit()
            invalidate_services_cache()
            return svc
        except Exception as e:
            logger.exception("ServiceRepo.update_price_cents failed for %s: %s", service_id, e)
            return None

    @staticmethod
    async def aggregate_services(service_ids: list[str]) -> dict[str, int | str]:
        total_minutes = 0
        total_price = 0
        currency = "UAH"
        try:
            if not service_ids:
                return {"total_minutes": 0, "total_price_cents": 0, "currency": currency}
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Service, ServiceProfile
                svc_rows = await session.execute(select(Service).where(Service.id.in_(list(service_ids))))
                services = {str(s.id): s for s in svc_rows.scalars().all()}
                prof_rows = await session.execute(select(ServiceProfile).where(ServiceProfile.service_id.in_(list(service_ids))))
                profiles = {str(p.service_id): p for p in prof_rows.scalars().all()}
                for sid in service_ids:
                    svc = services.get(str(sid))
                    if svc:
                        if isinstance(getattr(svc, "price_cents", None), int):
                            pc_raw = getattr(svc, "price_cents", 0)
                            try:
                                total_price += int(pc_raw)
                            except Exception:
                                pass
                        if getattr(svc, "currency", None):
                            currency = svc.currency or currency
                    prof = profiles.get(str(sid))
                    if prof:
                        try:
                            dur = int(getattr(prof, "duration_minutes", 0))
                        except Exception:
                            dur = 0
                    else:
                        dur = 0
                    total_minutes += dur if dur > 0 else 60
            return {"total_minutes": total_minutes, "total_price_cents": total_price, "currency": currency}
        except Exception as e:
            logger.warning("ServiceRepo.aggregate_services error for %s: %s", service_ids, e)
            return {"total_minutes": total_minutes, "total_price_cents": 0, "currency": currency}


async def generate_bookings_csv(
    mode: str,
    start: datetime | None,
    end: datetime | None,
    *,
    reference: datetime | None = None,
    optimized: bool = True,
    in_memory: bool = False,
    compress: bool = False,
) -> tuple[str, str]:
    """Stream bookings into a temporary CSV file and return its path + file name.

    Previous implementation materialized all rows + full CSV in memory which could
    lead to high RAM usage / OOM for large months. This version paginates through
    bookings and writes rows incrementally to a temporary file.

    Returns: (temp_file_path, file_name)
    """
    import tempfile
    try:
        # Prepare temp file for streaming writes
        now_local = reference or datetime.now(LOCAL_TZ)
        file_name = f"bookings_{mode}_{now_local:%Y_%m}.csv"
        if in_memory:
            tmp = tempfile.SpooledTemporaryFile(max_size=2_000_000, mode="w+b")
            text_wrapper = io.TextIOWrapper(tmp, encoding="utf-8", newline="")
            writer = csv.writer(text_wrapper)
        else:
            tmp = tempfile.NamedTemporaryFile("w", newline="", suffix=".csv", delete=False, encoding="utf-8")
            writer = csv.writer(tmp)
        try:
            writer.writerow(["ID", "Date", "Client", "Master", "Service", "Amount", "Status"])

            page = 1
            page_size = 1000  # tuned for reasonable memory / round trips
            while True:
                rows, _meta = await ServiceRepo.get_admin_bookings(
                    mode=mode,
                    page=page,
                    page_size=page_size,
                    start=start,
                    end=end,
                    optimized=optimized,
                )
                if not rows:
                    break
                for b in rows:
                    try:
                        c_cell = b.client_name or ""
                        m_cell = b.master_name or ""
                        s_name = b.service_name or str(b.service_id or "")

                        dt_local = b.starts_at
                        dt_txt = f"{dt_local:%Y-%m-%d %H:%M}" if dt_local else ""

                        cents = int(b.final_price_cents or b.original_price_cents or 0)
                        price = format_money_cents(cents)

                        status_val = getattr(b.status, "value", None)
                        status_value = str(status_val) if status_val is not None else str(b.status)
                        writer.writerow([b.id, dt_txt, c_cell, m_cell, s_name, price, status_value])
                    except Exception:
                        continue  # skip malformed row
                page += 1
        finally:
            if in_memory:
                try:
                    text_wrapper.flush()
                except Exception:
                    pass
            else:
                tmp.flush(); tmp.close()
        if in_memory:
            tmp.seek(0)
            raw_bytes = tmp.read()
            if isinstance(raw_bytes, str):
                raw_bytes = raw_bytes.encode("utf-8")
            if compress:
                import gzip
                raw_bytes = gzip.compress(raw_bytes)
                file_name = file_name + ".gz"
                suffix = ".csv.gz"
            else:
                suffix = ".csv"
            final_file = tempfile.NamedTemporaryFile("wb", suffix=suffix, delete=False)
            final_file.write(raw_bytes)
            final_file.flush(); final_file.close()
            return final_file.name, file_name
        return tmp.name, file_name
    except Exception as e:
        logger.exception("generate_bookings_csv failed: %s", e)
        raise


async def generate_unique_slug_from_name(name: str) -> str:
    """Generate a URL-safe unique slug for Service IDs derived from a display name."""
    trans = {
        'а':'a','б':'b','в':'v','г':'g','ґ':'g','д':'d','е':'e','є':'ye','ж':'zh','з':'z','и':'y','і':'i','ї':'yi','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ь':'','ю':'yu','я':'ya',
        'А':'A','Б':'B','В':'V','Г':'G','Ґ':'G','Д':'D','Е':'E','Є':'Ye','Ж':'Zh','З':'Z','И':'Y','І':'I','Ї':'Yi','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R','С':'S','Т':'T','У':'U','Ф':'F','Х':'Kh','Ц':'Ts','Ч':'Ch','Ш':'Sh','Щ':'Shch','Ь':'','Ю':'Yu','Я':'Ya'
    }

    def slugify(s: str) -> str:
        if not s:
            return "service"
        out = []
        for ch in s:
            out.append(trans.get(ch, ch))
        s2 = ''.join(out)
        s2 = s2.lower()
        s2 = re.sub(r'[^a-z0-9]+', '_', s2)
        s2 = s2.strip('_')
        return s2 or 'service'

    base = slugify(name)
    candidate = base
    idx = 1
    try:
        while True:
            exists = await ServiceRepo.get(candidate)
            if not exists:
                return candidate
            idx += 1
            candidate = f"{base}_{idx}"
    except Exception:
        return f"{base}_{int(time.time())}"


async def load_settings_from_db() -> None:
    """Load runtime settings into the in-memory cache from the DB Setting table."""
    global _settings_cache, _settings_last_checked
    try:
        from bot.app.domain.models import Setting

        async with get_session() as session:
            result = await session.execute(select(Setting))
            rows = result.scalars().all()
        if _settings_cache is None:
            _settings_cache = {}
        for setting in rows:
            key = str(getattr(setting, "key", ""))
            value = _parse_setting_value(getattr(setting, "value", None))
            if key:
                _settings_cache[key] = value
        _settings_last_checked = datetime.now(UTC)
        logger.info(
            "Runtime settings loaded from DB: %s",
            {k: _settings_cache.get(k) for k in ("reservation_hold_minutes", "timezone") if k in _settings_cache},
        )
    except Exception as e:
        logger.warning("SettingsRepo.load_settings_from_db failed: %s", e)


class SettingsRepo:
    """Repository wrapper around runtime settings cache and persistent Setting table.

    This class delegates to the canonical shared_services update/get helpers for
    runtime consistency while living in admin_services for targeted imports.
    """

    @staticmethod
    async def get_setting(key: str, default: Any = None) -> Any:
        """Return setting value via DB-only strategy (no in-memory divergence).

        Asynchronous facade: relies on short-lived snapshots when available and
        performs an async DB query when the cached TTL expires. This keeps the
        runtime cache consistent without calling ``asyncio.run`` from handlers.
        """
        try:
            from bot.app.core.db import get_session
            from bot.app.domain.models import Setting
        except Exception:
            return default
        global _settings_cache, _settings_last_checked
        try:
            if _settings_cache is not None and _settings_last_checked is not None:
                if (datetime.now(UTC) - _settings_last_checked) < timedelta(seconds=5):
                    return _parse_setting_value(_settings_cache.get(str(key), default))
        except Exception:
            pass
        try:
            async with get_session() as session:
                row = await session.scalar(select(Setting).where(Setting.key == str(key)))
            if row is None:
                return default
            value = _parse_setting_value(getattr(row, "value", default))
            try:
                if _settings_cache is None:
                    _settings_cache = {}
                _settings_cache[str(key)] = value
                _settings_last_checked = datetime.now(UTC)
            except Exception:
                pass
            return value
        except Exception:
            try:
                if _settings_cache is not None:
                    return _parse_setting_value(_settings_cache.get(str(key), default))
            except Exception:
                pass
            return default

    @staticmethod
    async def get_slot_duration() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("slot_duration", 60), 60)

    @staticmethod
    async def get_currency() -> str:
        """Return configured currency (ISO 4217) with ENV fallback.

        Precedence:
        1. settings table key 'currency'
        2. ENV var CURRENCY
        3. hardcoded 'UAH'
        """
        try:
            cur = await SettingsRepo.get_setting("currency", None)
            if cur:
                return str(cur).upper()
            env_cur = os.getenv("CURRENCY")
            if env_cur:
                return env_cur.upper()
        except Exception:
            pass
        return "UAH"

    @staticmethod
    async def get_reservation_hold_minutes() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("reservation_hold_minutes", 1), 1)

    @staticmethod
    async def get_expire_check_seconds() -> int:
        """Frequency (seconds) for background worker to scan and expire stale RESERVED bookings.

        Aggressive default: 30s to reduce "zombie" reservations impact.
        Setting key: reservation_expire_check_seconds.
        """
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("reservation_expire_check_seconds", 30), 30)

    @staticmethod
    async def get_client_reschedule_lock_hours() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("client_reschedule_lock_hours", DEFAULT_CLIENT_RESCHEDULE_LOCK_HOURS), DEFAULT_CLIENT_RESCHEDULE_LOCK_HOURS)

    @staticmethod
    async def get_client_cancel_lock_hours() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("client_cancel_lock_hours", DEFAULT_CLIENT_CANCEL_LOCK_HOURS), DEFAULT_CLIENT_CANCEL_LOCK_HOURS)

    @staticmethod
    async def get_same_day_lead_minutes() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("same_day_lead_minutes", DEFAULT_SAME_DAY_LEAD_MINUTES), DEFAULT_SAME_DAY_LEAD_MINUTES)

    @staticmethod
    async def get_reminder_lead_minutes() -> int:
        """Return configured lead time (minutes) to remind clients before booking start.

        Setting key: `reminder_lead_minutes`. Falls back to `DEFAULT_REMINDER_LEAD_MINUTES`.
        """
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("reminder_lead_minutes", DEFAULT_REMINDER_LEAD_MINUTES), DEFAULT_REMINDER_LEAD_MINUTES)

    @staticmethod
    async def get_calendar_max_days_ahead() -> int:
        return SettingsRepo._coerce_int(await SettingsRepo.get_setting("calendar_max_days_ahead", DEFAULT_CALENDAR_MAX_DAYS_AHEAD), DEFAULT_CALENDAR_MAX_DAYS_AHEAD)

    @staticmethod
    async def get_work_hours_map() -> dict[int, tuple[int, int] | None]:
        """Return per-day working hours map from JSON setting.

        Format: {0: [9,18], 1: [9,18], ..., 6: null} where 0=Mon.
        Returns dict[int, (start,end)|None]. Falls back to legacy global start/end if JSON missing.
        """
        raw = await SettingsRepo.get_setting("work_hours_json", None)
        if raw:
            try:
                data = json.loads(str(raw))
                out: dict[int, tuple[int, int] | None] = {}
                for k, v in data.items():
                    try:
                        day = int(k)
                    except Exception:
                        continue
                    if v is None:
                        out[day] = None
                    else:
                        try:
                            s, e = int(v[0]), int(v[1])
                            if 0 <= s <= 23 and 0 <= e <= 23 and e > s:
                                out[day] = (s, e)
                            else:
                                out[day] = None
                        except Exception:
                            out[day] = None
                return out
            except Exception:
                pass
        # Legacy fallback
        try:
            s = SettingsRepo._coerce_int(await SettingsRepo.get_setting("work_hours_start", None), 9)
            e = SettingsRepo._coerce_int(await SettingsRepo.get_setting("work_hours_end", None), 18)
            if s < e:
                return {d: (s, e) for d in range(7)}
        except Exception:
            pass
        return {}

    @staticmethod
    async def update_work_hours_map(map_data: dict[int, tuple[int, int] | None]) -> bool:
        """Persist the per-day working hours map as JSON.

        map_data: day -> (start,end)|None.
        """
        try:
            serial: dict[int, list[int] | None] = {}
            for d, rng in map_data.items():
                if rng is None:
                    serial[d] = None
                else:
                    serial[d] = [int(rng[0]), int(rng[1])]
            return await SettingsRepo.update_setting("work_hours_json", json.dumps(serial, ensure_ascii=False))
        except Exception as e:
            logger.warning("update_work_hours_map failed: %s", e)
            return False

    @staticmethod
    async def migrate_legacy_work_hours_if_needed() -> bool:
        """If JSON hours missing but legacy start/end present, persist JSON for all days.

        Returns True if migration performed, False otherwise.
        """
        try:
            if await SettingsRepo.get_setting("work_hours_json", None):
                return False
            s = await SettingsRepo.get_setting("work_hours_start", None)
            e = await SettingsRepo.get_setting("work_hours_end", None)
            if s is None or e is None:
                return False
            try:
                s_i = SettingsRepo._coerce_int(s, 9)
                e_i = SettingsRepo._coerce_int(e, 18)
            except Exception:
                return False
            if s_i >= e_i:
                return False
            serial = {d: [s_i, e_i] for d in range(7)}
            # Best-effort sync update; ignore failure silently
            try:
                ok = await SettingsRepo.update_setting("work_hours_json", json.dumps(serial, ensure_ascii=False))
                return bool(ok)
            except Exception:
                return False
        except Exception:
            return False

    @staticmethod
    def format_work_hours_summary(hours_map: dict[int, tuple[int, int] | None], lang: str = "uk") -> str:
        """Compact human summary, e.g., Mon–Fri 09:00–18:00; Sat 10:00–16:00; Sun Closed"""
        try:
            from bot.app.translations import tr
            day_labels = [
                tr("mon_short", lang=lang) or "Mon",
                tr("tue_short", lang=lang) or "Tue",
                tr("wed_short", lang=lang) or "Wed",
                tr("thu_short", lang=lang) or "Thu",
                tr("fri_short", lang=lang) or "Fri",
                tr("sat_short", lang=lang) or "Sat",
                tr("sun_short", lang=lang) or "Sun",
            ]
            # Normalize map to ordered list by day 0..6
            entries: list[tuple[int, tuple[int, int] | None]] = []
            for d in range(7):
                entries.append((d, hours_map.get(d)))

            # Group contiguous days with the same range
            groups: list[tuple[int, int, tuple[int, int] | None]] = []
            i = 0
            while i < 7:
                start_i = i
                rng = entries[i][1]
                j = i + 1
                while j < 7 and entries[j][1] == rng:
                    j += 1
                groups.append((start_i, j - 1, rng))
                i = j

            parts: list[str] = []
            closed_label = tr("closed_label", lang=lang) or "Closed"
            for g in groups:
                a, b, rng = g
                if a == b:
                    day_part = f"{day_labels[a]}"
                else:
                    day_part = f"{day_labels[a]}–{day_labels[b]}"
                if rng is None:
                    parts.append(f"{day_part} {closed_label}")
                else:
                    s, e = rng
                    parts.append(f"{day_part} {s:02d}:00–{e:02d}:00")
            return "; ".join(parts)
        except Exception:
            return ""

    @staticmethod
    async def update_setting(key: str, value: Any) -> bool:
        """Persist a setting and keep the runtime cache up to date."""
        global _settings_cache, _settings_last_checked
        try:
            # Update runtime cache for immediate visibility
            if _settings_cache is None:
                _settings_cache = {}
            _settings_cache[str(key)] = value

            _settings_last_checked = datetime.now(UTC)

            # Persist to DB Setting table when available
            try:
                from bot.app.domain.models import Setting
                async with get_session() as session:
                    from sqlalchemy import select
                    s = await session.scalar(select(Setting).where(Setting.key == str(key)))
                    now_ts = datetime.now(UTC)
                    if s:
                        s.value = str(value)
                        try:
                            setattr(s, "updated_at", now_ts)
                        except Exception:
                            pass
                    else:
                        session.add(Setting(key=str(key), value=str(value), updated_at=now_ts))
                    await session.commit()
            except Exception as db_e:
                logger.warning("SettingsRepo.update_setting: DB persist failed for %s: %s", key, db_e)
                # still consider update successful for runtime
            # Call optional hook for consumers. Import _safe_call lazily to
            # avoid circular imports with shared_services.
            try:
                from bot.app.services.shared_services import _safe_call as _safe_call_fn
                try:
                    _safe_call_fn("on_setting_update", key, value)
                except Exception:
                    pass
            except Exception:
                pass
            return True
        except Exception as e:
            logger.exception("SettingsRepo.update_setting failed: %s", e)
            return False

    @staticmethod
    def _coerce_int(value: Any, default: int) -> int:
        """Coerce a value to int using single try/except.

        Accepts ints, floats, numeric strings ("60", "60.0"). Returns default on failure.
        """
        try:
            if value is None or value == "":
                return default
            if isinstance(value, int):
                return value
            if isinstance(value, float):
                return int(value)
            return int(float(str(value).strip()))
        except (ValueError, TypeError):
            return default
        except Exception:
            return default


# Backwards-compatible module-level wrappers were removed in favor of
# calling repository APIs directly. Use `ServiceRepo.get_admin_bookings`
# plus the shared `format_booking_list_item(..., role="admin")` for list formatting.


class AdminRepo:
    """Repository centralizing admin analytics SQL queries.

    Copied from shared_services so admin code can import AdminRepo directly
    from admin_services without referencing shared_services.
    """

    @staticmethod
    async def get_basic_totals() -> dict[str, int]:
        try:
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, User
                total_bookings = await session.scalar(select(func.count(Booking.id))) or 0
                total_users = await session.scalar(select(func.count(User.id))) or 0
                return {"total_bookings": int(total_bookings), "total_users": int(total_users)}
        except Exception as e:
            logger.exception("AdminRepo.get_basic_totals failed: %s", e)
            return {"total_bookings": 0, "total_users": 0}

    @staticmethod
    async def set_user_admin(
        telegram_id: int,
        *,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
    ) -> bool:
        """Promote a user (by telegram_id) to admin; create user if missing.

        Centralized admin operation so handlers don't perform direct DB writes.
        Returns True on success, False on error.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import User

                user = await session.scalar(select(User).where(User.telegram_id == int(telegram_id)))
                display_name = format_user_display_name(username, first_name, last_name)
                if user:
                    user.is_admin = True
                    if username:
                        user.username = username
                    if first_name:
                        user.first_name = first_name
                    if last_name:
                        user.last_name = last_name
                    if display_name:
                        user.name = display_name
                    elif not getattr(user, "name", None):
                        user.name = str(telegram_id)
                    session.add(user)
                    await session.commit()
                    return True
                new = User(
                    telegram_id=int(telegram_id),
                    name=display_name or str(telegram_id),
                    username=username,
                    first_name=first_name,
                    last_name=last_name,
                    is_admin=True,
                )
                session.add(new)
                await session.commit()
            return True
        except Exception as e:
            logger.exception("AdminRepo.set_user_admin failed for %s: %s", telegram_id, e)
            return False

    @staticmethod
    async def list_admins() -> list[tuple[int, int, str]]:
        """Return list of admin users as (id, telegram_id, name).

        Used by admin handlers to render admin list without opening DB sessions
        directly in the handler module.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import User

                res = await session.execute(select(User.id, User.telegram_id, User.name).where(User.is_admin == True).order_by(User.id))
                rows = res.all()
                return [(int(r[0]), int(r[1]), str(r[2])) for r in rows]
        except Exception as e:
            logger.exception("AdminRepo.list_admins failed: %s", e)
            return []

    @staticmethod
    async def revoke_admin_by_id(admin_id: int) -> bool:
        """Revoke admin flag for a user by DB id.

        Returns True on success, False otherwise.
        """
        try:
            async with get_session() as session:
                from bot.app.domain.models import User
                user = await session.get(User, admin_id)
                if not user:
                    return False
                try:
                    user.is_admin = False
                except Exception:
                    pass
                session.add(user)
                await session.commit()
            return True
        except Exception as e:
            logger.exception("AdminRepo.revoke_admin_by_id failed for %s: %s", admin_id, e)
            return False

    @staticmethod
    async def get_booking_ids_for_master(master_tid: int) -> list[tuple[int, Any]]:
        """Return list of (booking_id, status) for bookings referencing the given master."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Booking
                res = await session.execute(select(Booking.id, Booking.status).where(Booking.master_id == master_tid))
                rows = res.all()
                return [(int(r[0]), r[1]) for r in rows]
        except Exception as e:
            logger.exception("AdminRepo.get_booking_ids_for_master failed for %s: %s", master_tid, e)
            return []

    @staticmethod
    async def get_active_future_booking_ids_for_master(master_tid: int) -> list[int]:
        """Return booking ids that are active/future and therefore block master deletion.

        Mirrors the logic from handlers (terminal states excluded and starts_at/ends_at checks).
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select, or_
                from bot.app.domain.models import Booking, BookingStatus
                now_utc = datetime.now(ZoneInfo("UTC"))
                terminal = [
                    BookingStatus.CANCELLED,
                    BookingStatus.DONE,
                    BookingStatus.NO_SHOW,
                    BookingStatus.EXPIRED,
                ]
                stmt = select(Booking.id).where(
                    Booking.master_id == master_tid,
                    ~Booking.status.in_(terminal),
                    or_(Booking.starts_at >= now_utc, Booking.ends_at >= now_utc),
                )
                res = await session.execute(stmt)
                return [int(x) for x in res.scalars().all()]
        except Exception as e:
            logger.exception("AdminRepo.get_active_future_booking_ids_for_master failed: %s", e)
            return []

    @staticmethod
    async def get_range_stats(kind: str, master_id: int | None = None) -> dict[str, Any]:
        """Return aggregate stats for the given period.

        If master_id is provided, restrict counts to that master.
        """
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func, and_
                from bot.app.domain.models import Booking, Master

                base_pred = Booking.starts_at.between(start, end)
                if master_id is not None:
                    total = await session.scalar(select(func.count(Booking.id)).where(and_(base_pred, Booking.master_id == int(master_id)))) or 0
                    unique_users = await session.scalar(select(func.count(func.distinct(Booking.user_id))).where(and_(base_pred, Booking.master_id == int(master_id)))) or 0
                    masters = 1
                else:
                    total = await session.scalar(select(func.count(Booking.id)).where(base_pred)) or 0
                    unique_users = await session.scalar(select(func.count(func.distinct(Booking.user_id))).where(base_pred)) or 0
                    masters = await session.scalar(select(func.count(func.distinct(Booking.master_id))).where(base_pred)) or 0

                days = max(1, (end - start).days)
                avg_per_day = (int(total) / days) if days else 0.0
                return {"bookings": int(total), "unique_users": int(unique_users), "masters": int(masters), "avg_per_day": avg_per_day}
        except Exception as e:
            logger.exception("AdminRepo.get_range_stats failed: %s", e)
            return {"bookings": 0, "unique_users": 0, "masters": 0, "avg_per_day": 0.0}

    @staticmethod
    async def get_top_masters(limit: int = 10) -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds("month")
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, Master
                stmt = (
                    select(Master.name, func.count(Booking.id).label("count"))
                    .join(Master, Booking.master_id == Master.telegram_id)
                    .where(Booking.starts_at.between(start, end))
                    .group_by(Master.name)
                    .order_by(func.count(Booking.id).desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                return [row._asdict() for row in res.all()]
        except Exception as e:
            logger.exception("AdminRepo.get_top_masters failed: %s", e)
            return []

    @staticmethod
    async def get_top_services(limit: int = 10) -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds("month")
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, Service
                stmt = (
                    select(Service.name.label("service"), func.count(Booking.id).label("count"))
                    .join(Service, Booking.service_id == Service.id)
                    .where(Booking.starts_at.between(start, end))
                    .group_by(Service.name)
                    .order_by(func.count(Booking.id).desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                return [row._asdict() for row in res.all()]
        except Exception as e:
            logger.exception("AdminRepo.get_top_services failed: %s", e)
            return []

    @staticmethod
    async def get_revenue_total(kind: str = "month", master_id: int | None = None) -> int:
        """Return total revenue (cents) for the given period.

        If master_id is provided, restrict to that master.
        """
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func, and_
                from bot.app.domain.models import Booking
                preds: list[Any] = [Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES))]
                if master_id is not None:
                    preds.append(Booking.master_id == int(master_id))
                stmt = select(func.coalesce(func.sum(_price_expr()), 0)).where(and_(*preds))
                revenue = int(await session.scalar(stmt) or 0)
                return revenue
        except Exception as e:
            logger.exception("AdminRepo.get_revenue_total failed: %s", e)
            return 0

    @staticmethod
    async def get_revenue_by_master(kind: str = "month", limit: int = 10) -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, Master
                # Include Master.telegram_id in the aggregation to avoid collisions when names duplicate
                stmt = (
                    select(
                        Master.telegram_id.label("telegram_id"),
                        Master.name,
                        func.sum(_price_expr()).label("revenue_cents"),
                        func.count(Booking.id).label("bookings"),
                    )
                    .join(Master, Booking.master_id == Master.telegram_id)
                    .where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))
                    .group_by(Master.telegram_id, Master.name)
                    .order_by(func.sum(_price_expr()).desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                return [row._asdict() for row in res.all()]
        except Exception as e:
            logger.exception("AdminRepo.get_revenue_by_master failed: %s", e)
            return []

    @staticmethod
    async def get_revenue_by_service(kind: str = "month", limit: int = 10) -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, Service
                stmt = (
                    select(Service.name.label("service"), func.sum(_price_expr()).label("revenue_cents"), func.count(Booking.id).label("bookings"))
                    .join(Service, Booking.service_id == Service.id)
                    .where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))
                    .group_by(Service.name)
                    .order_by(func.sum(_price_expr()).desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                return [row._asdict() for row in res.all()]
        except Exception as e:
            logger.exception("AdminRepo.get_revenue_by_service failed: %s", e)
            return []



    @staticmethod
    async def get_retention(kind: str = "month") -> dict[str, Any]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking
                subquery = (
                    select(Booking.user_id, func.count(Booking.id).label("c"))
                    .where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))
                    .group_by(Booking.user_id)
                    .subquery()
                )
                total_users = await session.scalar(select(func.count()).select_from(subquery)) or 0
                repeat_users = await session.scalar(select(func.count()).select_from(subquery).where(subquery.c.c > 1)) or 0
                rate = (repeat_users / total_users) if total_users else 0.0
                return {"repeaters": int(repeat_users), "total": int(total_users), "rate": rate}
        except Exception as e:
            logger.exception("AdminRepo.get_retention failed: %s", e)
            return {"repeaters": 0, "total": 0, "rate": 0.0}

    @staticmethod
    async def get_no_show_rates(kind: str = "month") -> dict[str, Any]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, BookingStatus
                base_query = select(Booking.id).where(Booking.starts_at.between(start, end), Booking.status.in_(_ACTIVE_FOR_NOSHOW_BASE)).subquery()
                total = await session.scalar(select(func.count()).select_from(base_query)) or 0
                no_shows = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end), Booking.status == BookingStatus.NO_SHOW)) or 0
                rate = (no_shows / total) if total else 0.0
                return {"no_show": int(no_shows), "total": int(total), "rate": rate}
        except Exception as e:
            logger.exception("AdminRepo.get_no_show_rates failed: %s", e)
            return {"no_show": 0, "total": 0, "rate": 0.0}

    @staticmethod
    async def get_top_clients_ltv(kind: str = "month", limit: int = 10) -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, User
                stmt = (
                    select(User.name, func.sum(_price_expr()).label("revenue_cents"), func.count(Booking.id).label("bookings"))
                    .join(User, Booking.user_id == User.id)
                    .where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))
                    .group_by(User.name)
                    .order_by(func.sum(_price_expr()).desc())
                    .limit(limit)
                )
                res = await session.execute(stmt)
                return [row._asdict() for row in res.all()]
        except Exception as e:
            logger.exception("AdminRepo.get_top_clients_ltv failed: %s", e)
            return []

    @staticmethod
    async def get_conversion(kind: str = "month") -> dict[str, Any]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, BookingStatus
                total_created = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end))) or 0
                converted = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end), Booking.status.in_({BookingStatus.PAID, BookingStatus.CONFIRMED}))) or 0
                rate = (converted / total_created) if total_created else 0.0
                return {"created": int(total_created), "converted": int(converted), "rate": rate}
        except Exception as e:
            logger.exception("AdminRepo.get_conversion failed: %s", e)
            return {"created": 0, "converted": 0, "rate": 0.0}

    @staticmethod
    async def get_cancellations(kind: str = "month") -> dict[str, Any]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking, BookingStatus
                total = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end))) or 0
                cancelled = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end), Booking.status == BookingStatus.CANCELLED)) or 0
                rate = (cancelled / total) if total else 0.0
                return {"cancelled": int(cancelled), "total": int(total), "rate": rate}
        except Exception as e:
            logger.exception("AdminRepo.get_cancellations failed: %s", e)
            return {"cancelled": 0, "total": 0, "rate": 0.0}
    @staticmethod
    async def get_daily_trends(kind: str = "month") -> list[dict[str, Any]]:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking
                date_trunc = func.date_trunc('day', Booking.starts_at)
                stmt = (
                    select(func.date(date_trunc).label('day'), func.count(Booking.id).label('bookings'), func.sum(_price_expr()).label('revenue_cents'))
                    .where(Booking.starts_at.between(start, end))
                    .group_by(func.date(date_trunc))
                    .order_by(func.date(date_trunc))
                )
                result = await session.execute(stmt)
                return [{"day": str(row.day), "bookings": int(row.bookings or 0), "revenue_cents": int(row.revenue_cents or 0)} for row in result.fetchall()]
        except Exception as e:
            logger.exception("AdminRepo.get_daily_trends failed: %s", e)
            return []

    @staticmethod
    async def get_aov(kind: str = "month") -> float:
        try:
            start, end = _range_bounds(kind)
            async with get_session() as session:
                from sqlalchemy import select, func
                from bot.app.domain.models import Booking
                revenue = await session.scalar(select(func.coalesce(func.sum(_price_expr()), 0)).where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))) or 0
                cnt = await session.scalar(select(func.count(Booking.id)).where(Booking.starts_at.between(start, end), Booking.status.in_(tuple(REVENUE_STATUSES)))) or 0
                return (revenue / cnt) if cnt else 0.0
        except Exception as e:
            logger.exception("AdminRepo.get_aov failed: %s", e)
            return 0.0

async def get_admin_dashboard_summary(lang: str | None = None) -> str:
    """Compatibility wrapper: return a localized text summary for admin dashboard.

    This function remains for backward compatibility with handlers that expect
    a pre-formatted string. Prefer calling `get_admin_dashboard_data` which
    returns raw structured data suitable for view-layer formatting.
    """
    try:
        data = await get_admin_dashboard_data()
        l = lang or data.get("language") or await SettingsRepo.get_setting("language", DEFAULT_LANGUAGE)
        # Build localized text from data (keeps previous formatting)
        try:
            date_label = datetime.now().strftime("%d %B")
            header_raw = tr("admin_dashboard_header", lang=l)
            header = header_raw.format(date=date_label) if "{date}" in header_raw else header_raw

            total_line = tr("admin_dashboard_total_bookings", lang=l).format(count=int(data.get("stats", {}).get("bookings", 0)))
            revenue_line = tr("admin_dashboard_revenue", lang=l).format(amount=(data.get("revenue_cents", 0) // 100))
            new_clients_line = tr("admin_dashboard_new_clients", lang=l).format(count=int(data.get("stats", {}).get("unique_users", 0)))
            master_load_header = tr("admin_dashboard_master_load", lang=l)

            masters_load_text_local = data.get("masters_text", "")
            text_root = "\n".join([header, total_line, revenue_line, new_clients_line, master_load_header, masters_load_text_local])
        except Exception:
            text_root = t("admin_panel_title", l)
        return text_root
    except Exception:
        return t("admin_panel_title", lang or default_language())


async def get_admin_dashboard_data(lang: str | None = None) -> dict[str, Any]:
    """Return structured admin dashboard data (no presentation).

    Returns a dict with keys: language, stats, revenue_cents, masters (list), masters_text.
    Handlers/views should take this data and render localized text/buttons.
    """
    l = lang or await SettingsRepo.get_setting("language", DEFAULT_LANGUAGE)
    stats = await AdminRepo.get_range_stats("today")
    revenue_cents = await AdminRepo.get_revenue_total("today")

    # One SQL to get bookings count per master (including zeros) for the same window
    start, end = _range_bounds("today")
    async with get_session() as session:
        stmt = (
            select(
                Master.name,
                Master.telegram_id,
                func.count(Booking.id).label("bookings"),
            )
            .select_from(Master)
            .join(
                Booking,
                and_(
                    Booking.master_id == Master.telegram_id,
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(tuple(REVENUE_STATUSES)),
                ),
                isouter=True,
            )
            .group_by(Master.telegram_id, Master.name)
            .order_by(Master.name)
        )
        rows = (await session.execute(stmt)).all()
        default_slots = DEFAULT_DAILY_SLOTS

        masters_lines: list[str] = []
        zero_names: list[str] = []
        for name, _tid, cnt in rows:
            cnt_int = int(cnt or 0)
            if cnt_int > 0:
                masters_lines.append(f"• {name}: {cnt_int}/{default_slots} слотов")
            else:
                zero_names.append(name)

    masters_load_text = "\n".join(masters_lines)
    if zero_names:
        zero_text = ", ".join(zero_names[:5])
        if len(zero_names) > 5:
            zero_text += ", ..."
        masters_load_text = (masters_load_text + "\n" if masters_load_text else "") + f"(Нет записей: {zero_text})"

    # Also include a simple masters list (raw) for views that want to render differently
    masters_raw = [
        {"name": name, "telegram_id": int(tid or 0), "bookings": int(cnt or 0)} for name, tid, cnt in rows
    ]

    return {
        "language": l,
        "stats": stats,
        "revenue_cents": revenue_cents,
        "masters": masters_raw,
        "masters_text": masters_load_text,
    }


# Note: thin facade helpers (get_service, services_cache, get_service_name,
# get_setting, get_hold_minutes, update_setting) have been removed. Call
# the repository APIs directly: `ServiceRepo` and `SettingsRepo`.

# Статусы, учитываемые при подсчете выручки
# Revenue is recognized for PAID and CONFIRMED (cash) and optionally DONE

# Статусы, которые считаются "активными" для расчета неявок
_ACTIVE_FOR_NOSHOW_BASE = {
    BookingStatus.PAID,
    BookingStatus.DONE,
    BookingStatus.NO_SHOW,
    BookingStatus.ACTIVE,
    BookingStatus.PENDING_PAYMENT,
    BookingStatus.AWAITING_CASH,
    BookingStatus.CONFIRMED,
    BookingStatus.RESERVED,
}


def _range_bounds(kind: str) -> tuple[datetime, datetime]:
    """Возвращает временные рамки (начало, конец) для периода.

    Args:
        kind: Тип периода ('week' или 'month').

    Returns:
        Кортеж (начало, конец) с локальной временной зоной.
    """
    now = datetime.now(LOCAL_TZ or UTC)
    if kind == "week":
        week_start = now - timedelta(days=now.weekday())
        start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        try:
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        except Exception:
            # Fallback: construct first-of-month explicitly; do NOT use sliding 30-day window
            try:
                start = datetime(year=now.year, month=now.month, day=1, tzinfo=now.tzinfo).replace(hour=0, minute=0, second=0, microsecond=0)
            except Exception:
                start = now  # last-resort fallback
    logger.debug("Рассчитаны рамки периода %s: start=%s, end=%s", kind, start, now)
    return start, now


def _price_expr() -> Any:
    """Возвращает SQLAlchemy выражение для цены.

    Returns:
        Выражение для final_price_cents или original_price_cents.
    """
    return func.coalesce(Booking.final_price_cents, Booking.original_price_cents, 0)


# Note: analytics facades (thin proxies to AdminRepo) were removed to
# reduce duplication. Callers should import AdminRepo and call the
# desired async methods directly, for example:
#   from bot.app.services.admin_services import AdminRepo
#   totals = await AdminRepo.get_basic_totals()


__all__ = [
    # Public repository classes and helpers
    "ServiceRepo",
    "SettingsRepo",
    "load_settings_from_db",
    "AdminRepo",
    "invalidate_services_cache",
    # Note: lightweight facades were removed; call repository APIs directly
]
# Note: Proxy analytics functions were intentionally removed. Callers should
# import and call AdminRepo.<method>() directly (e.g. AdminRepo.get_basic_totals()).