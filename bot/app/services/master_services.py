from __future__ import annotations

import logging
from datetime import UTC, datetime, date as _date, time as _time, timedelta
from typing import Any, Dict, List, Mapping, Optional
import re

from sqlalchemy import select, and_
from sqlalchemy.exc import SQLAlchemyError

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, MasterClientNote, User
from bot.app.services.shared_services import get_service_name, format_money_cents
from bot.app.translations import tr
from bot.app.services.admin_services import _price_expr, _REVENUE_STATUSES
import bot.config as cfg

logger = logging.getLogger(__name__)


async def fetch_booking_safe(booking_id: int) -> Any | None:
    """Safe wrapper around booking fetch that swallows DB errors."""
    try:
        # Direct DB access (avoid runtime import fallbacks)
        async with get_session() as session:
            return await session.get(Booking, booking_id)
    except Exception as e:
        logger.warning("fetch_booking_safe: DB access failed %s", e)
        return None


async def enrich_booking_context(booking: Any) -> tuple[Any | None, str]:
    """Return (client_obj, service_name) for a booking.

    Uses get_user_by_id and get_service_name; falls back to raw ids on failure.
    """
    client = None
    service_name = getattr(booking, "service_id", "—")
    try:
        if getattr(booking, "user_id", None):
            async with get_session() as session:
                client = await session.get(User, booking.user_id)
    except Exception:
        client = None
    try:
        service_name = await get_service_name(getattr(booking, "service_id", ""))
    except Exception:
        service_name = str(getattr(booking, "service_id", "—"))
    return client, service_name


async def build_client_history_view(booking_id: int) -> str | None:
    """Return a formatted client history view for master UI or None.

    This function composes the view using existing history service.
    """
    try:
        booking = await fetch_booking_safe(booking_id)
        if not booking:
            return None
        master_id = getattr(booking, "master_id", None)
        if master_id is None:
            return None
        # Try to use external client_history service if present; otherwise use internal helper
        try:
            from bot.app.services.client_history_service import get_client_history

            hist = await get_client_history(master_id, booking.user_id)
            if not hist:
                return None
            return format_client_history(hist, booking.user_id)
        except Exception:
            # Fallback to internal history builder
            try:
                hist_map = await get_client_history_for_master(booking_id)
                if not hist_map:
                    return None
                return format_client_history(hist_map, booking.user_id)
            except Exception:
                return None
    except Exception as e:
        logger.warning("build_client_history_view failed: %s", e)
        return None


def format_client_history(hist: Mapping, user_id: int) -> str:
    """Format client history mapping into a short text block for master UI."""
    try:
        lines: list[str] = [f"🗂️ История клиента #{user_id}"]
        visits = hist.get("visits", 0)
        spent = hist.get("total_spent_cents", 0)
        basic_fields = [
            ("Имя", hist.get("name")),
            ("Визитов", visits),
            ("Сумма", format_money_cents(spent)),
            ("Последний визит", hist.get("last_visit")),
            ("Оценка (последняя)", (f"{hist.get('rating')}⭐" if isinstance(hist.get("rating"), (int, float)) else None)),
            ("Заметка", hist.get("note")),
        ]
        for label, value in basic_fields:
            if value is None or value == "":
                continue
            lines.append(f"{label}: {value}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("format_client_history failed: %s", e)
        return ""


async def get_master_bookings_for_period(
    master_telegram_id: int,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    days: int | None = 7,
) -> List[Booking]:
    """Получает записи мастера за указанный период.

    This function supports two calling conventions for backward compatibility:
    - legacy: pass `days=int` (as before) — it will use now..now+days
    - preferred: pass explicit `start` (datetime) and optional `end` (datetime)

    If neither `start` nor `days` is provided, `days=7` is used.
    """
    try:
        if start is None:
            base = datetime.now(UTC)
            # If caller explicitly passed days=None, interpret as open-ended (all future)
            if days is None:
                start = base
                end = None
            else:
                # default behaviour: bounded window of `days` ahead
                start = base
                end = base + timedelta(days=days)
        else:
            # start provided; if end not provided and days given, compute end
            if end is None:
                if days is not None:
                    end = start + timedelta(days=days)
                else:
                    # caller requested open-ended window (no upper bound)
                    end = None

        async with get_session() as session:
            stmt = select(Booking).where(Booking.master_id == master_telegram_id)
            if end is not None:
                stmt = stmt.where(Booking.starts_at.between(start, end))
            else:
                stmt = stmt.where(Booking.starts_at >= start)

            # Exclude terminal/irrelevant statuses at DB level so callers
            # receive only active/upcoming bookings. This improves
            # performance when the table is large.
            stmt = stmt.where(
                ~Booking.status.in_(
                    (
                        BookingStatus.CANCELLED,
                        BookingStatus.DONE,
                        BookingStatus.NO_SHOW,
                        BookingStatus.EXPIRED,
                    )
                )
            ).order_by(Booking.starts_at)

            result = await session.execute(stmt)
            bookings = list(result.scalars().all())
            logger.info(
                "Получено %d записей для мастера %s в интервале %s - %s",
                len(bookings),
                master_telegram_id,
                start,
                end,
            )
            return bookings
    except SQLAlchemyError as e:
        logger.error("Ошибка получения записей для мастера %s: %s", master_telegram_id, e)
        return []


async def check_future_booking_conflicts(
    master_telegram_id: int,
    *,
    day_to_clear: int | None = None,
    clear_all: bool = False,
    horizon_days: int = 365,
) -> list[str]:
    """Return a list of human-readable conflict strings for future bookings.

    - If day_to_clear is provided, only bookings that fall into that weekday's
      configured windows are considered.
    - If clear_all is True, all weekdays configured in the master's schedule are
      considered.
    - horizon_days bounds how far into the future we scan (default 365 days).

    Returns a list of formatted strings like '#<id> 2025-01-01 09:00 — <client>'.
    """
    try:
        from datetime import timezone

        now = datetime.now(UTC)
        end = now + timedelta(days=horizon_days)

        # Load canonical schedule
        sched = await get_master_schedule(master_telegram_id) or {}

        # Determine which weekdays to check
        if clear_all:
            days_to_check = set(range(7))
        elif day_to_clear is not None:
            days_to_check = {int(day_to_clear)}
        else:
            # nothing to check
            return []

        bookings = await get_master_bookings_for_period(master_telegram_id, start=now, end=end)
        conflicts: list[str] = []
        for booking in (bookings or []):
            try:
                # Skip bookings that are already finished/cancelled — they should
                # not block clearing of schedule.
                try:
                    st = getattr(booking, "status", None)
                    if st in (BookingStatus.CANCELLED, BookingStatus.DONE, BookingStatus.EXPIRED, BookingStatus.NO_SHOW):
                        continue
                except Exception:
                    pass
                starts = getattr(booking, "starts_at", None)
                if not starts:
                    continue
                weekday = starts.weekday()
                if weekday not in days_to_check:
                    continue

                day_windows = sched.get(str(weekday)) or []
                if not day_windows:
                    continue

                start_min = starts.hour * 60 + starts.minute
                matched = False
                for w in (day_windows or []):
                    try:
                        if isinstance(w, (list, tuple)) and len(w) >= 2:
                            a, b = w[0], w[1]
                        else:
                            a, b = str(w).split("-")
                        a_h, a_m = map(int, a.split(":"))
                        b_h, b_m = map(int, b.split(":"))
                        a_min = a_h * 60 + a_m
                        b_min = b_h * 60 + b_m
                        if start_min >= a_min and start_min < b_min:
                            matched = True
                            break
                    except Exception:
                        continue
                if not matched:
                    continue

                # Enrich client name and include status in debug output
                try:
                    client, _ = await enrich_booking_context(booking)
                    user_name = getattr(client, "name", None) or f"id:{getattr(client, 'id', '?')}"
                except Exception:
                    user_name = f"id:{getattr(booking, 'user_id', '?')}"
                try:
                    iso = starts.isoformat()
                except Exception:
                    iso = starts.strftime('%Y-%m-%d %H:%M') if hasattr(starts, 'strftime') else str(starts)
                conflicts.append(
                    f"#{getattr(booking, 'id', '?')} {iso} (master={getattr(booking, 'master_id', '?')}) status={getattr(booking, 'status', '?')} — {user_name}"
                )
            except Exception:
                continue

        return conflicts
    except Exception as e:
        logger.exception("check_future_booking_conflicts failed for master %s: %s", master_telegram_id, e)
        return []


async def get_booking_details(booking_id: int) -> Optional[Dict[str, Any]]:
    """Получает детальную информацию о записи для мастера."""
    try:
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not booking:
                logger.warning("Запись не найдена: id=%s", booking_id)
                return None
            user = await session.get(User, booking.user_id) if getattr(booking, "user_id", None) else None
            service_name = await get_service_name(booking.service_id)
            texts = getattr(cfg, "MASTER_TEXT", {})
            details = {
                "id": booking.id,
                "client_name": (user.name if user else texts.get("unknown_client", "unknown")),
                # Provide telegram id and optional username separately so callers
                # can render a clickable tg://user link instead of printing the id
                "client_telegram_id": getattr(user, "telegram_id", None) if user else None,
                "client_username": getattr(user, "username", None) if user else None,
                # Keep phone only if user has an explicit phone field and it's
                # different from telegram_id; otherwise leave as None.
                "client_phone": (str(getattr(user, "phone", "")) if user and getattr(user, "phone", None) and str(getattr(user, "phone", "")).strip() and str(getattr(user, "phone", "")) != str(getattr(user, "telegram_id", "")) else None),
                "service_name": service_name,
                "date": booking.starts_at.strftime('%d.%m.%Y') if getattr(booking, "starts_at", None) else "",
                "time": booking.starts_at.strftime('%H:%M') if getattr(booking, "starts_at", None) else "",
                "status": getattr(booking.status, "value", str(getattr(booking, "status", "?"))),
            }
            logger.info("Детали записи #%s получены для мастера", booking_id)
            return details
    except SQLAlchemyError as e:
        logger.error("Ошибка получения деталей записи #%s: %s", booking_id, e)
        return None


async def update_booking_status(booking_id: int, new_status: BookingStatus) -> bool:
    try:
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not booking:
                logger.warning("Запись не найдена для обновления статуса: id=%s", booking_id)
                return False
            booking.status = new_status
            await session.commit()
            logger.info("Статус записи #%s обновлен на %s", booking_id, new_status)
            return True
    except SQLAlchemyError as e:
        logger.error("Ошибка обновления статуса записи #%s: %s", booking_id, e)
        return False


async def get_client_history_for_master(booking_id: int) -> Optional[Dict[str, Any]]:
    try:
        async with get_session() as session:
            current_booking = await session.get(Booking, booking_id)
            if not current_booking:
                logger.warning("Запись или пользователь не найдены: id=%s", booking_id)
                return None

            client_id = getattr(current_booking, "user_id", None)
            if client_id is None:
                logger.warning("У записи #%s отсутствует user_id", booking_id)
                return None
            master_id = current_booking.master_id

            history_stmt = (
                select(Booking)
                .where(Booking.user_id == client_id, Booking.master_id == master_id)
                .order_by(Booking.starts_at.desc())
            )
            history_result = await session.execute(history_stmt)
            all_bookings = history_result.scalars().all()

            note = await session.scalar(
                select(MasterClientNote.note).where(
                    and_(
                        MasterClientNote.master_telegram_id == master_id,
                        MasterClientNote.user_id == client_id,
                    )
                )
            )

            total_spent_cents = 0
            try:
                total_spent_cents = sum(
                    _price_expr().evaluate(b) for b in all_bookings if b.status in _REVENUE_STATUSES
                )
            except Exception as e:
                logger.error("Ошибка расчета total_spent_cents для записи #%s: %s", booking_id, e)

            user = await session.get(User, client_id)
            texts = getattr(cfg, "MASTER_TEXT", {})
            history = {
                "name": user.name if user else texts.get("unknown_client", "unknown"),
                "visits": len(all_bookings),
                "total_spent": format_money_cents(total_spent_cents, "UAH"),
                "last_visit": all_bookings[0].starts_at.strftime('%d.%m.%Y') if all_bookings else texts.get("no_visits", "Нет"),
                "note": note or texts.get("no_notes", "Нет заметок"),
            }
            logger.info("История клиента для записи #%s получена", booking_id)
            return history
    except SQLAlchemyError as e:
        logger.error("Ошибка получения истории клиента для записи #%s: %s", booking_id, e)
        return None


async def upsert_client_note(booking_id: int, note_text: str) -> bool:
    try:
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not (booking and booking.user_id and booking.master_id):
                logger.warning("Запись или необходимые поля не найдены: id=%s", booking_id)
                return False

            note = await session.scalar(
                select(MasterClientNote).where(
                    and_(
                        MasterClientNote.master_telegram_id == booking.master_id,
                        MasterClientNote.user_id == booking.user_id,
                    )
                )
            )
            if note:
                note.note = note_text
            else:
                note = MasterClientNote(
                    master_telegram_id=booking.master_id,
                    user_id=booking.user_id,
                    note=note_text,
                )
                session.add(note)
            await session.commit()
            logger.info("Заметка для записи #%s обновлена", booking_id)
            return True
    except SQLAlchemyError as e:
        logger.error("Ошибка обновления заметки для записи #%s: %s", booking_id, e)
        return False

async def ensure_booking_owner(user_id: int, booking_id: int) -> Optional[Booking]:
    """Проверяет, принадлежит ли запись пользователю (служебный метод).

    Возвращает объект Booking или None. Помещено в сервисный слой, чтобы
    инкапсулировать доступ к базе данных и избежать ленивых импортов в
    слое интерфейсов/хендлеров.
    """
    try:
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if booking and getattr(booking, "user_id", None) == user_id:
                logger.debug("ensure_booking_owner: booking #%s belongs to user %s", booking_id, user_id)
                return booking
        logger.debug("ensure_booking_owner: booking #%s does NOT belong to user %s", booking_id, user_id)
        return None
    except SQLAlchemyError as e:
        logger.error("ensure_booking_owner DB error for booking #%s: %s", booking_id, e)
        return None


async def get_master_stats_summary(master_id: int, days: int) -> Dict[str, Any]:
    try:
        now = datetime.now(UTC)
        bookings = await get_master_bookings_for_period(master_id, days=days)
        next_booking = min(
            (b for b in bookings if b.starts_at > now),
            key=lambda b: b.starts_at,
            default=None,
        )
        texts = getattr(cfg, "MASTER_TEXT", {})
        stats = {
            "next_booking_time": (
                next_booking.starts_at.strftime('%d.%m в %H:%M') if next_booking else texts.get("no_bookings", "Нет")
            ),
            "total_bookings": len(bookings),
            "completed_bookings": sum(1 for b in bookings if b.status == BookingStatus.DONE),
            "pending_payment": sum(
                1
                for b in bookings
                if b.status in {
                    getattr(BookingStatus, "AWAITING_CASH", object()),
                    BookingStatus.PENDING_PAYMENT,
                    BookingStatus.RESERVED,
                }
            ),
            "no_shows": sum(1 for b in bookings if b.status == BookingStatus.NO_SHOW),
        }
        logger.info("Статистика мастера %s за %d дней получена", master_id, days)
        return stats
    except Exception as e:
        logger.error("Ошибка получения статистики мастера %s: %s", master_id, e)
        return {
            "next_booking_time": getattr(cfg, "MASTER_TEXT", {}).get("no_bookings", "Нет"),
            "total_bookings": 0,
            "completed_bookings": 0,
            "pending_payment": 0,
            "no_shows": 0,
        }


async def get_master_profile_data(master_id: int) -> Optional[Dict[str, Any]]:
    """Fetch master profile data only (no formatting).

    Returns a dict with keys: master, services, durations_map, about_text, reviews
    or None if master not found.
    """
    try:
        from bot.app.domain.models import Master, Service, MasterService, MasterProfile, BookingRating, Booking

        async with get_session() as session:
            master = await session.get(Master, master_id)
            if not master:
                return None

            # services
            result = await session.execute(
                select(Service.id, Service.name, Service.category, Service.price_cents, Service.currency)
                .join(MasterService, MasterService.service_id == Service.id)
                .where(MasterService.master_telegram_id == master_id)
            )
            services = list(result.all())

            # profile durations/about
            durations_map: dict[str, int] = {}
            about_text: str | None = None
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_id))
            if prof and getattr(prof, "bio", None):
                try:
                    import json
                    data = json.loads(prof.bio or "{}") or {}
                    raw = data.get("durations") or {}
                    if isinstance(raw, dict):
                        durations_map = {str(k): int(v) for k, v in raw.items() if isinstance(v, (int, str)) and str(v).isdigit()}
                    about_text = data.get("about") or data.get("bio") or data.get("desc")
                except Exception:
                    durations_map = {}
                    about_text = None
            # schedule (canonicalized) — attempt to reuse get_master_schedule normalization
            try:
                schedule = await get_master_schedule(master_id)
            except Exception:
                schedule = {}

            # last two reviews
            result = await session.execute(
                select(BookingRating.rating, BookingRating.comment)
                .join(Booking, Booking.id == BookingRating.booking_id)
                .where(Booking.master_id == master_id)
                .order_by(BookingRating.id.desc())
                .limit(2)
            )
            reviews = list(result.all())

        return {
            "master": master,
            "services": services,
            "durations_map": durations_map,
            "about_text": about_text,
            "schedule": schedule,
            "reviews": reviews,
        }
    except Exception as e:
        logger.exception("get_master_profile_data failed: %s", e)
        return None


async def get_booking_display_data(booking_id: int) -> Optional[Dict[str, Any]]:
    """Return a display-friendly data dict for a booking.

    The dict includes keys:
      - booking_id
      - service_name
      - master_name
      - price_cents
      - currency
      - starts_at (datetime)
      - client_id
      - client_name
      - client_telegram_id

    Returns None on failure or missing booking.
    """
    try:
        async with get_session() as session:
            b = await session.get(Booking, booking_id)
            if not b:
                return None

            # Resolve client
            client = await session.get(User, getattr(b, "user_id", None)) if getattr(b, "user_id", None) else None

            # Resolve service name (support multi-service bookings)
            service_name = None
            try:
                from bot.app.domain.models import BookingItem, Service as Svc

                rows = list((await session.execute(
                    select(BookingItem.service_id, Svc.name)
                    .join(Svc, Svc.id == BookingItem.service_id)
                    .where(BookingItem.booking_id == booking_id)
                )).all())
                if rows:
                    service_name = " + ".join([r[1] or str(r[0]) for r in rows])
            except Exception:
                service_name = None

            if not service_name:
                try:
                    service_name = await get_service_name(getattr(b, "service_id", ""))
                except Exception:
                    service_name = str(getattr(b, "service_id", ""))

            # Resolve master name
            master_name = None
            try:
                from bot.app.domain.models import Master

                res = await session.execute(select(Master.name).where(Master.telegram_id == getattr(b, "master_id", 0)))
                master_name = res.scalar_one_or_none()
            except Exception:
                master_name = None

            # Price
            price_cents = getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", None) or 0
            currency = getattr(b, "currency", None) or "UAH"

            data = {
                "booking_id": getattr(b, "id", booking_id),
                "service_name": service_name,
                "master_name": master_name,
                "price_cents": price_cents,
                "currency": currency,
                "starts_at": getattr(b, "starts_at", None),
                "client_id": getattr(client, "id", None) if client else getattr(b, "user_id", None),
                "client_name": getattr(client, "name", None) if client else None,
                "client_telegram_id": getattr(client, "telegram_id", None) if client else None,
            }
            return data
    except Exception as e:
        logger.exception("get_booking_display_data failed: %s", e)
        return None


async def get_master_schedule(master_telegram_id: int) -> dict:
    """Возвращает расписание мастера, хранящееся в MasterProfile.bio как JSON.

    Формат: {"schedule": {"0": [["09:00","12:00"], ...], "1": [...], ...}}
    Возвращает пустой dict при отсутствии данных или на ошибке.
    """
    try:
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import MasterProfile
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
            if not prof or not getattr(prof, "bio", None):
                return {}
            import json
            data = json.loads(prof.bio or "{}") or {}
            sched = data.get("schedule") or {}
            # normalize and return canonical shape
            return _normalize_schedule(sched)
    except Exception as e:
        logger.warning("get_master_schedule failed for %s: %s", master_telegram_id, e)
        return {}


async def set_master_schedule(master_telegram_id: int, schedule: dict) -> bool:
    """Replace the master's schedule stored inside MasterProfile.bio.schedule with given dict.

    `schedule` should be a mapping where keys are weekday numbers (0=Mon..6=Sun) either as int or str
    and values are lists of [start, end] string pairs like [["09:00","12:00"], ["13:00","17:00"]].
    """
    try:
        # Strict input validation: only accept dict to avoid accidental
        # overwrites when callers pass a list (this caused the bug where
        # one-day changes erased the whole week's schedule).
        if not isinstance(schedule, dict):
            logger.warning("set_master_schedule called with non-dict type=%s for master=%s; rejecting", type(schedule), master_telegram_id)
            return False
        import json
        async with get_session() as session:
            from bot.app.domain.models import MasterProfile
            from sqlalchemy import select
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))

            # normalize schedule before persisting
            canonical = _normalize_schedule(schedule or {})
            if not prof:
                # create profile
                prof = MasterProfile(master_telegram_id=master_telegram_id, bio=json.dumps({"schedule": canonical}))
                session.add(prof)
            else:
                try:
                    bio_obj = json.loads(prof.bio or "{}") or {}
                except Exception:
                    bio_obj = {}
                bio_obj["schedule"] = {str(k): v for k, v in canonical.items()}
                prof.bio = json.dumps(bio_obj)
            await session.commit()

        logger.info("Schedule set for master %s", master_telegram_id)
        return True
    except Exception as e:
        logger.exception("Failed to set schedule for master %s: %s", master_telegram_id, e)
        return False


async def set_master_schedule_day(master_telegram_id: int, day: int, windows: list[list[str]]) -> bool:
    """Set windows for a single weekday (day: 0..6). Pass windows=[] to clear the day.
    windows should be list of [start, end] pairs as strings.
    """
    try:
        sched = await get_master_schedule(master_telegram_id) or {}
        sched[str(day)] = windows
        return await set_master_schedule(master_telegram_id, sched)
    except Exception as e:
        logger.exception("Failed to set schedule day for master %s day=%s: %s", master_telegram_id, day, e)
        return False


async def get_master_bio(master_telegram_id: int) -> dict:
    """Return the full MasterProfile.bio parsed as dict (or {} on error)."""
    try:
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import MasterProfile
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
            if not prof or not getattr(prof, "bio", None):
                return {}
            import json
            return json.loads(prof.bio or "{}") or {}
    except Exception as e:
        logger.warning("get_master_bio failed for %s: %s", master_telegram_id, e)
        return {}


async def update_master_bio(master_telegram_id: int, bio: dict) -> bool:
    """Overwrite MasterProfile.bio with given dict (merges existing keys if necessary externally)."""
    try:
        import json
        async with get_session() as session:
            from bot.app.domain.models import MasterProfile
            from sqlalchemy import select
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
            if not prof:
                prof = MasterProfile(master_telegram_id=master_telegram_id, bio=json.dumps(bio))
                session.add(prof)
            else:
                prof.bio = json.dumps(bio)
            await session.commit()
        logger.info("Updated master bio for %s", master_telegram_id)
        return True
    except Exception as e:
        logger.exception("Failed to update master bio for %s: %s", master_telegram_id, e)
        return False


def _parse_hm_to_minutes(hm: str) -> int:
    """Parse 'HH:MM' into minutes since midnight."""
    try:
        parts = str(hm).split(":")
        h = int(parts[0]) if parts and parts[0] != "" else 0
        m = int(parts[1]) if len(parts) > 1 else 0
        return max(0, min(23, h)) * 60 + max(0, min(59, m))
    except Exception:
        return 0


def _minutes_to_hm(minutes: int) -> str:
    minutes = max(0, min(24 * 60 - 1, int(minutes)))
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def _normalize_schedule(schedule: dict | None) -> dict:
    """Normalize various schedule shapes into canonical mapping of str(weekday)->list[[HH:MM,HH:MM],...].

    This consolidates the duplicated inner functions used elsewhere.
    """
    out: dict = {}
    if not schedule:
        return out
    for k, v in (schedule or {}).items():
        key = str(k)
        vals = v or []
        normalized_windows: list[list[str]] = []
        if not isinstance(vals, (list, tuple)):
            out[key] = []
            continue
        for item in vals:
            try:
                # pair-like item
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    a = str(item[0]).strip()
                    b = str(item[1]).strip()
                    if re.match(r"^\d{1,2}:\d{2}$", a) and re.match(r"^\d{1,2}:\d{2}$", b):
                        a_h, a_m = list(map(int, a.split(":")))
                        b_h, b_m = list(map(int, b.split(":")))
                        a_norm = f"{a_h:02d}:{a_m:02d}"
                        b_norm = f"{b_h:02d}:{b_m:02d}"
                        if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                            normalized_windows.append([a_norm, b_norm])
                    else:
                        # try recover from single-string inside tuple/list
                        s = str(item[0])
                        if "-" in s:
                            parts = s.split("-", 1)
                            if (
                                len(parts) == 2
                                and re.match(r"^\d{1,2}:\d{2}$", parts[0].strip())
                                and re.match(r"^\d{1,2}:\d{2}$", parts[1].strip())
                            ):
                                a = parts[0].strip()
                                b = parts[1].strip()
                                a_h, a_m = list(map(int, a.split(":")))
                                b_h, b_m = list(map(int, b.split(":")))
                                a_norm = f"{a_h:02d}:{a_m:02d}"
                                b_norm = f"{b_h:02d}:{b_m:02d}"
                                if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                                    normalized_windows.append([a_norm, b_norm])
                else:
                    # legacy string like '09:00-12:00'
                    s = str(item)
                    if "-" in s:
                        a, b = s.split("-", 1)
                        a = a.strip()
                        b = b.strip()
                        if re.match(r"^\d{1,2}:\d{2}$", a) and re.match(r"^\d{1,2}:\d{2}$", b):
                            a_h, a_m = list(map(int, a.split(":")))
                            b_h, b_m = list(map(int, b.split(":")))
                            a_norm = f"{a_h:02d}:{a_m:02d}"
                            b_norm = f"{b_h:02d}:{b_m:02d}"
                            if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                                normalized_windows.append([a_norm, b_norm])
            except Exception:
                continue
        out[key] = normalized_windows
    return out


def _parse_windows_from_bio(bio: dict | None, target_date: _date | datetime) -> list[tuple[_time, _time]]:
    """Parse MasterProfile.bio dict and return list of (time, time) windows for target_date.

    This encapsulates exceptions, weekly schedule and settings fallbacks. It accepts
    a pre-parsed bio dictionary (as returned by get_master_bio) so callers that already
    loaded bio can reuse it without causing extra DB queries.
    """
    try:
        if bio is None:
            return [( _time(hour=9), _time(hour=18) )]
        # Normalize target_date to a date object
        if isinstance(target_date, datetime):
            td = target_date.date()
        else:
            td = target_date

        # Exceptions override specific dates
        exceptions = bio.get("exceptions") or {}
        day_key = td.isoformat()
        windows: list[tuple[_time, _time]] = []
        if exceptions and day_key in exceptions:
            exc = exceptions.get(day_key) or {}
            if exc.get("off"):
                return []
            date_windows = exc.get("windows") or []
            for rng in date_windows:
                try:
                    if isinstance(rng, (list, tuple)) and len(rng) >= 2:
                        a = str(rng[0])
                        b = str(rng[1])
                    else:
                        s = str(rng)
                        if "-" in s:
                            a, b = s.split("-", 1)
                        else:
                            continue
                    a = a.strip()
                    b = b.strip()
                    # validate HH:MM
                    if re.match(r"^\d{1,2}:\d{2}$", a) and re.match(r"^\d{1,2}:\d{2}$", b):
                        a_h, a_m = list(map(int, a.split(":")))
                        b_h, b_m = list(map(int, b.split(":")))
                        windows.append((_time(hour=a_h, minute=a_m), _time(hour=b_h, minute=b_m)))
                except Exception:
                    continue
            return windows

        # No date-specific exception -> check canonical weekly schedule
        schedule = bio.get("schedule") or {}
        # Ensure schedule is normalized shape (strings 'HH:MM') using existing normalizer
        try:
            schedule_norm = _normalize_schedule(schedule)
        except Exception:
            schedule_norm = {}

        wd = td.weekday()
        day_windows = schedule_norm.get(str(wd)) or []
        for rng in day_windows:
            try:
                a = str(rng[0]).strip()
                b = str(rng[1]).strip()
                a_h, a_m = list(map(int, a.split(":")))
                b_h, b_m = list(map(int, b.split(":")))
                windows.append((_time(hour=a_h, minute=a_m), _time(hour=b_h, minute=b_m)))
            except Exception:
                continue

        # If still empty, check settings for defaults
        if not windows:
            settings = bio.get("settings") or {}
            s = settings.get("start_hour")
            e = settings.get("end_hour")
            try:
                if s and e:
                    sh_parts = [int(x) for x in str(s).split(":")[:2]]
                    eh_parts = [int(x) for x in str(e).split(":")[:2]]
                    windows = [(_time(hour=sh_parts[0], minute=sh_parts[1]), _time(hour=eh_parts[0], minute=eh_parts[1]))]
            except Exception:
                windows = []

        if not windows:
            windows = [(_time(hour=9), _time(hour=18))]
        return windows
    except Exception:
        return [(_time(hour=9), _time(hour=18))]


async def get_work_windows_for_day(master_telegram_id: int, target_date: _date | datetime) -> list[tuple[_time, _time]]:
    """Async helper: fetch MasterProfile.bio and return work windows for target_date.

    This is the externally visible helper recommended for use by clients needing
    the canonical work windows for a master on a given date.
    """
    try:
        bio = await get_master_bio(master_telegram_id)
        return _parse_windows_from_bio(bio, target_date)
    except Exception:
        return [(_time(hour=9), _time(hour=18))]


def insert_window(schedule: dict | None, day: int, start: str, end: str, adjacency_min: int = 0) -> dict:
    """Insert a time window into schedule[day], merging overlaps and normalizing.

    - schedule: dict as returned by get_master_schedule (may be None).
    - day: weekday number 0..6
    - start/end: strings 'HH:MM'
    - adjacency_min: merge windows that are within this many minutes

    Returns normalized schedule dict (mutated copy).
    """
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    key = str(day)
    existing = out.get(key) or []
    # Normalize existing windows into minute tuples
    tuples: list[tuple[int, int]] = []
    for w in existing:
        try:
            s = _parse_hm_to_minutes(w[0])
            e = _parse_hm_to_minutes(w[1])
            if e > s:
                tuples.append((s, e))
        except Exception:
            continue

    # Validate input strings before parsing. If caller passed malformed
    # strings, refuse to modify schedule and log a warning. This prevents a
    # pattern where invalid input becomes 00:00 due to parsing fallback.
    time_re = re.compile(r"^\d{1,2}:\d{2}$")
    if not time_re.match(str(start)) or not time_re.match(str(end)):
        logger.warning("insert_window: invalid time strings start=%r end=%r", start, end)
        return out

    s_new = _parse_hm_to_minutes(start)
    e_new = _parse_hm_to_minutes(end)
    if e_new <= s_new:
        # invalid window; ignore and return original schedule
        return out

    tuples.append((s_new, e_new))
    # sort and merge
    tuples.sort(key=lambda x: x[0])
    merged: list[tuple[int, int]] = []
    for s, e in tuples:
        if not merged:
            merged.append((s, e))
            continue
        last_s, last_e = merged[-1]
        if s <= last_e + adjacency_min:
            # overlap or adjacent -> merge
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))

    # format back
    out[key] = [[_minutes_to_hm(s), _minutes_to_hm(e)] for s, e in merged]
    return out


def remove_all_windows(schedule: dict | None, day: int) -> dict:
    """Mark given day as empty list (workday cleared)."""
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    out[str(day)] = []
    return out


def copy_day(schedule: dict | None, target_day: int, source_day: int, mode: str = "replace") -> dict:
    """Copy windows from source_day to target_day.

    mode: 'replace' (default) or 'append' (append and normalize)
    """
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    src = out.get(str(source_day)) or []
    if mode == "replace":
        out[str(target_day)] = [list(w) for w in src]
        return out

    # append mode: insert each window and normalize
    result = out.get(str(target_day)) or []
    tmp = {**out}
    tmp[str(target_day)] = [list(w) for w in result]
    for w in src:
        if not (isinstance(w, (list, tuple)) and len(w) >= 2):
            continue
        tmp = insert_window(tmp, target_day, str(w[0]), str(w[1]))
    return tmp


def render_schedule_table(schedule: dict | None) -> str:
    """Render schedule dict into human-readable multi-line table for Mon..Sun."""
    sched = schedule or {}
    days = tr("weekday_short") or ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    lines: list[str] = []
    for idx, name in enumerate(days):
        w = sched.get(str(idx)) or sched.get(idx) or []
        if not w:
            lines.append(f"{name}: выходной")
            continue
        parts = []
        for rng in (w or []):
            try:
                parts.append(f"{str(rng[0])}-{str(rng[1])}")
            except Exception:
                continue
        lines.append(f"{name}: {', '.join(parts) if parts else 'выходной'}")
    return "\n".join(lines)


__all__ = [
    "get_master_bookings_for_period",
    "get_booking_details",
    "update_booking_status",
    "get_client_history_for_master",
    "upsert_client_note",
    "get_master_stats_summary",
    "fetch_booking_safe",
    "enrich_booking_context",
    "build_client_history_view",
    "format_client_history",
    "get_master_profile_data",
    "get_booking_display_data",
]