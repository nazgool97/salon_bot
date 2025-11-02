from __future__ import annotations
import logging
from datetime import datetime, time as dtime, timedelta, UTC
from typing import Any, Dict, List, Sequence

from sqlalchemy import select, and_, func
from sqlalchemy.exc import SQLAlchemyError

from bot.app.domain.models import (
    Booking,
    BookingStatus,
    Service,
    ServiceProfile,
    User,
    BookingRating,
    MasterProfile,
    BookingItem,
)
from bot.app.core.db import get_session
from bot.app.services import master_services
import bot.config as cfg

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


async def get_user_locale(telegram_id: int) -> str:
    """Получает локаль пользователя по Telegram ID или возвращает глобальную локаль.

    Args:
        telegram_id: Telegram user id.

    Returns:
        Код локали (например, 'uk').
    """
    try:
        async with get_session() as session:
            from sqlalchemy import select
            result = await session.execute(select(User.locale).where(User.telegram_id == telegram_id))
            locale = result.scalar_one_or_none()
            if locale:
                logger.debug("Локаль пользователя %s: %s", telegram_id, locale)
                return locale
    except Exception as e:
        logger.debug("Ошибка получения локали пользователя %s: %s", telegram_id, e)
    default_locale = getattr(cfg, "SETTINGS", {}).get("language", "uk")
    logger.debug("Используется локаль по умолчанию для пользователя %s: %s", telegram_id, default_locale)
    return default_locale

# Локальная таймзона: из SETTINGS['timezone'] или fallback на Europe/Kyiv
try:
    _tz_name = str(getattr(cfg, "SETTINGS", {}).get("timezone", "Europe/Kyiv"))
    LOCAL_TZ = ZoneInfo(_tz_name)
except Exception:
    LOCAL_TZ = ZoneInfo("Europe/Kyiv")

async def get_or_create_user(telegram_id: int, name: str | None = None, username: str | None = None) -> User:
    """Find or create a User by Telegram ID. Also persist the latest username when provided.

    Args:
        telegram_id: Telegram user id.
        name: Display/full name (optional).
        username: Telegram @username (without @), optional — will be saved when provided.

    Returns:
        User ORM object.
    """
    try:
        async with get_session() as session:
            user = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if user:
                changed = False
                # Update username if provided and different
                if username and getattr(user, "username", None) != username:
                    try:
                        user.username = username
                        changed = True
                    except Exception:
                        # defensive: some schemas may not have username column
                        pass
                # Update name if provided and different
                if name and getattr(user, "name", None) != name:
                    try:
                        user.name = name
                        changed = True
                    except Exception:
                        pass
                if changed:
                    await session.commit()
                    logger.info("Обновлен пользователь: telegram_id=%s, name=%s, username=%s", telegram_id, getattr(user, "name", None), getattr(user, "username", None))
                else:
                    logger.debug("Пользователь %s найден (без изменений)", telegram_id)
                return user

            # create new
            new_user = User(telegram_id=telegram_id, name=name or (username or str(telegram_id)))
            # set username if the model supports it
            try:
                new_user.username = username
            except Exception:
                pass
            session.add(new_user)
            await session.commit()
            logger.info("Создан новый пользователь: telegram_id=%s, name=%s, username=%s", telegram_id, new_user.name, getattr(new_user, "username", None))
            return new_user
    except SQLAlchemyError as e:
        logger.error("Ошибка создания/поиска пользователя %s: %s", telegram_id, e)
        raise

async def get_available_time_slots(date: datetime, master_id: int, service_duration_min: int) -> List[dtime]:
    try:
        local_day_start = date.replace(tzinfo=LOCAL_TZ)
        local_day_end = local_day_start + timedelta(days=1)
        day_start_utc = local_day_start.astimezone(UTC)
        day_end_utc = local_day_end.astimezone(UTC)

        async with get_session() as session:
            try:
                hold_minutes = int(getattr(cfg, "SETTINGS", {}).get("hold_minutes", 1))
            except Exception:
                hold_minutes = 1
            now_utc = datetime.now(UTC)
            result = await session.execute(
                select(Booking.starts_at, Booking.cash_hold_expires_at, Booking.status, Booking.created_at)
                .where(
                    Booking.master_id == master_id,
                    Booking.starts_at >= day_start_utc,
                    Booking.starts_at < day_end_utc,
                    Booking.status.notin_([
                        BookingStatus.CANCELLED,
                        BookingStatus.DONE,
                        BookingStatus.NO_SHOW,
                        BookingStatus.EXPIRED,
                    ])
                )
            )
            bookings_raw = list(result.all())
            bookings: list[datetime] = []
            for starts_at, hold_expires, status, created_at in bookings_raw:
                # Нормализуем статус к Enum значению (или строке value)
                try:
                    s_val = status
                except Exception:
                    s_val = status
                # Блокируют слот только активные статусы с неистекшим удержанием
                if s_val in {BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT}:
                    # Истекшее явное удержание — не блокирует
                    if hold_expires and hold_expires <= now_utc:
                        logger.debug("Игнорируем бронь %s: cash_hold_expires_at=%s истекло", starts_at, hold_expires)
                        continue
                    # Нет явного удержания — ориентируемся на created_at; если отсутствует, считаем просроченным
                    if not hold_expires:
                        cutoff = now_utc - timedelta(minutes=max(1, hold_minutes))
                        if (created_at is None) or (created_at <= cutoff):
                            logger.debug("Игнорируем бронь %s: created_at=%s просрочено (cutoff=%s)", starts_at, created_at, cutoff)
                            continue
                bookings.append(starts_at)

        # Determine working windows using centralized helper to avoid duplicated parsing logic
        try:
            windows_local = await master_services.get_work_windows_for_day(master_id, date)
        except Exception:
            windows_local = [(dtime(hour=9), dtime(hour=18))]

        step = timedelta(minutes=service_duration_min)
        available_slots: List[dtime] = []
        # Iterate all windows
        for ws, we in windows_local:
            work_start_local = datetime.combine(date.date(), ws).replace(tzinfo=LOCAL_TZ)
            work_end_local = datetime.combine(date.date(), we).replace(tzinfo=LOCAL_TZ)
            work_start = work_start_local.astimezone(UTC)
            work_end = work_end_local.astimezone(UTC)
            current = work_start
            while current + step <= work_end:
                slot_start = current
                slot_end = current + step
                overlap = False
                for b_start in bookings:
                    b_end = b_start + step
                    if slot_start < b_end and slot_end > b_start:
                        overlap = True
                        break
                if not overlap:
                    available_slots.append(slot_start.astimezone(LOCAL_TZ).time())
                current += step

        logger.debug("Доступные слоты для мастера %s на %s: %s", master_id, date, available_slots)
        return available_slots
    except Exception as e:
        logger.exception("Ошибка получения слотов для мастера %s на %s: %s", master_id, date, e)
        return []


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
        async with get_session() as session:
            result = await session.execute(
                select(Booking.starts_at, Booking.cash_hold_expires_at, Booking.status, Booking.created_at)
                .where(
                    Booking.master_id == master_id,
                    Booking.starts_at >= month_start_utc,
                    Booking.starts_at < next_month_utc,
                    Booking.status.notin_([
                        BookingStatus.CANCELLED,
                        BookingStatus.DONE,
                        BookingStatus.NO_SHOW,
                        BookingStatus.EXPIRED,
                    ])
                )
            )
            bookings_raw = list(result.all())

            # Normalize bookings to starts_at datetimes that still block slots
            bookings: list[datetime] = []
            try:
                hold_minutes = int(getattr(cfg, "SETTINGS", {}).get("hold_minutes", 1))
            except Exception:
                hold_minutes = 1
            for starts_at, hold_expires, status, created_at in bookings_raw:
                s_val = status
                if s_val in {BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT}:
                    if hold_expires and hold_expires <= now_utc:
                        continue
                    if not hold_expires:
                        cutoff = now_utc - timedelta(minutes=max(1, hold_minutes))
                        if (created_at is None) or (created_at <= cutoff):
                            continue
                bookings.append(starts_at)

            # Load master profile once for schedule/windows
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_id))
            data = {}
            if prof and getattr(prof, 'bio', None):
                try:
                    import json
                    data = json.loads(prof.bio or "{}") or {}
                except Exception:
                    data = {}

        available_days: set[int] = set()

        step = timedelta(minutes=service_duration_min)

        # Same-day lead minutes
        try:
            lead_min = int(getattr(cfg, "SETTINGS", {}).get("same_day_lead_minutes", 0))
        except Exception:
            lead_min = 0
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
            bookings_for_day = [b for b in bookings if (b >= day_start_utc and b < day_end_utc)]

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
                    for b_start in bookings_for_day:
                        b_end = b_start + step
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
            # Snapshot service price at booking time and create booking via helper
            svc = await session.get(Service, service_id)
            svc_price = int(getattr(svc, "price_cents", 0) or 0)
            booking = await _create_booking_base(session, client_id, master_id, slot, price_cents=svc_price, hold_minutes=hold_minutes, service_id=service_id)
            await session.commit()
            await session.refresh(booking)
            logger.info("Создана запись №%s: client_id=%s, master_id=%s, service_id=%s, slot=%s, expires_at=%s", booking.id, client_id, master_id, service_id, slot, booking.cash_hold_expires_at)
            return booking
    except SQLAlchemyError as e:
        logger.error("Ошибка создания записи: client_id=%s, master_id=%s, service_id=%s, slot=%s, error=%s", client_id, master_id, service_id, slot, e)
        raise


async def get_services_duration_and_price(service_ids: Sequence[str], online_payment: bool = False) -> dict[str, int | str]:
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

            for sid in service_ids:
                svc = services.get(str(sid))
                if svc:
                    if isinstance(getattr(svc, "price_cents", None), int):
                        total_price += int(svc.price_cents or 0)
                    if getattr(svc, "currency", None):
                        currency = svc.currency or currency
                prof = profiles.get(str(sid))
                dur = int(getattr(prof, "duration_minutes", 0) or 0) if prof else 0
                total_minutes += dur if dur > 0 else 60
    except Exception as e:
        logger.warning("Ошибка расчета суммы длительности/цены для %s: %s", service_ids, e)
    if online_payment and total_price > 0:
        total_price = int(total_price * 0.95)
    return {"total_minutes": total_minutes, "total_price_cents": total_price, "currency": currency}


async def create_composite_booking(client_id: int, master_id: int, service_ids: Sequence[str], slot: datetime, *, hold_minutes: int | None = None) -> Booking:
    """Create a booking with multiple services snapshot into booking_items and total price snapshot on Booking.

    The Booking.service_id will be set to the first service id for backward compatibility; detailed list stored in BookingItem rows.
    """
    if not service_ids:
        raise ValueError("service_ids must not be empty")
    try:
        totals = await get_services_duration_and_price(service_ids, online_payment=False)
        async with get_session() as session:
            price_cents = int(totals.get("total_price_cents", 0) or 0) or None
            booking = await _create_booking_base(session, client_id, master_id, slot, price_cents=price_cents, hold_minutes=hold_minutes, service_id=str(service_ids[0]))
            await session.flush()
            # add items
            pos = 0
            for sid in service_ids:
                session.add(BookingItem(booking_id=booking.id, service_id=str(sid), position=pos))
                pos += 1
            await session.commit()
            await session.refresh(booking)
            logger.info("Создана композитная запись №%s: client=%s master=%s services=%s", booking.id, client_id, master_id, list(service_ids))
            return booking
    except SQLAlchemyError as e:
        logger.error("Ошибка создания композитной записи: client_id=%s, master_id=%s, services=%s, slot=%s, error=%s", client_id, master_id, service_ids, slot, e)
        raise

async def get_client_active_bookings(user_id: int) -> List[Booking]:
    """Возвращает активные и будущие записи клиента.

    Args:
        user_id: ID клиента.

    Returns:
        Список объектов Booking.
    """
    try:
        now = datetime.now(UTC)
        async with get_session() as session:
            stmt = (
                select(Booking)
                .where(
                    Booking.user_id == user_id,
                    Booking.starts_at >= now,
                    Booking.status.not_in(["CANCELLED", "DONE", "REFUNDED"])  # Исправьте на not_in если версия SQLAlchemy >=2.0
                )
                .order_by(Booking.starts_at)
            )
            result = await session.execute(stmt)
            bookings = list(result.scalars().all())
            logger.info("Получено %d активных записей для клиента %s", len(bookings), user_id)
            return bookings
    except SQLAlchemyError as e:
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
    _hold = hold_minutes if hold_minutes is not None else int(getattr(cfg, "SETTINGS", {}).get("hold_minutes", 1))
    booking.cash_hold_expires_at = datetime.now(UTC) + timedelta(minutes=max(1, _hold))
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

__all__ = [
    "get_or_create_user",
    "get_available_time_slots",
    "create_booking",
    "get_client_active_bookings",
    "calculate_price",
    "process_successful_payment",
    "record_booking_rating",
    "get_services_duration_and_price",
    "create_composite_booking",
]