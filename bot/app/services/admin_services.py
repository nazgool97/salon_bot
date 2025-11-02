from __future__ import annotations
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from bot.app.domain.models import Booking, Master, Service, User, BookingStatus
from bot.app.core.db import get_session
import bot.config as cfg

logger = logging.getLogger(__name__)


# ---------------- Payments & provider helpers (moved from shared_services) ----------------
import os
from datetime import UTC, datetime, timedelta
from typing import Optional

# Payment config cache
_PAYMENTS_ENABLED: bool | None = None
_PROVIDER_TOKEN_CACHE: str | None = None
_PAYMENTS_LAST_CHECKED: datetime | None = None
_PROVIDER_LAST_CHECKED: datetime | None = None
_SETTINGS_TTL_SECONDS: int = int(getattr(cfg, "SETTINGS", {}).get("settings_cache_ttl_seconds", 60))


def _settings_cache_expired(last_checked: datetime | None) -> bool:
    if last_checked is None:
        return True
    try:
        return (datetime.now(UTC) - last_checked) > timedelta(seconds=_SETTINGS_TTL_SECONDS)
    except Exception:
        return True


def is_telegram_payments_enabled() -> bool:
    """Проверяет, включены ли Telegram Payments.

    Returns:
        True, если платежи включены, иначе False.
    """
    global _PAYMENTS_ENABLED, _PAYMENTS_LAST_CHECKED
    if _PAYMENTS_ENABLED is None or _settings_cache_expired(_PAYMENTS_LAST_CHECKED):
        settings = getattr(cfg, "SETTINGS", {})
        _PAYMENTS_ENABLED = bool(settings.get("telegram_payments_enabled", True))
        _PAYMENTS_LAST_CHECKED = datetime.now(UTC)
        logger.debug("Telegram Payments refresh: %s", _PAYMENTS_ENABLED)
    return bool(_PAYMENTS_ENABLED)


async def toggle_telegram_payments() -> bool:
    """Переключает флаг Telegram Payments и возвращает новое значение.

    Returns:
        Новое значение флага (True/False).
    """
    global _PAYMENTS_ENABLED, _PAYMENTS_LAST_CHECKED
    new_val = not is_telegram_payments_enabled()
    _PAYMENTS_ENABLED = new_val
    _PAYMENTS_LAST_CHECKED = datetime.now(UTC)
    logger.info("Telegram Payments переключен: %s", new_val)
    return new_val


def get_telegram_provider_token(force_reload: bool = False) -> str | None:
    """Returns Telegram Payments provider token.

    This first consults runtime settings, and if missing (or force_reload=True),
    reloads environment variables from .env to pick up changes without a restart.

    Args:
        force_reload: When True, re-read .env and environment variables.

    Returns:
        The provider token string or None if not configured.
    """
    global _PROVIDER_TOKEN_CACHE, _PROVIDER_LAST_CHECKED
    try:
        token: str | None = None
        if not force_reload and _PROVIDER_TOKEN_CACHE and not _settings_cache_expired(_PROVIDER_LAST_CHECKED):
            # Prefer cached value to avoid excessive IO
            return _PROVIDER_TOKEN_CACHE

        # First check settings
        token = getattr(cfg, "SETTINGS", {}).get("telegram_payment_provider_token")

        if not token or force_reload:
            # Try to refresh from environment (supports runtime .env edits)
            try:
                from dotenv import load_dotenv  # type: ignore
                # override=True to pick up changes done after process start
                load_dotenv(override=True)
            except Exception:
                # dotenv is optional; silently continue
                pass
            token = os.getenv("TELEGRAM_PAYMENT_PROVIDER_TOKEN") or token
            if token:
                # Keep SETTINGS in sync so other code paths see updated value
                try:
                    getattr(cfg, "SETTINGS", {})["telegram_payment_provider_token"] = token
                except Exception:
                    pass
        _PROVIDER_TOKEN_CACHE = token or None
        _PROVIDER_LAST_CHECKED = datetime.now(UTC)
        return token or None
    except Exception as e:
        logger.warning("Failed to resolve Telegram provider token: %s", e)
        return None


def is_online_payments_available() -> bool:
    """True if Telegram online payments can be offered to clients.

    Requires both the feature flag to be enabled and a valid provider token.
    """
    try:
        return bool(is_telegram_payments_enabled() and get_telegram_provider_token())
    except Exception as e:
        logger.warning("Online payments availability check failed: %s", e)
        return False


# ---------------- Stats formatting (moved from shared_services) ----------------
from typing import Mapping, Any


def render_stats_overview(data: Mapping[str, Any], *, title_key: str = "stats_overview", lang: str = "uk") -> str:
    """Render a simple stats overview with a localized title and k:v pairs."""
    try:
        # Import translations lazily to avoid cycles at module import
        from bot.app.services.shared_services import tr

        title = tr(title_key, lang=lang)
        lines = [title]
        lines.extend(f"{k}: {v}" for k, v in data.items())
        return "\n".join(lines)
    except Exception:
        try:
            return "\n".join([title_key] + [f"{k}: {v}" for k, v in data.items()])
        except Exception:
            return title_key


async def update_service_price_cents(service_id: int | str, new_cents: int):
    """Update Service.price_cents and snapshot final_price_cents (if present).

    Returns the updated Service instance or None if not found or on error.
    Accepts integer or string IDs to remain compatible with current handlers.
    """
    try:
        async with get_session() as session:
            svc = await session.get(Service, service_id)
            if not svc:
                logger.debug("update_service_price_cents: service not found %s", service_id)
                return None
            svc.price_cents = int(new_cents)
            try:
                if hasattr(svc, "final_price_cents"):
                    setattr(svc, "final_price_cents", int(new_cents))
            except Exception:
                # keep best effort; don't fail entire operation for attribute issues
                logger.debug("Could not set final_price_cents for service %s", service_id)
            await session.commit()
            return svc
    except SQLAlchemyError as e:
        logger.error("DB error updating price for service %s: %s", service_id, e)
        return None
    except Exception as e:
        logger.exception("Unexpected error updating price for service %s: %s", service_id, e)
        return None

# Статусы, учитываемые при подсчете выручки
# Revenue is recognized for PAID and CONFIRMED (cash) and optionally DONE
_REVENUE_STATUSES = {BookingStatus.PAID, BookingStatus.CONFIRMED, BookingStatus.DONE}

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
        Кортеж (начало, конец).
    """
    now = datetime.now(UTC)
    days = 7 if kind == "week" else 30
    start = now - timedelta(days=days)
    logger.debug("Рассчитаны рамки периода %s: start=%s, end=%s", kind, start, now)
    return start, now


def _price_expr() -> Any:
    """Возвращает SQLAlchemy выражение для цены.

    Returns:
        Выражение для final_price_cents или original_price_cents.
    """
    return func.coalesce(Booking.final_price_cents, Booking.original_price_cents, 0)


async def get_basic_totals() -> Dict[str, int]:
    """Получает общее количество записей и пользователей.

    Returns:
        Словарь с total_bookings и total_users.
    """
    try:
        async with get_session() as session:
            total_bookings = await session.scalar(select(func.count(Booking.id))) or 0
            total_users = await session.scalar(select(func.count(User.id))) or 0
            result = {"total_bookings": total_bookings, "total_users": total_users}
            logger.info("Получены базовые показатели: %s", result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка получения базовых показателей: %s", e)
        return {"total_bookings": 0, "total_users": 0}


async def get_range_stats(kind: str) -> Dict[str, Any]:
    """Получает статистику по записям за период.

    Args:
        kind: Тип периода ('week' или 'month').

    Returns:
        Словарь со статистикой (bookings, unique_users, masters, avg_per_day).
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            base_query = select(Booking).where(Booking.starts_at.between(start, end))
            total = await session.scalar(select(func.count()).select_from(base_query.subquery())) or 0
            if total == 0:
                logger.info("Записей за период %s не найдено", kind)
                return {"bookings": 0, "unique_users": 0, "masters": 0, "avg_per_day": 0.0}

            unique_users = await session.scalar(
                select(func.count(func.distinct(Booking.user_id)))
                .where(Booking.starts_at.between(start, end))
            ) or 0
            unique_masters = await session.scalar(
                select(func.count(func.distinct(Booking.master_id)))
                .where(Booking.starts_at.between(start, end), Booking.master_id.isnot(None))
            ) or 0
            days = max(1, (end - start).days)
            result = {
                "bookings": total,
                "unique_users": unique_users,
                "masters": unique_masters,
                "avg_per_day": total / days,
            }
            logger.info("Статистика за период %s: %s", kind, result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка получения статистики за период %s: %s", kind, e)
        return {"bookings": 0, "unique_users": 0, "masters": 0, "avg_per_day": 0.0}


async def get_top_masters(limit: int = 10) -> List[Dict[str, Any]]:
    """Получает топ мастеров по количеству записей.

    Args:
        limit: Максимальное количество мастеров.

    Returns:
        Список словарей с полями name и count.
    """
    try:
        async with get_session() as session:
            stmt = (
                select(Master.name, func.count(Booking.id).label("count"))
                .join(Master, Booking.master_id == Master.telegram_id)
                .group_by(Master.name)
                .order_by(func.count(Booking.id).desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            top_masters = [{"name": name, "count": count} for name, count in result.all()]
            logger.info("Получен топ %d мастеров: %s", limit, top_masters)
            return top_masters
    except SQLAlchemyError as e:
        logger.error("Ошибка получения топа мастеров: %s", e)
        return []


async def get_top_services(limit: int = 10) -> List[Dict[str, Any]]:
    """Получает топ услуг по количеству записей.

    Args:
        limit: Максимальное количество услуг.

    Returns:
        Список словарей с полями service и count.
    """
    try:
        async with get_session() as session:
            stmt = (
                select(Service.name, func.count(Booking.id).label("count"))
                .join(Service, Booking.service_id == Service.id)
                .group_by(Service.name)
                .order_by(func.count(Booking.id).desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            top_services = [{"service": name, "count": count} for name, count in result.all()]
            logger.info("Получен топ %d услуг: %s", limit, top_services)
            return top_services
    except SQLAlchemyError as e:
        logger.error("Ошибка получения топа услуг: %s", e)
        return []


async def get_revenue_total(kind: str = "month") -> int:
    """Рассчитывает общую выручку за период.

    Args:
        kind: Тип периода ('week' или 'month').

    Returns:
        Сумма выручки в копейках.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            stmt = select(func.coalesce(func.sum(_price_expr()), 0)).where(
                Booking.starts_at.between(start, end),
                Booking.status.in_(_REVENUE_STATUSES),
            )
            revenue = int(await session.scalar(stmt) or 0)
            logger.info("Выручка за период %s: %d копеек", kind, revenue)
            return revenue
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета выручки за период %s: %s", kind, e)
        return 0


async def get_revenue_by_master(kind: str = "month", limit: int = 10) -> List[Dict[str, Any]]:
    """Рассчитывает выручку по мастерам.

    Args:
        kind: Тип периода ('week' или 'month').
        limit: Максимальное количество мастеров.

    Returns:
        Список словарей с полями name, revenue_cents, bookings.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            stmt = (
                select(
                    Master.name,
                    func.sum(_price_expr()).label("revenue_cents"),
                    func.count(Booking.id).label("bookings")
                )
                .join(Master, Booking.master_id == Master.telegram_id)
                .where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES)
                )
                .group_by(Master.name)
                .order_by(func.sum(_price_expr()).desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            revenue = [row._asdict() for row in result.all()]
            logger.info("Выручка по мастерам за период %s: %s", kind, revenue)
            return revenue
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета выручки по мастерам за период %s: %s", kind, e)
        return []


async def get_revenue_by_service(kind: str = "month", limit: int = 10) -> List[Dict[str, Any]]:
    """Рассчитывает выручку по услугам.

    Args:
        kind: Тип периода ('week' или 'month').
        limit: Максимальное количество услуг.

    Returns:
        Список словарей с полями service, revenue_cents, bookings.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            stmt = (
                select(
                    Service.name.label("service"),
                    func.sum(_price_expr()).label("revenue_cents"),
                    func.count(Booking.id).label("bookings")
                )
                .join(Service, Booking.service_id == Service.id)
                .where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES)
                )
                .group_by(Service.name)
                .order_by(func.sum(_price_expr()).desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            revenue = [row._asdict() for row in result.all()]
            logger.info("Выручка по услугам за период %s: %s", kind, revenue)
            return revenue
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета выручки по услугам за период %s: %s", kind, e)
        return []


async def get_retention(kind: str = "month") -> Dict[str, Any]:
    """Рассчитывает коэффициент удержания клиентов.

    Args:
        kind: Тип периода ('week' или 'month').

    Returns:
        Словарь с repeaters, total и rate.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            subquery = (
                select(Booking.user_id, func.count(Booking.id).label("c"))
                .where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES),
                )
                .group_by(Booking.user_id)
                .subquery()
            )
            total_users = await session.scalar(select(func.count()).select_from(subquery)) or 0
            repeat_users = await session.scalar(
                select(func.count()).select_from(subquery).where(subquery.c.c > 1)
            ) or 0
            rate = (repeat_users / total_users) if total_users else 0.0
            result = {"repeaters": repeat_users, "total": total_users, "rate": rate}
            logger.info("Retention за период %s: %s", kind, result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета retention за период %s: %s", kind, e)
        return {"repeaters": 0, "total": 0, "rate": 0.0}


async def get_no_show_rates(kind: str = "month") -> Dict[str, Any]:
    """Рассчитывает процент неявок.

    Args:
        kind: Тип периода ('week' или 'month').

    Returns:
        Словарь с no_show, total и rate.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            base_query = select(Booking.id).where(
                Booking.starts_at.between(start, end),
                Booking.status.in_(_ACTIVE_FOR_NOSHOW_BASE)
            ).subquery()
            total = await session.scalar(select(func.count()).select_from(base_query)) or 0
            no_shows = await session.scalar(
                select(func.count(Booking.id)).where(
                    Booking.starts_at.between(start, end),
                    Booking.status == BookingStatus.NO_SHOW
                )
            ) or 0
            rate = (no_shows / total) if total else 0.0
            result = {"no_show": no_shows, "total": total, "rate": rate}
            logger.info("No-show статистика за период %s: %s", kind, result)
            return result
    except SQLAlchemyError as e:
        logger.error("Ошибка расчета no-show за период %s: %s", kind, e)
        return {"no_show": 0, "total": 0, "rate": 0.0}


async def get_top_clients_ltv(kind: str = "month", limit: int = 10) -> List[Dict[str, Any]]:
    """Получает топ клиентов по LTV (выручке).

    Args:
        kind: Тип периода ('week' или 'month').
        limit: Максимальное количество клиентов.

    Returns:
        Список словарей с полями name, revenue_cents, bookings.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            stmt = (
                select(
                    User.name,
                    func.sum(_price_expr()).label("revenue_cents"),
                    func.count(Booking.id).label("bookings")
                )
                .join(User, Booking.user_id == User.id)
                .where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES)
                )
                .group_by(User.name)
                .order_by(func.sum(_price_expr()).desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            top_clients = [row._asdict() for row in result.all()]
            logger.info("Топ %d клиентов по LTV за период %s: %s", limit, kind, top_clients)
            return top_clients
    except SQLAlchemyError as e:
        logger.error("Ошибка получения топа клиентов по LTV за период %s: %s", kind, e)
        return []


# ---------------- Additional analytics helpers ----------------

async def get_conversion(kind: str = "month") -> Dict[str, Any]:
    """Share of bookings that became PAID or CONFIRMED in range.

    Returns dict with created, converted, rate.
    """
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            total_created = await session.scalar(
                select(func.count(Booking.id)).where(Booking.starts_at.between(start, end))
            ) or 0
            converted = await session.scalar(
                select(func.count(Booking.id)).where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_({BookingStatus.PAID, BookingStatus.CONFIRMED})
                )
            ) or 0
            rate = (converted / total_created) if total_created else 0.0
            return {"created": total_created, "converted": converted, "rate": rate}
    except SQLAlchemyError:
        return {"created": 0, "converted": 0, "rate": 0.0}


async def get_cancellations(kind: str = "month") -> Dict[str, Any]:
    """Counts cancelled bookings in range and share of total."""
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            total = await session.scalar(
                select(func.count(Booking.id)).where(Booking.starts_at.between(start, end))
            ) or 0
            cancelled = await session.scalar(
                select(func.count(Booking.id)).where(
                    Booking.starts_at.between(start, end), Booking.status == BookingStatus.CANCELLED
                )
            ) or 0
            rate = (cancelled / total) if total else 0.0
            return {"cancelled": cancelled, "total": total, "rate": rate}
    except SQLAlchemyError:
        return {"cancelled": 0, "total": 0, "rate": 0.0}


async def get_daily_trends(kind: str = "month") -> List[Dict[str, Any]]:
    """Daily bookings and revenue for the range."""
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            # Group by date (UTC date of starts_at)
            date_trunc = func.date_trunc('day', Booking.starts_at)
            stmt = (
                select(
                    func.date(date_trunc).label('day'),
                    func.count(Booking.id).label('bookings'),
                    func.sum(_price_expr()).label('revenue_cents')
                )
                .where(Booking.starts_at.between(start, end))
                .group_by(func.date(date_trunc))
                .order_by(func.date(date_trunc))
            )
            result = await session.execute(stmt)
            return [
                {"day": str(row.day), "bookings": int(row.bookings or 0), "revenue_cents": int(row.revenue_cents or 0)}
                for row in result.fetchall()
            ]
    except SQLAlchemyError:
        return []


async def get_aov(kind: str = "month") -> float:
    """Average order value (revenue per booking) for revenue statuses in range."""
    try:
        start, end = _range_bounds(kind)
        async with get_session() as session:
            revenue = await session.scalar(
                select(func.coalesce(func.sum(_price_expr()), 0)).where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES),
                )
            ) or 0
            cnt = await session.scalar(
                select(func.count(Booking.id)).where(
                    Booking.starts_at.between(start, end),
                    Booking.status.in_(_REVENUE_STATUSES),
                )
            ) or 0
            return (revenue / cnt) if cnt else 0.0
    except SQLAlchemyError:
        return 0.0


__all__ = [
    "get_basic_totals",
    "get_range_stats",
    "get_top_masters",
    "get_top_services",
    "get_revenue_total",
    "get_revenue_by_master",
    "get_revenue_by_service",
    "get_retention",
    "get_no_show_rates",
    "get_top_clients_ltv",
]