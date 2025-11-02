from __future__ import annotations
import logging
import os
from typing import Any, Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from bot.app.core.db import get_session
from bot.app.domain.models import Master, Service
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Kyiv")

logger = logging.getLogger(__name__)

# Загружаем переменные окружения из .env
load_dotenv()

# Основные настройки (overridden by DB at runtime if present)
SETTINGS: Dict[str, Any] = {
    "telegram_payment_provider_token": os.getenv("TELEGRAM_PAYMENT_PROVIDER_TOKEN", ""),
    "telegram_payments_enabled": os.getenv("TELEGRAM_PAYMENTS_ENABLED", "True").lower() == "true",
    # Таймаут удержания резерва (минуты)
    "reservation_hold_minutes": int(os.getenv("RESERVATION_HOLD_MINUTES", "5")),
    # Client is not allowed to reschedule within this many hours before start
    "client_reschedule_lock_hours": int(os.getenv("CLIENT_RESCHEDULE_LOCK_HOURS", "3")),
    # Client is not allowed to cancel within this many hours before start
    "client_cancel_lock_hours": int(os.getenv("CLIENT_CANCEL_LOCK_HOURS", "3")),
    # Calendar range (days ahead) for date picker
    "calendar_max_days_ahead": int(os.getenv("CALENDAR_MAX_DAYS_AHEAD", "365")),
    # Require this many minutes lead time for same-day bookings (0 = disabled)
    "same_day_lead_minutes": int(os.getenv("SAME_DAY_LEAD_MINUTES", "0")),
    "database_url": os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://salon_user:salon_pass@db:5432/salon_db"
    ),
    "bot_token": os.getenv("BOT_TOKEN", ""),
    "language": os.getenv("BOT_LANGUAGE", "uk"),  # Добавлена настройка языка по умолчанию
    # IANA timezone name for local business time (e.g., Europe/Kyiv)
    "timezone": os.getenv("TIMEZONE", "Europe/Kyiv"),
}

# Pagination and UI constants
# Number of bookings to show per page in master UI
BOOKINGS_PAGE_SIZE = int(os.getenv("BOOKINGS_PAGE_SIZE", "8"))

def refresh_local_tz() -> None:
    """Refresh module-level LOCAL_TZ from SETTINGS['timezone'] with safe fallback."""
    global LOCAL_TZ
    try:
        tz_name = str(SETTINGS.get("timezone", "Europe/Kyiv"))
        LOCAL_TZ = ZoneInfo(tz_name)
    except Exception:
        try:
            LOCAL_TZ = ZoneInfo("Europe/Kyiv")
        except Exception:
            # As a last resort keep existing value
            pass

# Initialize LOCAL_TZ from current SETTINGS/env
refresh_local_tz()

# Контакты салона (поддержка как CONTACT_*, так и устаревших BUSINESS_* переменных)
def _env_with_fallback(primary: str, fallback: str, default: str) -> str:
    val = os.getenv(primary)
    if val is None or val.strip() == "":
        val = os.getenv(fallback, default)
    return val

CONTACTS: Dict[str, str] = {
    "phone": _env_with_fallback("CONTACT_PHONE", "BUSINESS_PHONE", "+380671234567"),
    "instagram": _env_with_fallback("CONTACT_INSTAGRAM", "BUSINESS_INSTAGRAM", "https://instagram.com/salon_name"),
    "address": _env_with_fallback("CONTACT_ADDRESS", "BUSINESS_ADDRESS", "м. Київ, вул. Хрещатик, 1"),
}


# Читаем токен из переменной окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в переменных окружения")

# Дополнительно можно хранить тексты и дефолтные настройки
DEFAULT_LOCALE = "uk"
ADMIN_TEXT = {
    "access_denied": "Доступ запрещен: вы не администратор.",
}
MASTER_TEXT = {
    "access_denied": "Доступ запрещен: вы не мастер.",
}



# Администраторы (список Telegram ID)
ADMIN_IDS: set[int] = {
    int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

# Мастера (список Telegram ID)
MASTER_IDS: set[int] = {
    int(x) for x in os.getenv("MASTER_IDS", "").split(",") if x.strip().isdigit()
}

# Кэши для оптимизации
SERVICE_CACHE: Dict[str, str] = {}
MASTER_CACHE: Dict[int, str] = {}
MASTER_DIGEST_CACHE: Dict[int, str] = {}


def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором.

    Args:
        user_id: Telegram ID пользователя.

    Returns:
        True, если пользователь администратор, иначе False.
    """
    is_admin_user = user_id in ADMIN_IDS
    logger.debug("Проверка админа: user_id=%s, результат=%s", user_id, is_admin_user)
    return is_admin_user


def is_master(user_id: int) -> bool:
    """Проверяет, является ли пользователь мастером.

    Args:
        user_id: Telegram ID пользователя.

    Returns:
        True, если пользователь мастер, иначе False.
    """
    is_master_user = user_id in MASTER_IDS
    logger.debug("Проверка мастера: user_id=%s, результат=%s", user_id, is_master_user)
    return is_master_user


# Алиасы для обратной совместимости
check_admin = is_admin
is_master_env = is_master


def get_setting(key: str, default: Any = None) -> Any:
    """Безопасно получает настройку по ключу.

    Args:
        key: Ключ настройки.
        default: Значение по умолчанию, если ключ не найден.

    Returns:
        Значение настройки или default.
    """
    value = SETTINGS.get(key, default)
    logger.debug("Получена настройка: key=%s, value=%s", key, value)
    return value


async def load_settings_from_db() -> None:
    """Load settings from DB into in-memory SETTINGS so they persist across restarts."""
    try:
        from bot.app.domain.models import Setting
        async with get_session() as session:
            result = await session.execute(select(Setting))
            for s in result.scalars().all():
                SETTINGS[str(s.key)] = s.value
        logger.info("Runtime SETTINGS loaded from DB: %s", {k: SETTINGS[k] for k in ("reservation_hold_minutes", "timezone") if k in SETTINGS})
        # Keep LOCAL_TZ in sync with DB-provided timezone at runtime
        try:
            refresh_local_tz()
        except Exception:
            pass
    except Exception as e:
        logger.warning("Failed to load settings from DB: %s", e)


def get_hold_minutes() -> int:
    """Unified accessor for reservation_hold_minutes with safe fallback."""
    try:
        val = SETTINGS.get("reservation_hold_minutes")
        return max(1, int(val)) if val is not None else 5
    except Exception:
        return 5


def get_client_reschedule_lock_hours() -> int:
    """Returns the minimal hours before start when client reschedule is forbidden."""
    try:
        val = SETTINGS.get("client_reschedule_lock_hours", 3)
        return max(0, int(val))
    except Exception:
        return 3


def get_client_cancel_lock_hours() -> int:
    """Returns the minimal hours before start when client cancellation is forbidden."""
    try:
        val = SETTINGS.get("client_cancel_lock_hours", 3)
        return max(0, int(val))
    except Exception:
        return 3


def get_calendar_max_days_ahead() -> int:
    """Maximum days ahead for calendar navigation/selection."""
    try:
        val = SETTINGS.get("calendar_max_days_ahead", 365)
        return max(1, int(val))
    except Exception:
        return 365


def get_same_day_lead_minutes() -> int:
    """Minimal lead time in minutes for same-day slot to be selectable (0 to disable)."""
    try:
        val = SETTINGS.get("same_day_lead_minutes", 0)
        return max(0, int(val))
    except Exception:
        return 0


async def get_services() -> Dict[str, str]:
    """Возвращает список доступных услуг из базы, с безопасным запасным вариантом.

    Примечание: Функция асинхронная. Вызывающая сторона (например, client_keyboards.get_service_menu)
    уже поддерживает как sync, так и async возврат и корректно ожидает корутину.

    Returns:
        Словарь {service_id: service_name}.
    """
    try:
        async with get_session() as session:
            result = await session.execute(select(Service.id, Service.name))
            rows = result.all()
            services = {str(sid): str(name) for sid, name in rows}
            if services:
                logger.info("Получен список услуг из БД: %s", services)
                return services
    except SQLAlchemyError as e:
        logger.error("Ошибка загрузки услуг из базы данных: %s", e)
    except Exception as e:
        logger.exception("Неожиданная ошибка при загрузке услуг из базы: %s", e)

    # Фоллбэк (согласован с сидированием в bootstrap: haircut, color, nails, brows)
    fallback = {
        "haircut": "� Стрижка",
        "color": "🎨 Фарбування",
        "nails": "💅 Манікюр",
        "brows": "👁️ Корекція брів",
    }
    logger.info("Используется запасной список услуг: %s", fallback)
    return fallback


async def get_masters() -> Dict[int, str]:
    """Возвращает кэшированный список мастеров (telegram_id -> имя).

    Если кэш пуст, загружает данные из базы.

    Returns:
        Словарь {telegram_id: master_name}.
    """
    if not MASTER_CACHE:
        try:
            async with get_session() as session:
                result = await session.execute(select(Master.telegram_id, Master.name))
                for tg_id, name in result.all():
                    MASTER_CACHE[tg_id] = name
                logger.info("Кэш мастеров заполнен из базы данных, количество: %d", len(MASTER_CACHE))
        except SQLAlchemyError as e:
            logger.error("Ошибка загрузки мастеров из базы данных: %s", e)
            return {}
    return MASTER_CACHE


def invalidate_service_cache() -> None:
    """Очищает кэш услуг."""
    SERVICE_CACHE.clear()
    logger.info("Кэш услуг очищен")


# Алиас с множественным числом для обратной совместимости
def invalidate_services_cache() -> None:
    invalidate_service_cache()


def invalidate_master_cache() -> None:
    """Очищает кэш мастеров."""
    MASTER_CACHE.clear()
    logger.info("Кэш мастеров очищен")


def invalidate_master_digest(master_id: int) -> None:
    """Очищает кэш дайджеста мастера.

    Args:
        master_id: Telegram ID мастера.
    """
    MASTER_DIGEST_CACHE.pop(master_id, None)
    logger.info("Кэш дайджеста очищен для мастера %s", master_id)


def record_master_digest_shown(master_id: int) -> None:
    """Фиксирует факт отображения дайджеста мастера.

    Args:
        master_id: Telegram ID мастера.
    """
    MASTER_DIGEST_CACHE[master_id] = datetime.now().isoformat()
    logger.info("Дайджест записан для мастера %s", master_id)


# Алиасы для обратной совместимости
invalidate_masters_cache = invalidate_master_cache

__all__ = [
    "SETTINGS",
    "CONTACTS",
    "ADMIN_TEXT",
    "MASTER_TEXT",
    "ADMIN_IDS",
    "MASTER_IDS",
    "is_admin",
    "is_master",
    "check_admin",
    "is_master_env",
    "get_setting",
    "get_services",
    "get_masters",
    "invalidate_service_cache",
    "invalidate_master_cache",
    "invalidate_masters_cache",
    "invalidate_master_digest",
    "record_master_digest_shown",
    "SERVICE_CACHE",
    "MASTER_CACHE",
    "MASTER_DIGEST_CACHE",
]