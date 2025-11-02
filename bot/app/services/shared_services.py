from __future__ import annotations
import logging
import os
from typing import Any, Dict, Iterable, Sequence
from dataclasses import dataclass

from sqlalchemy import select, or_
from sqlalchemy.exc import SQLAlchemyError

import bot.config as cfg
from bot.app.core.db import get_session
from bot.app.domain.models import User
from bot.app.translations import tr as _tr_raw
from aiogram import Bot
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Admin IDs and notifications
import os
from typing import Optional

# ADMIN_IDS can be configured in runtime settings (cfg.SETTINGS) or via ENV ADMIN_IDS
ADMIN_IDS = getattr(cfg, "ADMIN_IDS", None)
if not ADMIN_IDS:
    ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]


async def notify_admins(message: str, bot: Optional[Bot] = None) -> None:
    """Compatibility wrapper for admin notification.

    This function preserves the previous signature (bot optional) for
    call-sites across the codebase. It forwards to the canonical
    implementation in `bot.app.core.notifications` which requires an
    explicit Bot instance. When `bot` is omitted we try a best-effort
    fallback to the bootstrap bot to preserve existing behavior.
    """
    admin_ids = getattr(cfg, "ADMIN_IDS", None) or ADMIN_IDS or []
    if not admin_ids:
        logger.debug("notify_admins: no ADMIN_IDS configured; skipping")
        return

    bot_instance = bot
    if bot_instance is None:
        try:
            from bot.app.core.bootstrap import bot as _bootstrap_bot

            bot_instance = _bootstrap_bot
        except Exception:
            bot_instance = None

    if bot_instance is None:
        # Nothing we can do — log and return
        logger.debug("notify_admins: no bot available to send notifications")
        return

    # import the canonical implementation and forward the call
    try:
        from bot.app.core.notifications import notify_admins as _core_notify

        await _core_notify(message, bot_instance)
    except Exception as e:
        # As a last resort, attempt to send directly using the local helper
        logger.debug("shared_services.notify_admins: forwarding failed, falling back: %s", e)
        for admin_id in admin_ids:
            try:
                try:
                    await _safe_send(bot_instance, admin_id, message)
                except Exception:
                    await bot_instance.send_message(admin_id, message)
            except Exception as exc:
                logger.error("notify_admins fallback: failed to notify %s: %s", admin_id, exc)

# Кэш для имен услуг
_service_name_cache: Dict[str, str] = {}

# Эмодзи для статусов
STATUS_EMOJI: Dict[str, str] = {
    "paid": "✅",
    "confirmed": "💵",
    "awaiting_cash": "💵",  # legacy
    "pending_payment": "⏳",
    "reserved": "🟡",
    "expired": "⌛",
    "active": "🟢",  # legacy
    "cancelled": "❌",
    "done": "✔️",
    "no_show": "👻",
}


def _settings_cache_expired(last_checked: datetime | None) -> bool:
    # delegate to admin_services implementation to keep a single cache location
    try:
        from bot.app.services import admin_services

        return admin_services._settings_cache_expired(last_checked)  # type: ignore[attr-defined]
    except Exception:
        # Fallback: if admin_services not available, use a conservative response
        try:
            if last_checked is None:
                return True
            return (datetime.now(UTC) - last_checked) > timedelta(seconds=int(getattr(cfg, "SETTINGS", {}).get("settings_cache_ttl_seconds", 60)))
        except Exception:
            return True


def is_telegram_payments_enabled() -> bool:
    """Compatibility wrapper delegating to admin_services.is_telegram_payments_enabled."""
    try:
        from bot.app.services import admin_services

        return admin_services.is_telegram_payments_enabled()
    except Exception:
        logger.debug("is_telegram_payments_enabled: admin_services not available, defaulting to False")
        return False


async def toggle_telegram_payments() -> bool:
    """Delegate to admin_services.toggle_telegram_payments."""
    from bot.app.services import admin_services

    return await admin_services.toggle_telegram_payments()


def get_telegram_provider_token(force_reload: bool = False) -> str | None:
    """Delegate to admin_services.get_telegram_provider_token."""
    try:
        from bot.app.services import admin_services

        return admin_services.get_telegram_provider_token(force_reload=force_reload)
    except Exception:
        return None


def is_online_payments_available() -> bool:
    """Delegate to admin_services.is_online_payments_available."""
    try:
        from bot.app.services import admin_services

        return admin_services.is_online_payments_available()
    except Exception:
        return False


def format_money_cents(cents: int | float | None, currency: str = "UAH") -> str:
    """Форматирует сумму в копейках в читаемый вид (например, '100.00 UAH').

    Args:
        cents: Сумма в копейках.
        currency: Валюта (по умолчанию UAH).

    Returns:
        Отформатированная строка с суммой.
    """
    try:
        # Приводим к целым копейкам; None и некорректные значения -> 0
        cents_int = 0
        if isinstance(cents, (int, float)):
            cents_int = int(cents)
        value = cents_int / 100
        formatted = f"{value:.2f} {currency}"
        # Используем %s для безопасности при непредвиденных типах
        logger.debug("Форматирована сумма: %s копеек -> %s", cents_int, formatted)
        return formatted
    except Exception as e:
        logger.error("Ошибка форматирования суммы: cents=%s, error=%s", cents, e)
        return f"0.00 {currency}"


def status_to_emoji(status: object) -> str:
    """Возвращает эмодзи для указанного статуса (без учета регистра).

    Args:
        status: Статус записи (строка или Enum с атрибутом ``value``).

    Returns:
        Эмодзи, соответствующее статусу, или '❓' если статус неизвестен.
    """
    try:
        sval = getattr(status, "value", status)
        sval_str = str(sval).lower()
        emoji = STATUS_EMOJI.get(sval_str, "❓")
        logger.debug("Статус %s (%s) преобразован в эмодзи: %s", status, sval_str, emoji)
        return emoji
    except Exception:
        return "❓"


async def get_user_locale(telegram_id: int) -> str:
    """Получает локаль пользователя по Telegram ID или возвращает глобальную локаль.

    The Telegram integration always passes the Telegram user id (for
    example, via ``cb.from_user.id`` or ``message.from_user.id``). To avoid
    heuristic guessing we always look up by ``User.telegram_id``.

    Args:
        telegram_id: Telegram user id.

    Returns:
        Код локали (например, 'uk').
    """
    # Delegate to client_services implementation (DB-backed). Keep wrapper for backward compatibility.
    try:
        from bot.app.services import client_services

        return await client_services.get_user_locale(telegram_id)
    except Exception as e:
        logger.debug("shared_services.get_user_locale: delegation failed: %s", e)
        default_locale = getattr(cfg, "SETTINGS", {}).get("language", "uk")
        return default_locale


async def translate_for_user(user_id: int, key: str, **kwargs: Any) -> str:
    """Переводит текст для пользователя по ключу с учетом локали.

    Args:
        user_id: Telegram ID пользователя.
        key: Ключ перевода.
        **kwargs: Параметры для форматирования строки.

    Returns:
        Переведенная строка или ключ, если перевод не найден.
    """
    try:
        master_text = getattr(cfg, "MASTER_TEXT", {})
        admin_text = getattr(cfg, "ADMIN_TEXT", {})
        template = master_text.get(key, admin_text.get(key, key))
        translated = template.format(**kwargs)
        logger.debug("Перевод для пользователя %s: key=%s, result=%s", user_id, key, translated)
        return translated
    except Exception as e:
        logger.error("Ошибка перевода: user_id=%s, key=%s, error=%s", user_id, key, e)
        return key


async def get_service_name(service_id: str) -> str:
    """Получает название услуги по ID с использованием кэша.

    Args:
        service_id: ID услуги.

    Returns:
        Название услуги или ID, если услуга не найдена.
    """
    # Prefer the bulk-loaded services cache to avoid per-item caching.
    try:
        all_services = await services_cache()
        name = all_services.get(service_id)
        if name:
            logger.debug("Название услуги %s взято из services_cache: %s", service_id, name)
            return name
    except Exception as e:
        logger.error("Ошибка получения services_cache: %s", e)

    # Fallback to DB lookup if cache missed
    try:
        async with get_session() as session:
            from bot.app.domain.models import Service
            svc = await session.get(Service, service_id)
            if svc:
                logger.info("Название услуги %s взято из БД (fallback): %s", service_id, svc.name)
                return svc.name
    except SQLAlchemyError as e:
        logger.error("Ошибка получения названия услуги %s из БД: %s", service_id, e)

    logger.warning("Услуга %s не найдена, возвращается ID", service_id)
    return service_id


__all__ = [
    "is_telegram_payments_enabled",
    "toggle_telegram_payments",
    "get_telegram_provider_token",
    "is_online_payments_available",
    "format_money_cents",
    "status_to_emoji",
    "get_user_locale",
    "translate_for_user",
    "get_service_name",
]

# ---------------- New shared helpers (i18n, profiles, notifications) ---------------- #
from typing import Optional, Mapping
from aiogram.types import Message, CallbackQuery
# ---------------- Repositories (DB access helpers) ---------------- #

class UserRepo:
    """Repository for User-related lookups."""

    @staticmethod
    async def get_by_id(user_id: int) -> User | None:
        try:
            async with get_session() as session:
                return await session.get(User, user_id)
        except Exception:
            return None

    @staticmethod
    async def get_by_telegram_id(telegram_id: int) -> User | None:
        try:
            async with get_session() as session:
                from sqlalchemy import select
                result = await session.execute(select(User).where(User.telegram_id == telegram_id))
                return result.scalar_one_or_none()
        except Exception:
            return None

    @staticmethod
    async def get_locale(telegram_id: int) -> str | None:
        """Return a user's locale by Telegram ID or None.

        We explicitly accept a Telegram ID here — callers in the Telegram
        integration should pass ``from_user.id`` directly.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                result = await session.execute(select(User.locale).where(User.telegram_id == telegram_id))
                return result.scalar_one_or_none()
        except Exception:
            return None


class BookingRepo:
    """Repository for Booking-related lookups."""

    @staticmethod
    async def get(booking_id: int):
        try:
            async with get_session() as session:
                from bot.app.domain.models import Booking
                return await session.get(Booking, booking_id)
        except Exception:
            return None


def _msg(obj: Message | CallbackQuery | Any) -> Message | None:
    """Safely extract Message from Message or CallbackQuery.

    Returns None when the message is inaccessible (e.g., outdated or missing).
    """
    try:
        candidate = getattr(obj, "message", obj)
        if isinstance(candidate, Message) and not getattr(candidate, "is_inaccessible", False):
            return candidate
    except Exception:
        pass
    return None


def safe_user_id(obj: Message | CallbackQuery | Any) -> int:
    """Return Telegram user id from Message/CallbackQuery or 0 if not available."""
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or 0)
    except Exception:
        return 0


def _safe_call(name: str, *args, **kwargs) -> None:
    """Call cfg.<name>(*args, **kwargs) if callable, swallow errors.

    Useful for optional cache invalidations/hooks that may not exist in some deployments.
    """
    try:
        fn = getattr(cfg, name, None)
        if callable(fn):
            try:
                fn(*args, **kwargs)
            except Exception:
                # Best-effort only
                pass
    except Exception:
        pass


def _get_id_from_callback(data: str | None, prefix: str) -> Optional[int]:
    """Extract trailing integer id from callback data that starts with prefix.

    Returns None when data is missing/doesn't start with prefix/has no int tail.
    """
    if not data or not isinstance(data, str) or not data.startswith(prefix):
        return None
    try:
        return int(data.split("_")[-1])
    except (ValueError, IndexError):
        return None


# ---------------- Stats rendering (centralized) ---------------- #

def render_stats_overview(data: Mapping[str, Any], *, title_key: str = "stats_overview", lang: str = "uk") -> str:
    """Delegate to admin_services.render_stats_overview to keep a single implementation.

    This wrapper preserves the previous import location so callers that import
    from shared_services continue to work while the canonical implementation
    lives in `admin_services`.
    """
    try:
        from bot.app.services import admin_services

        # delegate to the admin implementation (which performs a lazy import of translations)
        return admin_services.render_stats_overview(data, title_key=title_key, lang=lang)
    except Exception as e:
        logger.debug("shared_services.render_stats_overview: delegation failed: %s", e)
        # best-effort fallback (simple, non-localized)
        try:
            return "\n".join([title_key] + [f"{k}: {v}" for k, v in data.items()])
        except Exception:
            return title_key


# ---------------- Cached services/masters (config-backed) ---------------- #

_services_cache_store: dict[str, str] | None = None
_masters_cache_store: dict[int, str] | None = None


async def services_cache() -> dict[str, str]:
    """Return cached services mapping {service_id: name} from cfg.get_services()."""
    global _services_cache_store
    if _services_cache_store is not None:
        return _services_cache_store
    try:
        # Prefer loading services from the DB (canonical source). Fall back to
        # configuration module `cfg.get_services` when DB access fails or returns
        # no rows (to support initial deployments without DB seeds).
        try:
            async with get_session() as session:
                from bot.app.domain.models import Service
                res = await session.execute(select(Service.id, Service.name))
                rows = res.all()
                if rows:
                    _services_cache_store = {str(r[0]): str(r[1]) for r in rows}
        except Exception:
            # DB access failed; fall back to configuration provider below
            _services_cache_store = None

        if not _services_cache_store:
            # IMPORTANT: do NOT fall back to seeded placeholder services from
            # configuration. Returning actual placeholders here would recreate
            # undesired default masters/services. Instead, when DB is
            # unavailable or empty, return an empty mapping so UIs show no
            # services and admins must create them explicitly.
            logger.info("services_cache: DB empty/unavailable; returning empty services mapping (no fallback placeholders)")
            _services_cache_store = {}
    except Exception as e:
        logger.exception("services_cache load failed: %s", e)
        _services_cache_store = {}
    return _services_cache_store or {}


async def masters_cache() -> dict[int, str]:
    """Return cached masters mapping {telegram_id: name} loaded from the DB.

    This queries the canonical Master table (telegram_id, name) and caches
    the result. If DB access fails or returns no rows, we return an empty
    mapping (no configuration fallback) so callers behave consistently with
    services_cache().
    """
    global _masters_cache_store
    if _masters_cache_store is not None:
        return _masters_cache_store
    try:
        # Try to load masters from the DB (canonical source)
        async with get_session() as session:
            from bot.app.domain.models import Master

            res = await session.execute(select(Master.telegram_id, Master.name))
            rows = res.all()
            if rows:
                # telegram_id could be int-like; ensure keys are ints
                _masters_cache_store = {int(r[0]): (str(r[1]) if r[1] is not None else "") for r in rows}
            else:
                logger.info("masters_cache: DB empty; returning empty masters mapping")
                _masters_cache_store = {}
    except SQLAlchemyError as e:
        logger.exception("masters_cache DB load failed: %s", e)
        _masters_cache_store = {}
    except Exception as e:
        # Catch-all to avoid breaking callers; default to empty mapping
        logger.exception("masters_cache unexpected error: %s", e)
        _masters_cache_store = {}
    return _masters_cache_store or {}


def invalidate_services_cache() -> None:
    """Invalidate services cache (useful after CRUD)."""
    global _services_cache_store
    _services_cache_store = None


def invalidate_masters_cache() -> None:
    """Invalidate masters cache (useful after CRUD)."""
    global _masters_cache_store
    _masters_cache_store = None


# ---------------- Settings API (centralized access to cfg.SETTINGS) ---------------- #
# In many places the code reads `cfg.SETTINGS` directly. Provide a small
# centralized accessor so call-sites can use `shared_services.get_setting(...)`
# and `shared_services.update_setting(...)` without touching cfg directly.

_settings_cache: dict[str, Any] | None = None
_settings_last_checked: datetime | None = None


def get_setting(key: str, default: Any = None) -> Any:
    """Return a runtime setting value.

    This is a lightweight accessor that prefers an in-memory cache seeded
    from `cfg.SETTINGS`. It is intentionally synchronous to be safe to call
    from code paths that aren't async. It does not perform DB I/O — use
    `update_setting` to persist changes.
    """
    global _settings_cache, _settings_last_checked
    try:
        if _settings_cache is None:
            # Seed from runtime config (fallback to {})
            _settings_cache = dict(getattr(cfg, "SETTINGS", {}) or {})
            _settings_last_checked = datetime.now(UTC)
        return _settings_cache.get(key, default)
    except Exception:
        return default


def get_hold_minutes(default: int = 15) -> int:
    """Return reservation_hold_minutes as an int (convenience wrapper)."""
    try:
        val = get_setting("reservation_hold_minutes", default)
        return int(val) if val is not None else int(default)
    except Exception:
        return int(default)


async def update_setting(key: str, value: Any) -> bool:
    """Persist a setting and update runtime cache and cfg.SETTINGS.

    Returns True on success (DB write attempted and no exception raised),
    False otherwise. Always updates the in-memory cache and `cfg.SETTINGS`
    so callers see the new value immediately.
    """
    global _settings_cache, _settings_last_checked
    try:
        # Update runtime cache and cfg for immediate visibility
        if _settings_cache is None:
            _settings_cache = dict(getattr(cfg, "SETTINGS", {}) or {})
        _settings_cache[str(key)] = value
        try:
            getattr(cfg, "SETTINGS", {})[str(key)] = value
        except Exception:
            # Best-effort only
            pass

        _settings_last_checked = datetime.now(UTC)

        # Persist to DB Setting table when available
        try:
            from bot.app.domain.models import Setting
            async with get_session() as session:
                from sqlalchemy import select
                s = await session.scalar(select(Setting).where(Setting.key == str(key)))
                if s:
                    s.value = str(value)
                else:
                    session.add(Setting(key=str(key), value=str(value)))
                await session.commit()
        except Exception as db_e:
            logger.warning("update_setting: DB persist failed for %s: %s", key, db_e)
            # still consider update successful for runtime
        # Call optional hook for consumers
        try:
            _safe_call("on_setting_update", key, value)
        except Exception:
            pass
        return True
    except Exception as e:
        logger.exception("update_setting failed: %s", e)
        return False


async def safe_get_locale(user_id: int | None, default: str = "uk") -> str:
    """Get locale for user, with a robust fallback.

    Returns default when user_id is None or any error occurs.
    """
    if not user_id:
        return default
    try:
        return await get_user_locale(int(user_id))
    except Exception:
        return default


def tr(key: str, *, lang: str | None = None, user_id: int | None = None, **fmt: Any) -> str:
    """Unified translation helper delegating to translations.tr()."""
    try:
        use_lang = lang or getattr(cfg, "SETTINGS", {}).get("language", "uk")
        return _tr_raw(key, lang=use_lang, **fmt)
    except Exception:
        return key


def tz_convert(dt: datetime, tz: ZoneInfo | str | None = None) -> datetime:
    """Convert a datetime to a target timezone (defaults to cfg.LOCAL_TZ).

    - If tz is a string, it is interpreted as an IANA timezone name.
    - If tz is None, uses cfg.LOCAL_TZ when available.
    - Returns dt unchanged on any error.
    """
    try:
        if tz is None:
            tz = getattr(cfg, "LOCAL_TZ", None)
        if isinstance(tz, str):
            tz = ZoneInfo(tz)
        if tz is None:
            return dt
        return dt.astimezone(tz)
    except Exception:
        return dt


def format_date(dt: datetime, fmt: str = "%d.%m %H:%M", tz: ZoneInfo | str | None = None) -> str:
    """Format datetime with optional timezone conversion and format.

    Falls back to ISO-like safe formatting on error.
    """
    try:
        return tz_convert(dt, tz).strftime(fmt)
    except Exception:
        try:
            return str(dt)
        except Exception:
            return "N/A"


async def get_booking_service_names(booking_id: int) -> str:
    """Return service display name for a booking, combining multiple items if present."""
    try:
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
                names = [r[1] or r[0] for r in rows]
                return " + ".join(names)
            # fallback single service
            return await get_service_name(b.service_id)
    except Exception:
        try:
            return await get_service_name(str(booking_id))
        except Exception:
            return str(booking_id)


async def format_master_profile(master_id: int, lang: str, *, with_title: bool = True) -> str:
    """Wrapper that fetches data and formats the profile text.

    Prefer calling the pure formatter `format_master_profile_text` with pre-fetched
    data. This wrapper keeps compatibility for callers that pass master_id.
    """
    try:
        # Import inside function to avoid import-time cycles
        from bot.app.services import master_services

        data = await master_services.get_master_profile_data(master_id)
        # If master not found, the formatter will handle None
        return format_master_profile_text(data, lang, with_title=with_title)
    except Exception as e:
        logger.exception("format_master_profile wrapper failed: %s", e)
        return tr("error", lang=lang)


def format_master_profile_text(data: dict | None, lang: str, *, with_title: bool = True) -> str:
    """Pure formatter: build profile text from pre-fetched `data`.

    `data` is expected to be the dict returned by
    `master_services.get_master_profile_data` or None.
    """
    try:
        if not data:
            return tr("master_not_found", lang=lang)

        master = data.get("master")
        services = data.get("services") or []
        durations_map = data.get("durations_map") or {}
        about_text = data.get("about_text")
        reviews = data.get("reviews") or []

        lines: list[str] = []
        if with_title:
            lines.append(tr("profile_title", lang=lang) if tr("profile_title", lang=lang) != "profile_title" else "📋 Профіль")
        uname = getattr(master, "username", None)
        master_name = getattr(master, "name", "")
        master_tid = getattr(master, "telegram_id", "")
        lines.append(f"👤 {master_name} (@{uname})" if uname else f"👤 {master_name}")
        lines.append(f"🆔 {master_tid}")
        if getattr(master, "phone", None):
            lines.append(f"📞 {getattr(master, 'phone', '')}")
        if getattr(master, "email", None):
            lines.append(f"✉️ {getattr(master, 'email', '')}")

        # Services
        if services:
            lines.append("")
            lines.append(tr("services_list_title", lang=lang))
            for sid, sname, category, price_cents, currency in services:
                dur = durations_map.get(str(sid))
                dur_txt = f"{dur} {tr('minutes_short', lang=lang)}" if isinstance(dur, int) and dur > 0 else None
                price_txt = format_money_cents(price_cents or 0, currency or "UAH")
                tail = []
                if dur_txt:
                    tail.append(f"({dur_txt})")
                if price_txt:
                    tail.append(f"— {price_txt}")
                head = f"• {sname}" if not category else f"• {category} → {sname}"
                lines.append(" ".join([head] + tail) if tail else head)
        else:
            lines.append("")
            lines.append("❌ " + tr("no_services_for_master", lang=lang))

        # Rating
        if getattr(master, "rating", None):
            lines.append("")
            rating_label = tr("rating_label", lang=lang) if tr("rating_label", lang=lang) != "rating_label" else "Рейтинг"
            orders_word = tr("orders", lang=lang) if tr("orders", lang=lang) != "orders" else "замовлень"
            lines.append(
                f"⭐ {rating_label}: {getattr(master, 'rating', 0):.1f}/5 ({int(getattr(master, 'completed_orders', 0) or 0)} {orders_word})"
            )

        if about_text:
            lines.append("")
            lines.append(tr("about_title", lang=lang) if tr("about_title", lang=lang) != "about_title" else "📝 Про себе:")
            lines.append(str(about_text))

        if reviews:
            lines.append("")
            lines.append(tr("reviews_title", lang=lang) if tr("reviews_title", lang=lang) != "reviews_title" else "💬 Відгуки:")
            for rating, comment in reviews:
                if comment:
                    lines.append(f"• \"{comment}\"")
                else:
                    lines.append(f"• ⭐ {rating}/5")

        # Schedule / working hours (optional, taken from master profile bio.schedule normalized by master_services.get_master_schedule)
        sched = data.get("schedule") or {}
        if isinstance(sched, dict):
            try:
                lines.append("")
                sched_title = tr("schedule_title", lang=lang) if tr("schedule_title", lang=lang) != "schedule_title" else "Графік роботи"
                lines.append(f"{sched_title}:")
                # localized full weekday names
                wd_full = (tr("weekday_full", lang=lang) if tr("weekday_full", lang=lang) else None) or [
                    "Понеділок",
                    "Вівторок",
                    "Середа",
                    "Четвер",
                    "П'ятниця",
                    "Субота",
                    "Неділя",
                ]
                for i in range(7):
                    key = str(i)
                    windows = sched.get(key) or []
                    if not windows:
                        lines.append(f"• {wd_full[i]}: —")
                        continue
                    # windows is list of [start, end]
                    parts: list[str] = []
                    for w in windows:
                        try:
                            if isinstance(w, (list, tuple)) and len(w) >= 2:
                                parts.append(f"{w[0]}–{w[1]}")
                            else:
                                s = str(w)
                                if "-" in s:
                                    a, b = s.split("-", 1)
                                    parts.append(f"{a.strip()}–{b.strip()}")
                        except Exception:
                            continue
                    if parts:
                        lines.append(f"• {wd_full[i]}: {', '.join(parts)}")
                    else:
                        lines.append(f"• {wd_full[i]}: —")
            except Exception:
                # non-fatal: skip schedule rendering on any error
                logger.exception("Failed to render schedule for master profile: %s", master_name)

        return "\n".join(lines)
    except Exception as e:
        logger.exception("format_master_profile_text failed: %s", e)
        return tr("error", lang=lang)


async def send_booking_notification(
    bot: Bot,
    booking_id: int,
    event_type: str,
    recipients: Sequence[int],
    *,
    previous_starts_at: datetime | None = None,
) -> None:
    """Compose and send a booking notification to recipients.

    event_type: one of
      - 'paid' | 'cancelled' | 'cash_confirmed'
      - 'rescheduled_by_client' | 'rescheduled_by_master'
    """
    from bot.app.domain.models import Booking, User
    try:
        b = await BookingRepo.get(booking_id)
        if not b:
            return
        # datetime local
        try:
            dt_txt = format_date(b.starts_at)
        except Exception:
            dt_txt = "N/A"
        # service names
        svc_names = await get_booking_service_names(booking_id)
        # client display
        client_name = getattr(b, "client_name", None) or ""
        client_username = getattr(b, "client_username", None)
        if not client_name:
            # Try to resolve from user
            u = await UserRepo.get_by_id(b.user_id) if getattr(b, "user_id", None) else None
            if u and getattr(u, "name", None):
                client_name = u.name
            # For role routing we also need the telegram id of the client
        client_tg_id = None
        try:
            u = await UserRepo.get_by_id(b.user_id) if getattr(b, "user_id", None) else None
            client_tg_id = getattr(u, "telegram_id", None) if u else None
        except Exception:
            client_tg_id = None
        client_line = f"{client_name} (@{client_username})" if client_username else client_name
        # price
        price_txt = format_money_cents(getattr(b, "final_price_cents", 0) or getattr(b, "original_price_cents", 0))
        # Build texts per recipient
        logger.info("send_booking_notification: booking=%s event=%s recipients=%s", booking_id, event_type, recipients)
        for rid in recipients:
            try:
                rid_int = int(rid)
            except Exception:
                logger.warning("send_booking_notification: invalid recipient id, skipping: %r", rid)
                continue
            lang = await safe_get_locale(rid_int)
            # Titles per event/role
            if event_type == "paid":
                title = tr("notif_paid_confirmed", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
            elif event_type == "cash_confirmed":
                title = tr("notif_cash_confirmed", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
            elif event_type == "cancelled":
                # Include who cancelled in the title when possible
                try:
                    title = tr("notif_client_cancelled", lang=lang).format(id=booking_id, user=client_line)
                except Exception:
                    title = tr("notif_client_cancelled", lang=lang)
            elif event_type == "rescheduled_by_client":
                # Master sees a role-specific title; admins get generic client-rescheduled
                if int(rid) == int(getattr(b, "master_id", 0)):
                    title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                else:
                    title = tr("notif_client_rescheduled", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
            elif event_type == "rescheduled_by_master":
                # Client sees "master rescheduled"; admins see admin variant with master id
                if client_tg_id and int(rid) == int(client_tg_id):
                    title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                else:
                    title = tr("notif_master_rescheduled_admin", lang=lang).format(master=getattr(b, "master_id", ""), id=booking_id, service=svc_names, dt=dt_txt)
            else:
                # Fallback to a neutral title
                title = f"#{booking_id}: {svc_names} {dt_txt}"
            # Compose body (consistent fields)
            # Use canonical booking summary formatter for the body so all
            # consumers share the same representation. Pass recipient's
            # locale so localized labels are used.
            try:
                bd = await build_booking_details(b, user_id=rid, lang=lang)
                body = format_booking_details_text(bd, lang)
            except Exception:
                # Fallback to the previous explicit body composition
                body = (
                    f"🆔 ID: {booking_id}\n"
                    f"👤 {tr('client_label', lang=lang)}: {client_line}\n"
                    f"💇 {tr('service_label', lang=lang)}: {svc_names}\n"
                    f"📅 {dt_txt}\n"
                    f"💰 {tr('amount_label', lang=lang)}: {price_txt}"
                )
            try:
                await bot.send_message(chat_id=rid_int, text=f"{title}\n\n{body}")
                logger.info("send_booking_notification: sent to %s", rid_int)
            except Exception as se:
                logger.warning("Failed to send notification to %s: %s", rid_int, se)
    except Exception as e:
        logger.exception("send_booking_notification failed: %s", e)


@dataclass
class BookingDetails:
    """Canonical representation of booking data for presentation.

    Keep this small — fields can be expanded if consumers need more.
    """
    booking_id: int
    service_name: str | None = None
    master_name: str | None = None
    price_cents: int = 0
    currency: str = "UAH"
    starts_at: datetime | None = None
    date_str: str | None = None
    client_id: int | None = None
    raw: Any | None = None
    # Fields requested by master UI
    status: str | None = None
    client_name: str | None = None
    client_phone: str | None = None
    client_telegram_id: int | None = None
    client_username: str | None = None
    # Computed permissions for client actions
    can_cancel: bool = False
    can_reschedule: bool = False


async def build_booking_details(
    booking: object,
    service_name: str | None = None,
    master_name: str | None = None,
    user_id: int | None = None,
    date: str | None = None,
    lang: str | None = None,
) -> BookingDetails:
    """Build a BookingDetails instance from a booking id, model or dict.

    This centralizes the fetch logic (DB access) in the service layer.
    """
    # Resolve language for locale-sensitive lookups (best-effort)
    try:
        if not lang and user_id:
            lang = await safe_get_locale(user_id)
    except Exception:
        lang = getattr(cfg, "SETTINGS", {}).get("language", "uk")

    data: dict | None = None
    try:
        # import here to avoid cycles
        from bot.app.services import master_services

        if isinstance(booking, dict):
            data = booking
        else:
            bid = getattr(booking, "id", None) or (booking if isinstance(booking, int) else None)
            if bid is not None:
                data = await master_services.get_booking_display_data(int(bid))
    except Exception:
        data = None

    # If fetch failed, build minimal data from provided args/object
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

    # Apply overrides from builder args
    if service_name:
        data["service_name"] = service_name
    if master_name:
        data["master_name"] = master_name
    if date:
        data["date_str"] = date

    # Try to enrich with Booking/User models when available to provide
    # status and client contact fields used by master UI.
    status_val = data.get("status")
    client_name = data.get("client_name")
    client_phone = data.get("client_phone")
    client_tg = data.get("client_telegram_id") or data.get("client_tid") or data.get("client_tg_id")
    client_username = data.get("client_username")

    # If not present, attempt DB fetch to enrich data
    try:
        from bot.app.domain.models import Booking, BookingStatus

        # Use BookingRepo to fetch canonical booking model when possible
        b = None
        try:
            b = await BookingRepo.get(int(data.get("booking_id") or 0))
        except Exception:
            b = None

        if b is not None:
            # status
            try:
                status_val = getattr(b.status, "value", str(b.status))
            except Exception:
                status_val = str(getattr(b, "status", ""))
            # starts_at may be more reliable on the model
            if not data.get("starts_at") and getattr(b, "starts_at", None):
                data["starts_at"] = b.starts_at
            # price fallback
            if not data.get("price_cents"):
                data["price_cents"] = getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", None) or 0
            # try to load user info
            try:
                if getattr(b, "user_id", None):
                    u = await UserRepo.get_by_id(int(b.user_id))
                    if u:
                        client_name = client_name or getattr(u, "name", None)
                        client_tg = client_tg or getattr(u, "telegram_id", None)
                        client_username = client_username or getattr(u, "username", None)
            except Exception:
                pass
    except Exception:
        # domain models / repo not available — continue with what we have
        pass

    # Compute permission booleans based on starts_at and global settings
    can_cancel = False
    can_reschedule = False
    try:
        starts_at_dt = data.get("starts_at")
        if starts_at_dt:
            # compute seconds until start in UTC
            now_utc = datetime.now(UTC)
            try:
                starts_utc = starts_at_dt.astimezone(UTC)
            except Exception:
                starts_utc = starts_at_dt
            delta_seconds = (starts_utc - now_utc).total_seconds()
            # lock hours from config
            lock_r = getattr(cfg, "get_client_reschedule_lock_hours", lambda: 3)()
            lock_c = getattr(cfg, "get_client_cancel_lock_hours", lambda: 3)()
            can_reschedule = delta_seconds >= (lock_r * 3600)
            can_cancel = delta_seconds >= (lock_c * 3600)
        else:
            # Without start time be conservative
            can_cancel = False
            can_reschedule = False
    except Exception:
        can_cancel = False
        can_reschedule = False

    # Enforce status-based blocking: finished/no-show/expired bookings should
    # never be cancellable or reschedulable regardless of lock windows.
    try:
        from bot.app.domain.models import BookingStatus

        sval_norm = str(status_val).upper() if status_val is not None else ""
        if sval_norm in {
            getattr(BookingStatus, "DONE").value,
            getattr(BookingStatus, "NO_SHOW").value,
            getattr(BookingStatus, "EXPIRED").value,
            getattr(BookingStatus, "CANCELLED").value,
        }:
            can_cancel = False
            can_reschedule = False
    except Exception:
        # If we cannot import BookingStatus or something else fails, keep
        # the previously computed booleans.
        pass

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


async def format_booking_details(
    booking: object,
    service_name: str | None = None,
    master_name: str | None = None,
    user_id: int | None = None,
    date: str | None = None,
    lang: str | None = None,
    role: str = "client",
) -> str:
    """Compatibility wrapper: build BookingDetails and call the pure formatter.

    Existing callers that expect a string can keep using this function.
    """
    details = await build_booking_details(booking, service_name, master_name, user_id, date, lang)
    return format_booking_details_text(details, lang, role=role)


def format_booking_details_text(data: dict | BookingDetails, lang: str | None = None, role: str = "client") -> str:
    """Pure formatter that builds booking details text from pre-fetched data.

    Accepts either a dict or BookingDetails. Performs no DB access.
    """
    from bot.app.translations import tr
    try:
        _lang = lang or getattr(cfg, "SETTINGS", {}).get("language", "uk")
        _tr = lambda k: tr(k, lang=_lang)

        if isinstance(data, BookingDetails):
            booking_id = data.booking_id
            price_cents = data.price_cents or 0
            currency = data.currency or "UAH"
            service_name = data.service_name
            master_name = data.master_name
            date_str = data.date_str
            starts_at = data.starts_at
        else:
            booking_id = data.get("booking_id", 0)
            price_cents = data.get("price_cents", 0) or 0
            currency = data.get("currency", "UAH")
            service_name = data.get("service_name")
            master_name = data.get("master_name")
            date_str = data.get("date_str")
            starts_at = data.get("starts_at")

        human_price = format_money_cents(price_cents, currency)

        service_name = service_name or _tr("service_label")
        master_name = master_name or _tr("master_label")

        if not date_str:
            if starts_at:
                try:
                    from bot.app.services.client_services import LOCAL_TZ

                    dt_local = starts_at.astimezone(LOCAL_TZ)
                    date_str = f"{dt_local:%d.%m.%Y}"
                except Exception:
                    date_str = "—"
            else:
                date_str = "—"

        # Build a single unified output (base + optional role tweaks)
        lines: list[str] = []
        lines.append(f"<b>{_tr('booking_label')} #{booking_id}</b>")
        lines.append(f"{_tr('service_label')}: <b>{service_name}</b>")
        lines.append(f"{_tr('master_label')}: {master_name}")
        lines.append(f"{_tr('date_label')}: <b>{date_str}</b>")
        lines.append(f"{_tr('amount_label')}: {human_price}")

        if str(role).lower() == "master":
            try:
                # status line
                st_val = None
                if isinstance(data, BookingDetails):
                    st_val = getattr(data, 'status', None)
                else:
                    st_val = data.get('status')
                if st_val:
                    lines.append(f"{_tr('status_label')}: {st_val}")

                # client contact block
                client_display = None
                client_phone = None
                client_tg = None
                client_un = None
                if isinstance(data, BookingDetails):
                    client_display = getattr(data, 'client_name', None)
                    client_phone = getattr(data, 'client_phone', None)
                    client_tg = getattr(data, 'client_telegram_id', None)
                    client_un = getattr(data, 'client_username', None)
                else:
                    client_display = data.get('client_name')
                    client_phone = data.get('client_phone')
                    client_tg = data.get('client_telegram_id') or data.get('client_tid') or data.get('client_tg_id')
                    client_un = data.get('client_username')

                if client_display:
                    if client_un:
                        lines.insert(1, f"{_tr('client_label')}: {client_display} (@{client_un})")
                    elif client_tg:
                        try:
                            lines.insert(1, f"{_tr('client_label')}: <a href='tg://user?id={int(client_tg)}'>{client_display}</a>")
                        except Exception:
                            lines.insert(1, f"{_tr('client_label')}: {client_display}")
                    else:
                        lines.insert(1, f"{_tr('client_label')}: {client_display}")
                if client_phone:
                    lines.insert(2, f"{_tr('phone_label')}: {client_phone}")
            except Exception:
                # keep base block on any failure
                pass
        # Completed successfully: return assembled lines
        return "\n".join(lines)
    except Exception:
        # On unexpected errors, return a minimal string
        try:
            if isinstance(data, BookingDetails):
                return str(data.booking_id)
            return str(data.get("booking_id", "—"))
        except Exception:
            return "—"


async def _safe_send(bot: Bot, chat_id: int | str, text: str, reply_markup: Any = None, **kwargs: Any) -> bool:
    """Best-effort send wrapper for bot.send_message.

    - Swallows and logs exceptions and returns False on failure.
    - Returns True on success.
    - Use local import of Bot type to keep typing but avoid heavy runtime deps.
    """
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, **kwargs)
        return True
    except Exception as e:
        try:
            logger.warning("_safe_send failed for %s: %s", chat_id, e)
        except Exception:
            pass
        return False