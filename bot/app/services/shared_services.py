from __future__ import annotations
import logging
import os
import re
from importlib import import_module
from typing import Any, Dict, Iterable, Sequence, Mapping
from dataclasses import dataclass


from sqlalchemy import select, or_
from sqlalchemy import func, delete
from sqlalchemy.exc import SQLAlchemyError

from bot.app.core.db import get_session
from bot.app.core.constants import DEFAULT_SERVICE_FALLBACK_DURATION
from bot.app.domain.models import User
from bot.app.translations import tr as _tr_raw
from aiogram import Bot
try:
    from aiogram.exceptions import TelegramAPIError
except Exception:
    # If aiogram isn't available at import time (tests, static analysis),
    # set to None so we don't accidentally catch all Exceptions below.
    TelegramAPIError = None
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def get_env_int(name: str, default: int) -> int:
    """Read an int from environment with a safe default.

    - Returns `default` if variable is missing/empty or not an int.
    - Logs a warning on invalid values to aid diagnostics.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r, defaulting to %s", name, raw, default)
        return default


def _default_language() -> str:
    return os.getenv("DEFAULT_LANGUAGE") or os.getenv("LANGUAGE") or "uk"


def _default_currency() -> str:
    return os.getenv("CURRENCY") or "UAH"


def _parse_env_int_list(name: str) -> list[int]:
    raw = os.getenv(name, "") or ""
    if not raw.strip():
        return []
    parts = re.split(r"[\s,]+", raw.strip())
    result: list[int] = []
    for part in parts:
        if part and part.isdigit():
            result.append(int(part))
    return result


def default_language() -> str:
    return _default_language()


def format_user_display_name(username: str | None, first_name: str | None, last_name: str | None) -> str | None:
    """Return the best available display name for a Telegram user."""
    try:
        uname = (username or "").strip()
    except Exception:
        uname = ""
    if uname:
        return uname
    parts: list[str] = []
    for value in (first_name, last_name):
        try:
            if value:
                trimmed = str(value).strip()
                if trimmed:
                    parts.append(trimmed)
        except Exception:
            continue
    if parts:
        return " ".join(parts)
    return None


def _env_with_fallback(primary: str, fallback: str, default: str) -> str:
    val = os.getenv(primary)
    if val is None or not val.strip():
        val = os.getenv(fallback)
    if val is None or not val.strip():
        return default
    return val.strip()


async def get_contact_info() -> dict[str, str]:
    # Try to read contact settings from DB-backed SettingsRepo first (preferred).
    # Fall back to environment variables only if DB values are missing/unavailable.
    phone = None
    instagram = None
    address = None
    try:
        # Import lazily to avoid circular imports at module import time
        from bot.app.services.admin_services import SettingsRepo

        # Try a few canonical keys in Settings table. SettingsRepo.get_setting
        # is best-effort: if it's missing or raises, we'll fallback to env below.
        try:
            phone = await SettingsRepo.get_setting("contact_phone", None)
        except Exception:
            phone = None
        try:
            instagram = await SettingsRepo.get_setting("contact_instagram", None)
        except Exception:
            instagram = None
        try:
            address = await SettingsRepo.get_setting("contact_address", None)
        except Exception:
            address = None
    except Exception:
        # SettingsRepo not available or import failed; fall back to env below
        phone = instagram = address = None

    # Resolve from env if DB value is missing or empty
    phone_val = (str(phone).strip() if phone is not None and str(phone).strip() else None) or _env_with_fallback("CONTACT_PHONE", "BUSINESS_PHONE", "+380671234567")
    instagram_val = (str(instagram).strip() if instagram is not None and str(instagram).strip() else None) or _env_with_fallback("CONTACT_INSTAGRAM", "BUSINESS_INSTAGRAM", "https://instagram.com/salon_name")
    address_val = (str(address).strip() if address is not None and str(address).strip() else None) or _env_with_fallback("CONTACT_ADDRESS", "BUSINESS_ADDRESS", "м. Київ, вул. Хрещатик, 1")

    return {
        "phone": phone_val,
        "instagram": instagram_val,
        "address": address_val,
    }


def get_admin_ids() -> list[int]:
    return _parse_env_int_list("ADMIN_IDS")


def get_master_ids() -> list[int]:
    return _parse_env_int_list("MASTER_IDS")


def _resolve_local_tz() -> ZoneInfo | None:
    tz_name = os.getenv("LOCAL_TIMEZONE", "Europe/Kyiv")
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return None


LOCAL_TZ = _resolve_local_tz()

"""Shared utilities used across service layers.

Persistence guideline:
    • Always store timestamps in UTC using datetime.now(UTC) or aware UTC values.
    • Convert to the local timezone ONLY when rendering text for users.
    • LOCAL_TZ is resolved once at startup; call get_local_tz() at render time
        to re-resolve from env (LOCAL_TIMEZONE) if container config changes.

This module provides lightweight, DB-agnostic helpers (money formatting,
status emoji, simple list item formatting, locale resolution, notification
sending, and minimal inline keyboard primitives). Repository/database logic
lives in role-specific modules (admin_services, client_services, master_services)
to keep responsibilities clear and avoid circular imports.
"""

# Эмодзи для статусов
STATUS_EMOJI: Dict[str, str] = {
    "paid": "✅",
    "confirmed": "💵",
    "pending_payment": "⏳",
    "reserved": "🟡",
    "expired": "⌛",
    "active": "🟢",  # legacy
    "cancelled": "❌",
    "done": "✔️",
    "no_show": "👻",
}

# --- Payments/provider runtime cache (shared helper; used across modules) ---
_PAYMENTS_ENABLED: bool | None = None
_PROVIDER_TOKEN_CACHE: str | None = None
_PAYMENTS_LAST_CHECKED: datetime | None = None
_PROVIDER_LAST_CHECKED: datetime | None = None



def _settings_cache_expired(last_checked: datetime | None) -> bool:
    """Return True when a settings cache timestamp is considered expired.

    This is a local implementation used by payment/provider helpers. It uses
    a TTL from environment (``SETTINGS_CACHE_TTL_SECONDS``)
    with a conservative default of 60 seconds.
    """
    try:
        _ttl = int(os.getenv("SETTINGS_CACHE_TTL_SECONDS", "60"))
    except ValueError:
        _ttl = 60
    if last_checked is None:
        return True
    try:
        return (utc_now() - last_checked) > timedelta(seconds=_ttl)
    except Exception:
        return True


async def is_telegram_payments_enabled() -> bool:
    """Check whether Telegram Payments are enabled using a shared store.

    Priority:
      1) DB-backed runtime settings via SettingsRepo (shared across processes)
      2) Environment fallback (for initial bootstrap)

    A small TTL is kept to reduce DB chatter, but the source of truth is the
    Settings table to avoid per-process divergence.
    """
    global _PAYMENTS_ENABLED, _PAYMENTS_LAST_CHECKED
    try:
        from bot.app.services.admin_services import SettingsRepo, load_settings_from_db
        if _PAYMENTS_ENABLED is None or _settings_cache_expired(_PAYMENTS_LAST_CHECKED):
            try:
                await load_settings_from_db()
            except Exception:
                pass
            val = await SettingsRepo.get_setting("telegram_payments_enabled", None)
            if val is None:
                val = _env_bool("TELEGRAM_PAYMENTS_ENABLED", True)
            _PAYMENTS_ENABLED = bool(val)
            _PAYMENTS_LAST_CHECKED = utc_now()
            logger.debug("Telegram Payments (shared) refresh: %s", _PAYMENTS_ENABLED)
        return bool(_PAYMENTS_ENABLED)
    except Exception:
        # Fallback to env-only behavior if repos are unavailable
        if _PAYMENTS_ENABLED is None or _settings_cache_expired(_PAYMENTS_LAST_CHECKED):
            _PAYMENTS_ENABLED = _env_bool("TELEGRAM_PAYMENTS_ENABLED", True)
            _PAYMENTS_LAST_CHECKED = utc_now()
        return bool(_PAYMENTS_ENABLED)


async def toggle_telegram_payments() -> bool:
    """Toggle Telegram payments using a shared store (DB settings), with env fallback.

    Persist the new value via SettingsRepo.update_setting so all processes can
    observe it, avoiding per-process globals divergence.
    """
    global _PAYMENTS_ENABLED, _PAYMENTS_LAST_CHECKED
    try:
        new_val = not await is_telegram_payments_enabled()
        from bot.app.services.admin_services import SettingsRepo
        ok = await SettingsRepo.update_setting("telegram_payments_enabled", bool(new_val))
        if not ok:
            logger.warning("toggle_telegram_payments: DB persist failed; falling back to env only")
        _PAYMENTS_ENABLED = bool(new_val)
        _PAYMENTS_LAST_CHECKED = utc_now()
        # Keep env in sync for compatibility
        os.environ["TELEGRAM_PAYMENTS_ENABLED"] = "1" if new_val else "0"
        logger.info("Telegram Payments toggled (shared): %s", new_val)
        return bool(new_val)
    except Exception:
        # Env-only fallback
        new_val = not await is_telegram_payments_enabled()
        _PAYMENTS_ENABLED = bool(new_val)
        _PAYMENTS_LAST_CHECKED = utc_now()
        os.environ["TELEGRAM_PAYMENTS_ENABLED"] = "1" if new_val else "0"
        logger.info("Telegram Payments toggled (env fallback): %s", new_val)
        return bool(new_val)


async def get_telegram_provider_token(force_reload: bool = False) -> str | None:
    """Return Telegram Payments provider token from shared settings with env fallback."""
    global _PROVIDER_TOKEN_CACHE, _PROVIDER_LAST_CHECKED
    try:
        if not force_reload and _PROVIDER_TOKEN_CACHE and not _settings_cache_expired(_PROVIDER_LAST_CHECKED):
            return _PROVIDER_TOKEN_CACHE
        token: str | None = None
        try:
            from bot.app.services.admin_services import SettingsRepo
            token = await SettingsRepo.get_setting("telegram_provider_token", None)
        except Exception:
            token = None
        if not token:
            token = os.getenv("TELEGRAM_PAYMENT_PROVIDER_TOKEN")
        _PROVIDER_TOKEN_CACHE = token or None
        _PROVIDER_LAST_CHECKED = utc_now()
        return token or None
    except Exception as e:
        logger.warning("Failed to resolve Telegram provider token: %s", e)
        return None


async def is_online_payments_available() -> bool:
    """Return True when Telegram online payments can be offered to clients.

    Both the feature flag and a valid provider token are required.
    """
    try:
        enabled = await is_telegram_payments_enabled()
        token = await get_telegram_provider_token()
        return bool(enabled and token)
    except Exception as e:
        logger.warning("Online payments availability check failed: %s", e)
        return False


def format_money_cents(cents: int | float | None, currency: str | None = None) -> str:
    """Форматирует сумму в копейках в читаемый вид (например, '100.00 UAH').

    Args:
        cents: Сумма в копейках.
        currency: Валюта (по умолчанию UAH).

    Returns:
        Отформатированная строка с суммой.
    """
    try:
        if not currency:
            currency = _default_currency()
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


# ---------------- Time utilities (shared) ---------------- #
def _parse_hm_to_minutes(hm: str) -> int:
    """Parse 'HH:MM' into minutes since midnight."""
    try:
        parts = str(hm).split(":")
        h = int(parts[0]) if parts and parts[0] != "" else 0
        # Be defensive: parts[1] may be an empty string (e.g. '9:'), which
        # would raise ValueError on int('') and be caught below returning 0.
        # Instead treat an empty minute part as 0.
        m = int(parts[1]) if len(parts) > 1 and parts[1] != "" else 0
        return max(0, min(23, h)) * 60 + max(0, min(59, m))
    except Exception:
        return 0


def _minutes_to_hm(minutes: int) -> str:
    minutes = max(0, min(24 * 60 - 1, int(minutes)))
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def get_local_tz() -> ZoneInfo:
    """Return dynamic local timezone for rendering (fallback to cached or UTC)."""
    try:
        tz_name = os.getenv("LOCAL_TIMEZONE", None)
        if tz_name:
            return ZoneInfo(tz_name)
    except Exception:
        pass
    return LOCAL_TZ or ZoneInfo("UTC")
    # Time helpers: prefer these helpers throughout the codebase so all
    # modules consistently produce timezone-aware datetimes.
from datetime import timezone


def utc_now() -> datetime:
    """Return current time as an aware UTC datetime."""
    try:
        return datetime.now(UTC)
    except Exception:
        return datetime.now(timezone.utc)


def local_now() -> datetime:
    """Return current time in the configured local timezone (aware).

    Falls back to UTC when local timezone resolution fails.
    """
    try:
        return datetime.now(get_local_tz())
    except Exception:
        return datetime.now(UTC)


def format_slot_label(slot: datetime | None, fmt: str = "%H:%M", tz: ZoneInfo | str | None = None) -> str:
    """Format a single UI time slot consistently across the app.

    - `slot` may be a `datetime` or `time`-like object; when `None` returns empty string.
    - `fmt` defaults to ``"%H:%M"`` and can be overridden for other layouts.
    - `tz` may be a `zoneinfo.ZoneInfo` or a string timezone name; when provided,
      the slot will be converted to that timezone before formatting.

    This consolidates UI slot rendering (buttons, compact pickers) so the
    format is applied uniformly and can centralize future localization.
    """
    if slot is None:
        return ""
    try:
        # Resolve timezone preference
        if tz is None:
            lt = get_local_tz()
        else:
            lt = tz if isinstance(tz, ZoneInfo) else ZoneInfo(str(tz))
        # If slot is a datetime with tzinfo, convert; if it's time-only, just format
        if hasattr(slot, "tzinfo") and getattr(slot, "tzinfo") is not None:
            try:
                return slot.astimezone(lt).strftime(fmt)
            except Exception:
                return slot.strftime(fmt) if hasattr(slot, "strftime") else str(slot)
        # Fallback formatting for naive datetimes or time objects
        return slot.strftime(fmt) if hasattr(slot, "strftime") else str(slot)
    except Exception:
        try:
            return slot.strftime(fmt) if hasattr(slot, "strftime") else str(slot)
        except Exception:
            logger.exception("format_slot_label failed for slot=%s", slot)
            return str(slot)


def ensure_utc(dt: datetime | None) -> datetime | None:
    """Convert given datetime to an aware UTC datetime.

    If `dt` is naive, interpret it as UTC (do not guess local timezone).
    Returns None when `dt` is None.
    """
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        try:
            return dt.replace(tzinfo=UTC)
        except Exception:
            return None


def _decode_time(tok: str | None) -> str | None:
    """Normalize various time token formats into HH:MM or return None.

    Accepts tokens like '0900', '900', '09:00' and returns '09:00' etc.
    """
    if not tok:
        return None
    try:
        if ":" in tok:
            return tok
        tok = str(tok).strip()
        if len(tok) == 4 and tok.isdigit():
            return f"{tok[:2]}:{tok[2:]}"
        if len(tok) == 3 and tok.isdigit():
            return f"{tok[0]}:{tok[1:]}"
        return tok
    except Exception:
        return None


async def get_service_duration(session, service_id: str | None, master_id: int | None = None) -> int:
    """Resolve the effective duration (minutes) for a service+master pair.

    Resolution order:
      1. If `master_id` provided, check `master_services.duration_minutes`.
      2. Check `services.duration_minutes` (canonical service-level value).
      3. Check `service_profiles.duration_minutes` (legacy profile data).
      4. Fallback to `SettingsRepo.get_slot_duration()` or
         `DEFAULT_SERVICE_FALLBACK_DURATION`.

    This helper is async and takes a SQLAlchemy `session` so callers can
    reuse their existing transaction/session and avoid extra roundtrips.
    """
    try:
        # Lazy imports to avoid circular dependencies at module-import time
        from bot.app.domain.models import MasterService, Service, ServiceProfile
        from bot.app.services.admin_services import SettingsRepo

        # 1) master-specific override
        if master_id is not None and service_id is not None:
            try:
                ms = await session.scalar(
                    select(MasterService).where(
                        MasterService.master_id == int(master_id),
                        MasterService.service_id == service_id,
                    )
                )
                if ms and getattr(ms, "duration_minutes", None):
                    return int(getattr(ms, "duration_minutes") or 0)
            except Exception:
                # best-effort: continue to other fallbacks
                pass

        # 2) service-level duration
        if service_id is not None:
            try:
                svc = await session.scalar(select(Service).where(Service.id == service_id))
                if svc and getattr(svc, "duration_minutes", None):
                    return int(getattr(svc, "duration_minutes") or 0)
            except Exception:
                pass

        # 3) legacy service_profile
        if service_id is not None:
            try:
                sp = await session.scalar(select(ServiceProfile).where(ServiceProfile.service_id == service_id))
                if sp and getattr(sp, "duration_minutes", None):
                    return int(getattr(sp, "duration_minutes") or 0)
            except Exception:
                pass

        # 4) settings fallback
        try:
            val = await SettingsRepo.get_slot_duration()
            if isinstance(val, int) and val > 0:
                return int(val)
        except Exception:
            pass

    except Exception:
        # If imports or lookups fail, fall through to hardcoded default
        pass

    return int(DEFAULT_SERVICE_FALLBACK_DURATION)


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


def _format_client_booking_row(fields: dict[str, str]) -> str:
    status_label = str(fields.get("status_label") or "")
    st = str(fields.get("st") or "")
    dt = str(fields.get("dt") or "")
    master_name = str(fields.get("master_name") or "")
    service_name = str(fields.get("service_name") or "")
    price_txt = str(fields.get("price_txt") or "")
    return f"{status_label} {st} {dt} {master_name[:20]} {service_name[:24]} {price_txt}".strip()


def _format_master_booking_row(fields: dict[str, str]) -> str:
    status_label = str(fields.get("status_label") or "")
    st = str(fields.get("st") or "")
    dt = str(fields.get("dt") or "")
    client_name = str(fields.get("client_name") or "")
    service_name = str(fields.get("service_name") or "")
    price_txt = str(fields.get("price_txt") or "")
    return f"{status_label} {st} {dt} {client_name[:20]} {service_name[:24]} {price_txt}".strip()


def _format_admin_booking_row(fields: dict[str, str]) -> str:
    status_label = str(fields.get("status_label") or "")
    st = str(fields.get("st") or "")
    dt = str(fields.get("dt") or "")
    master_name = str(fields.get("master_name") or "")
    client_name = str(fields.get("client_name") or "")
    service_name = str(fields.get("service_name") or "")
    price_txt = str(fields.get("price_txt") or "")
    return f"{status_label} {st} {dt} {master_name[:20]} / {client_name[:20]} {service_name[:20]} {price_txt}".strip()


def format_booking_list_item(row: Any, role: str = "client", lang: str = "uk") -> tuple[str, int]:
    """Format a booking entry for compact list display.

    The input is normalized into a stable DTO first to avoid brittle heuristics
    in this function. Repositories should prefer returning BookingInfo directly.
    """
    info = normalize_booking_row(row)
    data = {
        "id": info.id,
        "master_id": info.master_id,
        "service_id": info.service_id,
        "status": info.status,
        "starts_at": info.starts_at,
        "original_price_cents": info.original_price_cents,
        "final_price_cents": info.final_price_cents,
        "currency": info.currency,
        "master_name": info.master_name,
        "service_name": info.service_name,
        "client_name": info.client_name,
        "client_username": info.client_username,
    }

    bid = int(data.get("id") or data.get("booking_id") or 0)
    starts_at = data.get("starts_at")
    master_name = str(data.get("master_name") or data.get("master_id") or "")
    service_name = str(data.get("service_name") or data.get("service_id") or "")
    client_name = str(data.get("client_name") or data.get("user_name") or data.get("user_id") or "")
    client_username = data.get("client_username")
    if client_username:
        client_name = f"{client_name} (@{client_username})" if client_name else f"@{client_username}"
    st = dt = ""
    if starts_at:
        try:
            lt = get_local_tz()
            st = format_slot_label(starts_at, fmt="%H:%M", tz=lt)
            dt = format_date(starts_at, "%d.%m", tz=lt)
        except Exception:
            st = dt = ""
    price_cents = data.get("final_price_cents") or data.get("original_price_cents")
    currency = data.get("currency") or _default_currency()
    price_txt = format_money_cents(int(price_cents), currency) if price_cents else ""
    status_val = data.get("status")
    status_label = status_to_emoji(status_val) if status_val is not None else ""
    row_fields = {
        "status_label": status_label,
        "st": st,
        "dt": dt,
        "master_name": master_name,
        "service_name": service_name,
        "client_name": client_name,
        "price_txt": price_txt,
    }
    formatter = {
        "master": _format_master_booking_row,
        "admin": _format_admin_booking_row,
    }
    formatter_fn = formatter.get(str(role).lower(), _format_client_booking_row)
    text = formatter_fn(row_fields)
    return text, bid


# ---- Stable DTO + normalizer for booking list items ----
@dataclass
class BookingInfo:
    id: int | None = None
    master_id: int | None = None
    master_name: str | None = None
    service_id: str | None = None
    service_name: str | None = None
    status: Any | None = None
    starts_at: datetime | None = None
    original_price_cents: int | None = None
    final_price_cents: int | None = None
    currency: str | None = None
    client_name: str | None = None
    client_username: str | None = None
    client_id: int | None = None


def booking_info_from_mapping(data: Mapping[str, Any]) -> BookingInfo:
    return BookingInfo(
        id=_to_int(data.get("id") or data.get("booking_id")),
        master_id=_to_int(data.get("master_id")),
        master_name=_to_str(data.get("master_name")),
        service_id=_to_str(data.get("service_id")),
        service_name=_to_str(data.get("service_name")),
        status=data.get("status"),
        starts_at=data.get("starts_at"),
        original_price_cents=_to_int(data.get("original_price_cents")),
        final_price_cents=_to_int(data.get("final_price_cents")),
        currency=_to_str(data.get("currency") or "UAH"),
        client_name=_to_str(data.get("client_name")),
        client_username=_to_str(data.get("client_username")),
        client_id=_to_int(data.get("client_id") or data.get("user_id")),
    )


def normalize_booking_row(row: Any) -> BookingInfo:
    """Normalize diverse DB row shapes into a stable BookingInfo DTO."""
    try:
        if isinstance(row, BookingInfo):
            return row
        if isinstance(row, Mapping):
            return booking_info_from_mapping(row)
        if hasattr(row, "_mapping"):
            m = dict(row._mapping)  # type: ignore[attr-defined]
            return normalize_booking_row(m)
        return booking_info_from_mapping({
            "id": getattr(row, "id", None),
            "master_id": getattr(row, "master_id", None),
            "master_name": getattr(row, "master_name", None),
            "service_id": getattr(row, "service_id", None),
            "service_name": getattr(row, "service_name", None),
            "status": getattr(row, "status", None),
            "starts_at": getattr(row, "starts_at", None),
            "original_price_cents": getattr(row, "original_price_cents", None),
            "final_price_cents": getattr(row, "final_price_cents", None),
            "currency": getattr(row, "currency", None) or "UAH",
            "client_name": getattr(row, "client_name", None),
            "client_username": getattr(row, "client_username", None),
            "client_id": getattr(row, "client_id", None) or getattr(row, "user_id", None),
        })
    except Exception:
        return BookingInfo()


def _to_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _to_str(v: Any) -> str | None:
    try:
        if v is None:
            return None
        s = str(v)
        return s
    except Exception:
        return None


def format_booking_details_text(data: dict | Any, lang: str | None = None, role: str = "client") -> str:
    """Pure formatter that builds booking details text from pre-fetched data.

    Accepts a dict-like or object with attributes. Avoids DB access and side-effects.
    Canonical single source of truth for all roles (client/master/admin).
    """
    try:
        _lang = lang or default_language()
        __ = lambda k: tr(k, lang=_lang)

        # Extract fields from either object or dict
        def _get(attr: str, default: Any = None) -> Any:
            if isinstance(data, dict):
                return data.get(attr, default)
            return getattr(data, attr, default)

        booking_id = _get("booking_id", 0)
        price_cents = _get("price_cents", 0) or 0
        currency = _get("currency", "UAH")
        service_name = _get("service_name", None)
        master_name = _get("master_name", None)
        date_str = _get("date_str", None)
        starts_at = _get("starts_at", None)
        ends_at = _get("ends_at", None)
        duration_minutes: int | None = None
        try:
            if starts_at and ends_at:
                diff = ends_at - starts_at
                duration_minutes = int(diff.total_seconds() // 60)
        except Exception:
            duration_minutes = None
        if duration_minutes is None:
            # Look for explicit duration field in raw data
            duration_minutes = _get("duration_minutes", None)
        if not isinstance(duration_minutes, int) or duration_minutes <= 0:
            # Fallback to global slot duration
            duration_minutes = DEFAULT_SERVICE_FALLBACK_DURATION

        human_price = format_money_cents(int(price_cents), currency)
        service_name = service_name or __("service_label")
        master_name = master_name or __("master_label")

        if not date_str:
            if starts_at:
                try:
                    dt_local = starts_at.astimezone(get_local_tz())
                    date_str = f"{dt_local:%d.%m.%Y}"
                except Exception:
                    date_str = "—"
            else:
                date_str = "—"

        lines: list[str] = []
        # Use the № symbol in booking header per UX request
        lines.append(f"<b>{__("booking_label")} №{booking_id}</b>")
        lines.append(f"{__("service_label")}: <b>{service_name}</b>")
        lines.append(f"{__("master_label")}: {master_name}")
        lines.append(f"{__("date_label")}: <b>{date_str}</b>")
        try:
            lines.append(f"{__("slot_duration_label")}: {int(duration_minutes)} {__("minutes_short")}")
        except Exception:
            pass
        lines.append(f"{__("amount_label")}: {human_price}")

        if str(role).lower() == "master":
            try:
                st_val = _get('status', None)
                if st_val:
                    lines.append(f"{__("status_label")}: {st_val}")
                client_display = _get('client_name', None)
                client_phone = _get('client_phone', None)
                client_tg = _get('client_telegram_id', None) or _get('client_tid', None) or _get('client_tg_id', None)
                client_un = _get('client_username', None)
                if client_display:
                    if client_un:
                        lines.insert(1, f"{__("client_label")}: {client_display} (@{client_un})")
                    elif client_tg:
                        try:
                            lines.insert(1, f"{__("client_label")}: <a href='tg://user?id={int(client_tg)}'>{client_display}</a>")
                        except Exception:
                            lines.insert(1, f"{__("client_label")}: {client_display}")
                    else:
                        lines.insert(1, f"{__("client_label")}: {client_display}")
                if client_phone:
                    lines.insert(2, f"{__("phone_label")}: {client_phone}")
            except Exception:
                pass
        return "\n".join(lines)
    except Exception:
        try:
            bid = None
            if isinstance(data, dict):
                bid = data.get("booking_id")
            else:
                bid = getattr(data, "booking_id", None)
            return str(bid if bid is not None else "—")
        except Exception:
            return "—"


async def get_user_locale(telegram_id: int) -> str:
    """Get user locale via UserRepo; fallback to default on error.

    Note: DB access is delegated to the repository layer.
    """
    try:
        from bot.app.services.client_services import UserRepo as _UserRepo
        locale = await _UserRepo.get_locale_by_telegram_id(int(telegram_id))
        if locale:
            logger.debug("User locale for %s: %s", telegram_id, locale)
            return str(locale)
    except Exception as e:
        logger.debug("shared_services.get_user_locale: repo lookup failed for %s: %s", telegram_id, e)
    return _default_language()


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
        use_lang = kwargs.pop("lang", None) or _default_language()
        translated = _tr_raw(key, lang=use_lang, **kwargs)
        logger.debug("Перевод для пользователя %s: key=%s, result=%s", user_id, key, translated)
        return translated
    except Exception as e:
        logger.error("Ошибка перевода: user_id=%s, key=%s, error=%s", user_id, key, e)
        return key


# (Service & master cache helpers now reside in their role modules.)


__all__ = [
    "is_telegram_payments_enabled",
    "toggle_telegram_payments",
    "get_telegram_provider_token",
    "is_online_payments_available",
    "format_money_cents",
    "status_to_emoji",
    "get_user_locale",
    "translate_for_user",
    "default_language",
    "get_env_int",
    "get_admin_ids",
    "get_master_ids",
    # Note: get_service_name moved to bot.app.services.admin_services
    "format_booking_list_item",
    "format_booking_details_text",
    "format_slot_label",
    "BookingInfo",
    "booking_info_from_mapping",
]

# ---------------- New shared helpers (i18n, profiles, notifications) ---------------- #
from typing import Optional, Mapping
from aiogram.types import Message, CallbackQuery
# (Repository classes are intentionally not duplicated here.)









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


# ---------------- Service & Settings helpers ----------------
# ServiceRepo and SettingsRepo implementations live in
# `bot.app.services.admin_services`. Callers should import service-related
# helpers (services_cache, get_service_name, invalidate_services_cache,
# update_setting, get_setting, get_hold_minutes) from that module.

def safe_user_id(obj: Message | CallbackQuery | Any) -> int:
    """Return Telegram user id from Message/CallbackQuery or 0 if not available."""
    # Assume aiogram always provides `from_user` for user-originated updates.
    # Let exceptions surface so issues are visible in logs instead of silently
    # returning 0 and masking incorrect behavior.
    return int(obj.from_user.id)


def _safe_call(name: str, *args, **kwargs) -> None:
    """Call cfg.<name>(*args, **kwargs) if callable, swallow errors.

    Useful for optional cache invalidations/hooks that may not exist in some deployments.
    """
    try:
        cfg_mod = import_module("bot.config")
        fn = getattr(cfg_mod, name, None)
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

# (Stats rendering & cache helpers were relocated; import from admin/master services.)
#   from bot.app.services.admin_services import services_cache, get_service_name, update_setting
#   from bot.app.services.master_services import masters_cache, invalidate_masters_cache


async def safe_get_locale(user_id: int | None, default: str | None = None) -> str:
    """Get locale for user, falling back to dynamic default language.

    If user has no stored locale, fallback chain:
      1) explicit `default` argument if provided
      2) environment-driven `_default_language()` (DEFAULT_LANGUAGE/LANGUAGE)
    """
    fallback = default or _default_language()
    if not user_id:
        return fallback
    try:
        from bot.app.services.client_services import UserRepo as _UserRepo
        loc = await _UserRepo.get_locale_by_telegram_id(int(user_id))
        return str(loc) if loc else fallback
    except Exception:
        return fallback


def tr(key: str, *, lang: str | None = None, user_id: int | None = None, **fmt: Any) -> str:
    """Unified translation helper delegating to translations.tr()."""
    try:
        use_lang = lang or _default_language()
        return _tr_raw(key, lang=use_lang, **fmt)
    except Exception:
        return key


# ---------------- Minimal shared UI primitives moved to UI modules ---------------- #
# The keyboard builders are UI-only and now live under
# `bot.app.telegram.*_keyboards` (for role-specific factories) and
# `bot.app.telegram.client.client_keyboards` for simple/common helpers.


# UI rendering moved to client client_keyboards.render_bookings_list_page


def tz_convert(dt: datetime, tz: ZoneInfo | str | None = None) -> datetime:
    """Convert a datetime to a target timezone (defaults to LOCAL_TZ)."""
    try:
        if tz is None:
            tz = LOCAL_TZ
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


# Booking- and master-related helpers (service name resolution, bookings list,
# master profile formatting) have been moved into their respective modules to
# avoid duplication and import cycles:
# - Booking helpers -> bot.app.services.client_services
# - Master profile helpers -> bot.app.services.master_services
#
# The original implementations were removed from shared_services. Callers
# should import the canonical functions/classes from the modules listed above.



# BookingDetails dataclass and build_booking_details() were moved to
# `bot.app.services.client_services`. Import the canonical implementations
# from that module when booking-specific logic is required.


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
        # Prefer to catch only Telegram API errors here. Other exceptions may
        # indicate bugs (formatting, logic) and should surface to the
        # caller/routing-level error handlers so they can be observed and fixed.
        try:
            if TelegramAPIError is not None and isinstance(e, TelegramAPIError):
                logger.warning("_safe_send TelegramAPIError for %s: %s", chat_id, e)
                return False
        except Exception:
            # If checking isinstance itself fails for some reason, fall back
            # to the generic logging below and re-raise.
            logger.exception("_safe_send: error while handling exception for %s", chat_id)
            raise

        # Unexpected exception: log full traceback and re-raise so bugs are visible.
        logger.exception("_safe_send unexpected error for %s: %s", chat_id, e)
        raise


# build_bookings_dashboard_kb intentionally removed: import from client_keyboards


# Removed thin proxy format_booking_card_text; callers should import
# `format_booking_details_text` from `bot.app.services.client_services`.


# build_booking_card_kb deprecated shim removed; import from client_keyboards


# `render_booking_card` removed — handlers should call the builder + formatter + kb
# explicitly (build_booking_details -> format_booking_details_text -> build_booking_card_kb).