from __future__ import annotations
import logging
import os
import re
from importlib import import_module
from typing import Any, Dict, Iterable, Sequence, Mapping, TYPE_CHECKING
from dataclasses import dataclass


from sqlalchemy import select, or_
from sqlalchemy import func, delete
from sqlalchemy.exc import SQLAlchemyError

from bot.app.core.db import get_session
from bot.app.core.constants import (
    ADMIN_IDS_LIST,
    DEFAULT_CURRENCY,
    DEFAULT_LANGUAGE,
    DEFAULT_LOCAL_TIMEZONE,
    DEFAULT_SERVICE_FALLBACK_DURATION,
    MASTER_IDS_LIST,
    SETTINGS_CACHE_TTL_SECONDS,
    TELEGRAM_PROVIDER_TOKEN,
)
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
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Default online payment discount percent (used when SettingsRepo lookup fails)
ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT = 5


async def resolve_online_payment_discount_percent() -> int:
    """Return the configured online payment discount percent (0-100).

    Falls back to `ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT` only when settings
    lookup fails; otherwise clamps invalid values to the 0-100 range.
    """
    pct: int | None
    try:
        from bot.app.services.admin_services import SettingsRepo

        pct = await SettingsRepo.get_online_payment_discount_percent()
    except Exception:
        pct = ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT

    try:
        pct_int = int(pct or 0)
    except Exception:
        pct_int = ONLINE_PAYMENT_DISCOUNT_PERCENT_DEFAULT

    if pct_int < 0:
        return 0
    if pct_int > 100:
        return 100
    return pct_int


def apply_online_payment_discount(
    price_cents: int | Decimal | None, discount_pct: int
) -> tuple[int, int]:
    """Apply an online-payment discount to a cents amount.

    Returns a tuple of (discounted_cents, savings_cents). Rounds half up to
    mirror Telegram invoice rounding.
    """
    try:
        base = Decimal(int(price_cents or 0))
    except Exception:
        base = Decimal(0)

    pct = discount_pct if isinstance(discount_pct, int) else 0
    pct = max(0, min(100, pct))

    if base <= 0 or pct == 0:
        return int(base), 0

    multiplier = Decimal(100 - pct) / Decimal(100)
    discounted = (base * multiplier).to_integral_value(rounding=ROUND_HALF_UP)
    discounted_int = int(discounted)
    savings = max(0, int(base) - discounted_int)
    return discounted_int, savings


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


def _parse_setting_value(raw: Any) -> Any:
    """Parse a Setting.value-like string into bool/int/float when reasonable."""
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


def _coerce_int(value: int | str | None, default: int) -> int:
    """Coerce a possibly-None value into int, returning `default` on failure.

    This helper centralizes the small UI/service need to accept either an
    integer or a string that should parse to int. It intentionally does not
    perform environment lookups; for env-backed integers use `get_env_int`.
    """
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _default_language() -> str:
    return DEFAULT_LANGUAGE


def _default_currency() -> str:
    # UI-only synchronous fallback for places that cannot access DB.
    # Prefer `DEFAULT_CURRENCY` env var when present, then legacy `CURRENCY`.
    return DEFAULT_CURRENCY


async def get_global_currency() -> str:
    """Async helper to resolve the canonical runtime currency.

    Tries DB-backed `SettingsRepo.get_currency()` and falls back to
    the environment-only `_default_currency()` when the repo is
    unavailable. Call renderers should prefer this helper when
    they can await; otherwise use `_default_currency()` for
    synchronous code paths.
    """
    try:
        from bot.app.services.admin_services import SettingsRepo

        try:
            val = await SettingsRepo.get_currency()
            return val or _default_currency()
        except Exception:
            return _default_currency()
    except Exception:
        return _default_currency()


def normalize_currency(code: str | None) -> str | None:
    """Normalize and validate an ISO 4217 currency code.

    Returns the uppercased 3-letter alphabetic code when it looks like a
    valid ISO 4217 code (three letters). This intentionally does NOT
    whitelist specific currencies so admins can enter any 3-letter code.

    Note: acceptance here is syntactic only; downstream systems (payment
    providers) may not support all codes. Consider integrating a
    comprehensive currency library (e.g. `pycountry`) if you need a
    canonical list.
    """
    try:
        if not code:
            return None
        c = str(code).strip().upper()
        if not c:
            return None
        # Accept any 3-letter A-Z code (ISO 4217 format). Do not maintain
        # a hardcoded whitelist here so deployments worldwide work out of
        # the box without code changes.
        if re.fullmatch(r"[A-Z]{3}", c):
            return c
        return None
    except Exception:
        return None


def normalize_error_code(val: str | Exception | None, default: str) -> str:
    """Return a safe, frontend-friendly error code.

    This mirrors previous local implementations across the codebase and
    centralizes normalization so callers don't accidentally leak
    exception messages or unusual characters to the client. The result
    is lowercased, trimmed, restricted to alnum/_/- and limited to 64
    chars. On any unexpected input the provided `default` is returned.
    """
    if val is None:
        return default
    try:
        code = str(val).strip().lower()
    except Exception:
        return default
    if not code:
        return default
    if not all(ch.isalnum() or ch in {"_", "-"} for ch in code):
        return default
    return code[:64]


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


# ---------------- Pagination utility ----------------
def compute_pagination(
    total: int, page: int | None, page_size: int | None
) -> tuple[int, int, int, int | None]:
    """Compute safe pagination values.

    Returns (page, total_pages, offset, limit).
    - total: total item count (negative treated as 0)
    - page: requested page (None/invalid -> 1)
    - page_size: items per page (None/<=0 -> no limit)
    """
    try:
        t = int(total or 0)
    except Exception:
        t = 0
    if t < 0:
        t = 0
    try:
        ps = None if page_size is None else int(page_size)
    except Exception:
        ps = None
    if ps is not None and ps <= 0:
        ps = None
    if ps is None:
        total_pages = 1
        p = 1
        offset = 0
        limit = None
        return p, total_pages, offset, limit
    try:
        total_pages = max(1, (t + ps - 1) // ps)
    except Exception:
        total_pages = 1
    try:
        req = int(page or 1)
    except Exception:
        req = 1
    p = max(1, min(req, total_pages))
    offset = (p - 1) * ps
    return p, total_pages, offset, ps


def get_cancel_keywords(lang: str | None = None) -> set[str]:
    """Return a set of localized cancel keywords for the given language.

    The translation key `cancel_keywords` may be either a list of strings
    or a single string; this helper normalizes both variants and returns
    a lowercased set for robust comparison with user input.
    """
    try:
        lang = lang or _default_language()
        raw = _tr_raw("cancel_keywords", lang=lang)
        kws: set[str] = set()
        if isinstance(raw, list):
            for kw in raw:
                try:
                    if kw:
                        kws.add(str(kw).strip().lower())
                except Exception:
                    continue
        elif raw:
            try:
                kws.add(str(raw).strip().lower())
            except Exception:
                pass
        if not kws:
            kws = {"cancel"}
        return kws
    except Exception:
        return {"cancel"}


def is_cancel_text(text: str | None, lang: str | None = None) -> bool:
    """Return True when the provided text should be interpreted as a cancel action.

    This compares a normalized lowercased input against the localized cancel
    keywords set returned by `get_cancel_keywords`.
    """
    try:
        if not text:
            return False
        normalized = str(text).strip().lower()
        kws = get_cancel_keywords(lang=lang)
        return normalized in kws
    except Exception:
        return False


def format_user_display_name(
    username: str | None, first_name: str | None, last_name: str | None
) -> str | None:
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
        try:
            webapp_title = await SettingsRepo.get_setting("webapp_title", None)
        except Exception:
            webapp_title = None
    except Exception:
        # SettingsRepo not available or import failed; fall back to env below
        phone = instagram = address = None

    # Resolve from env if DB value is missing or empty
    phone_val = (
        str(phone).strip() if phone is not None and str(phone).strip() else None
    ) or _env_with_fallback("CONTACT_PHONE", "BUSINESS_PHONE", "+380671234567")
    instagram_val = (
        str(instagram).strip() if instagram is not None and str(instagram).strip() else None
    ) or _env_with_fallback(
        "CONTACT_INSTAGRAM", "BUSINESS_INSTAGRAM", "https://instagram.com/salon_name"
    )
    address_val = (
        str(address).strip() if address is not None and str(address).strip() else None
    ) or _env_with_fallback("CONTACT_ADDRESS", "BUSINESS_ADDRESS", "м. Київ, вул. Хрещатик, 1")
    # WebApp title: admin-configured salon title shown in contacts header
    title_val = (
        str(webapp_title).strip()
        if ("webapp_title" in locals() and webapp_title is not None and str(webapp_title).strip())
        else None
    ) or _env_with_fallback("WEBAPP_TITLE", "BUSINESS_NAME", "Telegram Mini App • Beauty")

    return {
        "phone": phone_val,
        "instagram": instagram_val,
        "address": address_val,
        "title": title_val,
    }


def get_admin_ids() -> list[int]:
    return ADMIN_IDS_LIST


def get_master_ids() -> list[int]:
    return MASTER_IDS_LIST


def _resolve_local_tz() -> ZoneInfo | None:
    tz_name = DEFAULT_LOCAL_TIMEZONE
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return None


LOCAL_TZ = _resolve_local_tz()


# Эмодзи для статусов
STATUS_EMOJI: Dict[str, str] = {
    "paid": "💳",
    "confirmed": "💵",
    "pending_payment": "⏳",
    "reserved": "🟡",
    "expired": "⌛",
    "cancelled": "❌",
    "done": "✅",
    "no_show": "👻",
}


async def render_booking_item_for_api(
    booking: Any, user_telegram_id: int | None = None, lang: str | None = None
) -> dict:
    """Return a dict with API-friendly booking fields.

    This centralizes status label/emoji, price formatting and permission
    checks so API endpoints can be thin and consistent.
    """
    out: dict = {}
    try:
        from bot.app.services.shared_services import STATUS_EMOJI as _SE  # local alias
        from bot.app.services.shared_services import LOCAL_TZ as _LT  # noqa: F401
    except Exception:
        _SE = STATUS_EMOJI

    try:
        # Status label (localized) and emoji
        try:
            from bot.app.telegram.common.status import get_status_label

            status_label = await get_status_label(getattr(booking, "status", None), lang=lang)
        except Exception:
            status_label = str(getattr(booking, "status", ""))
        try:
            status_emoji = status_to_emoji(getattr(booking, "status", None))
        except Exception:
            status_emoji = ""

        # Price snapshot — keep both original and final to drive discount UI in mini app
        try:
            original_price_val = getattr(booking, "original_price_cents", None)
            original_price_val = int(original_price_val) if original_price_val is not None else None
        except Exception:
            original_price_val = None
        try:
            final_price_val = getattr(booking, "final_price_cents", None)
            final_price_val = int(final_price_val) if final_price_val is not None else None
        except Exception:
            final_price_val = None
        try:
            discount_amount_val = getattr(booking, "discount_amount_cents", None)
            discount_amount_val = (
                int(discount_amount_val) if discount_amount_val is not None else None
            )
        except Exception:
            discount_amount_val = None

        # Derive missing pieces so the API always returns a full breakdown
        if (
            final_price_val is None
            and original_price_val is not None
            and discount_amount_val is not None
        ):
            final_price_val = original_price_val - discount_amount_val
        if (
            original_price_val is None
            and final_price_val is not None
            and discount_amount_val is not None
        ):
            original_price_val = final_price_val + discount_amount_val
        if (
            discount_amount_val is None
            and original_price_val is not None
            and final_price_val is not None
        ):
            delta = original_price_val - final_price_val
            if delta > 0:
                discount_amount_val = delta

        # Backward compatible single price value still used by legacy UI bits
        price_val = final_price_val or original_price_val
        try:
            currency_val = getattr(booking, "currency", None)
        except Exception:
            currency_val = None
        try:
            price_fmt = (
                format_money_cents(price_val, currency_val) if price_val is not None else None
            )
            original_price_fmt = (
                format_money_cents(original_price_val, currency_val)
                if original_price_val is not None
                else None
            )
            final_price_fmt = (
                format_money_cents(final_price_val, currency_val)
                if final_price_val is not None
                else None
            )
            discount_amount_fmt = (
                format_money_cents(discount_amount_val, currency_val)
                if discount_amount_val is not None
                else None
            )
        except Exception:
            price_fmt = None
            original_price_fmt = None
            final_price_fmt = None
            discount_amount_fmt = None

        # Payment method inference
        try:
            pm = None
            if (
                getattr(booking, "payment_provider", None)
                or getattr(booking, "paid_at", None) is not None
                or getattr(booking, "payment_id", None)
            ):
                pm = "online"
            else:
                pm = "cash"
        except Exception:
            pm = None

        # Permissions: delegate to client_services.calculate_booking_permissions
        try:
            from bot.app.services import client_services as _client_services

            # Try to read lock windows from SettingsRepo if available (best-effort)
            try:
                from bot.app.services.admin_services import SettingsRepo

                try:
                    lock_r = await SettingsRepo.get_client_reschedule_lock_minutes()
                except Exception:
                    lock_r = None
                try:
                    lock_c = await SettingsRepo.get_client_cancel_lock_minutes()
                except Exception:
                    lock_c = None
            except Exception:
                lock_r = lock_c = None

            (
                can_cancel_calc,
                can_reschedule_calc,
            ) = await _client_services.calculate_booking_permissions(
                booking,
                lock_r_minutes=lock_r,
                lock_c_minutes=lock_c,
                settings=None,
            )
            can_cancel = bool(can_cancel_calc)
            can_reschedule = bool(can_reschedule_calc)
            try:
                # Authoritative reschedule check (only if we have a caller telegram id)
                if user_telegram_id is not None:
                    can_reschedule_primary, _ = await _client_services.can_client_reschedule(
                        int(getattr(booking, "id", 0) or 0), int(user_telegram_id)
                    )
                    if can_reschedule_primary:
                        can_reschedule = True
            except Exception:
                pass
        except Exception:
            can_cancel = False
            can_reschedule = False

        # safe isoformat extraction
        starts_obj = getattr(booking, "starts_at", None)
        ends_obj = getattr(booking, "ends_at", None)
        starts_iso = starts_obj.isoformat() if starts_obj is not None else None
        ends_iso = ends_obj.isoformat() if ends_obj is not None else None

        # Duration: prefer explicit booking.duration_minutes, otherwise derive from starts/ends
        try:
            dur_val = getattr(booking, "duration_minutes", None)
            if dur_val is None and starts_obj is not None and ends_obj is not None:
                try:
                    delta = ends_obj - starts_obj
                    dur_val = int(delta.total_seconds() // 60)
                except Exception:
                    dur_val = None
            duration_minutes_val = int(dur_val) if dur_val is not None else None
        except Exception:
            duration_minutes_val = None

        out = {
            "status": str(getattr(booking, "status", "")),
            "status_label": status_label or None,
            "status_emoji": status_emoji or None,
            "price_cents": price_val,
            "price_formatted": price_fmt,
            "original_price_cents": original_price_val,
            "final_price_cents": final_price_val,
            "discount_amount_cents": discount_amount_val,
            "original_price_formatted": original_price_fmt,
            "final_price_formatted": final_price_fmt,
            "discount_amount_formatted": discount_amount_fmt,
            "currency": currency_val or None,
            "payment_method": pm,
            "can_cancel": bool(can_cancel),
            "can_reschedule": bool(can_reschedule),
            "starts_at": starts_iso,
            "ends_at": ends_iso,
            "duration_minutes": duration_minutes_val,
        }
    except Exception:
        # Best-effort fallback: return minimal fields
        starts_obj = getattr(booking, "starts_at", None)
        ends_obj = getattr(booking, "ends_at", None)
        starts_iso = starts_obj.isoformat() if starts_obj is not None else None
        ends_iso = ends_obj.isoformat() if ends_obj is not None else None
        out = {
            "status": str(getattr(booking, "status", "")),
            "status_label": getattr(booking, "status", None),
            "status_emoji": None,
            "price_cents": None,
            "price_formatted": None,
            "currency": None,
            "payment_method": None,
            "can_cancel": False,
            "can_reschedule": False,
            "starts_at": starts_iso,
            "ends_at": ends_iso,
            "duration_minutes": None,
        }
    return out


# --- Payments/provider runtime cache (shared helper; used across modules) ---
_PAYMENTS_ENABLED: bool | None = None
_PROVIDER_TOKEN_CACHE: str | None = None
_PAYMENTS_LAST_CHECKED: datetime | None = None
_PROVIDER_LAST_CHECKED: datetime | None = None
_MINIAPP_ENABLED: bool | None = None
_MINIAPP_LAST_CHECKED: datetime | None = None


def _settings_cache_expired(last_checked: datetime | None) -> bool:
    """Return True when a settings cache timestamp is considered expired.

    This is a local implementation used by payment/provider helpers. It uses
    a TTL from environment (``SETTINGS_CACHE_TTL_SECONDS``)
    with a conservative default of 60 seconds.
    """
    _ttl = SETTINGS_CACHE_TTL_SECONDS
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


async def is_telegram_miniapp_enabled() -> bool:
    """Check whether Telegram MiniApp booking is enabled using a shared store.

    Priority:
      1) DB-backed runtime settings via SettingsRepo
      2) Environment fallback (TELEGRAM_MINIAPP_ENABLED)
    """
    global _MINIAPP_ENABLED, _MINIAPP_LAST_CHECKED
    try:
        from bot.app.services.admin_services import SettingsRepo, load_settings_from_db

        if _MINIAPP_ENABLED is None or _settings_cache_expired(_MINIAPP_LAST_CHECKED):
            try:
                await load_settings_from_db()
            except Exception:
                pass
            val = await SettingsRepo.get_setting("telegram_miniapp_enabled", None)
            if val is None:
                val = _env_bool("TELEGRAM_MINIAPP_ENABLED", True)
            _MINIAPP_ENABLED = bool(val)
            _MINIAPP_LAST_CHECKED = utc_now()
            logger.debug("Telegram MiniApp (shared) refresh: %s", _MINIAPP_ENABLED)
        return bool(_MINIAPP_ENABLED)
    except Exception:
        if _MINIAPP_ENABLED is None or _settings_cache_expired(_MINIAPP_LAST_CHECKED):
            _MINIAPP_ENABLED = _env_bool("TELEGRAM_MINIAPP_ENABLED", True)
            _MINIAPP_LAST_CHECKED = utc_now()
        return bool(_MINIAPP_ENABLED)


async def toggle_telegram_miniapp() -> bool:
    """Toggle Telegram MiniApp booking using shared store (DB settings), with env fallback."""
    global _MINIAPP_ENABLED, _MINIAPP_LAST_CHECKED
    try:
        new_val = not await is_telegram_miniapp_enabled()
        from bot.app.services.admin_services import SettingsRepo

        ok = await SettingsRepo.update_setting("telegram_miniapp_enabled", bool(new_val))
        if not ok:
            logger.warning("toggle_telegram_miniapp: DB persist failed; falling back to env only")
        _MINIAPP_ENABLED = bool(new_val)
        _MINIAPP_LAST_CHECKED = utc_now()
        os.environ["TELEGRAM_MINIAPP_ENABLED"] = "1" if new_val else "0"
        logger.info("Telegram MiniApp toggled (shared): %s", new_val)
        return bool(new_val)
    except Exception:
        new_val = not await is_telegram_miniapp_enabled()
        _MINIAPP_ENABLED = bool(new_val)
        _MINIAPP_LAST_CHECKED = utc_now()
        os.environ["TELEGRAM_MINIAPP_ENABLED"] = "1" if new_val else "0"
        logger.info("Telegram MiniApp toggled (env fallback): %s", new_val)
        return bool(new_val)


async def get_telegram_provider_token(force_reload: bool = False) -> str | None:
    """Return Telegram Payments provider token from shared settings with env fallback."""
    global _PROVIDER_TOKEN_CACHE, _PROVIDER_LAST_CHECKED
    try:
        if (
            not force_reload
            and _PROVIDER_TOKEN_CACHE
            and not _settings_cache_expired(_PROVIDER_LAST_CHECKED)
        ):
            return _PROVIDER_TOKEN_CACHE
        token: str | None = None
        try:
            from bot.app.services.admin_services import SettingsRepo

            token = await SettingsRepo.get_setting("telegram_provider_token", None)
        except Exception:
            token = None
        if not token:
            token = TELEGRAM_PROVIDER_TOKEN
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
    """Format money given in cents using locale-aware formatting when
    Babel is available; otherwise fall back to a simple ``{amount} {CUR}``
    representation.

    Args:
        cents: amount in minor units (cents).
        currency: ISO 4217 3-letter code (if None, resolved from env/settings).

    Returns:
        Localized money string, e.g. "$1.00" or "1,00 €" where possible.
    """
    try:
        if not currency:
            currency = _default_currency()
        # Coerce to integer cents; accept float/int/Decimal
        cents_int = 0
        try:
            if isinstance(cents, Decimal):
                cents_int = int(cents)
            elif isinstance(cents, (int, float)):
                cents_int = int(cents)
            else:
                cents_int = int(float(cents)) if cents is not None else 0
        except Exception:
            cents_int = 0

        amount = Decimal(cents_int) / Decimal(100)

        # Try to use Babel for proper locale-aware currency formatting.
        try:
            # Use dynamic import to avoid static-analysis missing-import errors
            _bn = import_module("babel.numbers")
            format_currency = getattr(_bn, "format_currency")
            try:
                _bc = import_module("babel.core")
                Locale = getattr(_bc, "Locale")
                _b = import_module("babel")
                _babel_default_locale = getattr(_b, "default_locale")
            except Exception:
                Locale = None  # type: ignore
                _babel_default_locale = None  # type: ignore

            # Resolve a best-effort locale using Babel parsing instead of manual mappings.
            locale_str = "en_US"
            try:
                lang = (
                    os.getenv("DEFAULT_LANGUAGE")
                    or os.getenv("LANGUAGE")
                    or default_language()
                    or "en"
                )
                locale_hint = (lang or "en").replace("-", "_")
                if Locale is not None:
                    locale_str = str(Locale.parse(locale_hint, sep="_"))
                else:
                    locale_str = locale_hint
            except Exception:
                try:
                    if _babel_default_locale:
                        locale_str = _babel_default_locale() or locale_str
                except Exception:
                    pass

            try:
                formatted = format_currency(amount, str(currency), locale=locale_str)
                logger.debug("Formatted money (Babel): %s %s -> %s", cents_int, currency, formatted)
                return formatted
            except Exception:
                # If Babel fails for this locale/currency, fallback below
                pass
        except Exception:
            # Babel not installed or import failed; fall back
            pass

        # Fallback: simple formatted string with dot and currency suffix
        value = float(amount)
        formatted = f"{value:.2f} {currency}"
        logger.debug("Formatted money (fallback): %s -> %s", cents_int, formatted)
        return formatted
    except Exception as e:
        logger.exception("format_money_cents failed: %s", e)
        try:
            return f"0.00 {currency or 'UNK'}"
        except Exception:
            return "0.00"


def format_minutes_short(minutes: int, lang: str | None = None) -> str:
    """Return a compact, localized minutes label (uses hours when divisible).

    Examples: 120 -> "2 h" (with localized hours_short), 90 -> "90 min".
    """
    try:
        mins = int(minutes)
    except Exception:
        return str(minutes)

    try:
        l = lang or default_language()
    except Exception:
        l = lang or default_language()

    try:
        hours_label = _tr_raw("hours_short", lang=l) or "h"
    except Exception:
        hours_label = "h"
    try:
        minutes_label = _tr_raw("minutes_short", lang=l) or "min"
    except Exception:
        minutes_label = "min"

    if mins >= 120 and mins % 60 == 0:
        return f"{mins // 60} {hours_label}"
    return f"{mins} {minutes_label}"


# ---------------- Time utilities (shared) ---------------- #
def _parse_hm_to_minutes(text: str | int) -> int:
    """Parse an HH:MM-like input into minutes since midnight.

    Accepted forms:
      - "09:30", "9:30" -> 570
      - "0930", "930"   -> 570
      - "9"              -> 540 (interpreted as hours)
      - integer minutes (returned as-is when in valid range)

    On invalid input a ValueError is raised.
    """
    if text is None:
        raise ValueError("empty time")

    # Integers are treated as minutes
    if isinstance(text, int):
        minutes = int(text)
        if minutes < 0 or minutes >= 24 * 60:
            raise ValueError(f"minutes out of range: {minutes}")
        return minutes

    s = str(text).strip()
    if not s:
        raise ValueError("empty time")

    # HH:MM or H:MM
    m = re.fullmatch(r"(\d{1,2}):(\d{1,2})", s)
    if m:
        h = int(m.group(1))
        mm = int(m.group(2))
        if not (0 <= h < 24 and 0 <= mm < 60):
            raise ValueError(f"time out of range: {s}")
        return h * 60 + mm

    # HHMM or HMM numeric forms like 930 or 0930
    if re.fullmatch(r"\d{3,4}", s):
        s2 = s.zfill(4)
        h = int(s2[:2])
        mm = int(s2[2:])
        if not (0 <= h < 24 and 0 <= mm < 60):
            raise ValueError(f"time out of range: {s}")
        return h * 60 + mm

    # Single hour number like "9" -> 9:00
    if re.fullmatch(r"\d{1,2}", s):
        h = int(s)
        if not (0 <= h < 24):
            raise ValueError(f"hour out of range: {s}")
        return h * 60

    raise ValueError(f"unrecognized time format: {text}")


def _minutes_to_hm(minutes: int) -> str:
    """Format minutes-since-midnight as "HH:MM".

    Raises ValueError when `minutes` is out of 0..(24*60-1).
    """
    try:
        m = int(minutes)
    except Exception:
        raise ValueError(f"invalid minutes value: {minutes}")
    if m < 0 or m >= 24 * 60:
        raise ValueError(f"minutes out of range: {m}")
    h = m // 60
    mm = m % 60
    return f"{h:02d}:{mm:02d}"


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
        # Prefer the `UTC` constant when available (PEP 495 friendly).
        return datetime.now(UTC)
    except Exception:
        # Fallback to the well-known timezone.utc instance.
        from datetime import timezone as _tz

        return datetime.now(_tz.utc)


def local_now() -> datetime:
    """Return current time in the configured local timezone (aware).

    Falls back to UTC when local timezone resolution fails.
    """
    try:
        # Derive local time from a single source of truth (UTC) so all parts
        # of the application use consistent anchor points for arithmetic.
        return utc_now().astimezone(get_local_tz())
    except Exception:
        try:
            return utc_now()
        except Exception:
            # Last-resort: naive now in UTC
            return datetime.utcnow().replace(tzinfo=UTC)


def format_slot_label(
    slot: datetime | None, fmt: str = "%H:%M", tz: ZoneInfo | str | None = None
) -> str:
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


async def get_service_duration(
    session, service_id: str | None, master_id: int | None = None
) -> int:
    """Resolve the effective duration (minutes) for a service+master pair.

        Resolution order:
            1. If `master_id` provided, check `master_services.duration_minutes`.
            2. Check `services.duration_minutes` (canonical service-level value).
            3. Fallback to `SettingsRepo.get_slot_duration()` or
                 `DEFAULT_SERVICE_FALLBACK_DURATION`.

    This helper is async and takes a SQLAlchemy `session` so callers can
    reuse their existing transaction/session and avoid extra roundtrips.
    """
    try:
        # Lazy imports to avoid circular dependencies at module-import time
        from bot.app.domain.models import MasterService, Service
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

        # 3) legacy fallback removed: durations are canonical on `services` table

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
        # Use env/default currency SSoT
        "currency": _default_currency(),
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
        client_name = (
            f"{client_name} (@{client_username})" if client_name else f"@{client_username}"
        )
    st = dt = ""
    if starts_at:
        try:
            lt = get_local_tz()
            st = format_slot_label(starts_at, fmt="%H:%M", tz=lt)
            dt = format_date(starts_at, "%d.%m", tz=lt)
        except Exception:
            st = dt = ""
    price_cents = data.get("final_price_cents") or data.get("original_price_cents")
    # Use env-configured default currency as the single source of truth.
    # Ignore any per-service or per-booking stored currency values.
    currency = _default_currency()
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
    # Prefer role-specific formatters defined in service modules. Import lazily
    # to avoid circular imports at module import time. Fall back to a simple
    # client-style formatter when the role module isn't importable (tests/etc.).
    try:
        from bot.app.services.client_services import format_client_booking_row as _client_fmt
    except Exception:
        _client_fmt = lambda f: f.get("service_name", "")

    try:
        from bot.app.services.master_services import format_master_booking_row as _master_fmt
    except Exception:
        _master_fmt = _client_fmt

    try:
        # Import module then getattr to avoid static import symbol warnings
        import bot.app.services.admin_services as _admin_mod

        _admin_fmt = getattr(_admin_mod, "format_admin_booking_row", _client_fmt)
    except Exception:
        _admin_fmt = _client_fmt

    formatter = {
        "master": _master_fmt,
        "admin": _admin_fmt,
        "client": _client_fmt,
    }
    formatter_fn = formatter.get(str(role).lower(), _client_fmt)
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
    # Currency is intentionally not stored in the DTO; use global setting
    # resolved at render time via `get_global_currency()` or `_default_currency()`.
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
        # Do not trust per-booking or per-service stored currency values.
        # Renderers should use the global currency instead.
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
        return booking_info_from_mapping(
            {
                "id": getattr(row, "id", None),
                "master_id": getattr(row, "master_id", None),
                "master_name": getattr(row, "master_name", None),
                "service_id": getattr(row, "service_id", None),
                "service_name": getattr(row, "service_name", None),
                "status": getattr(row, "status", None),
                "starts_at": getattr(row, "starts_at", None),
                "original_price_cents": getattr(row, "original_price_cents", None),
                "final_price_cents": getattr(row, "final_price_cents", None),
                # Do not materialize currency on the DTO; renderers use global SSoT.
                "client_name": getattr(row, "client_name", None),
                "client_username": getattr(row, "client_username", None),
                "client_id": getattr(row, "client_id", None) or getattr(row, "user_id", None),
            }
        )
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


def format_booking_details_text(
    data: dict | Any, lang: str | None = None, role: str = "client"
) -> str:
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
        currency = _get("currency") or _default_currency()
        service_name = _get("service_name", None)
        master_name = _get("master_name", None)
        status_raw = _get("status", None)
        paid_at = _get("paid_at", None)
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

        time_str: str | None = None
        if not date_str:
            if starts_at:
                try:
                    dt_local = starts_at.astimezone(get_local_tz())
                    date_str = f"{dt_local:%d.%m.%Y}"
                    time_str = f"{dt_local:%H:%M}"
                except Exception:
                    date_str = "—"
                    time_str = None
            else:
                date_str = "—"

        lines: list[str] = []
        # Use the № symbol in booking header per UX request
        lines.append(f"<b>{__("booking_label")} №{booking_id}</b>")
        lines.append(f"{__("service_label")}: <b>{service_name}</b>")
        lines.append(f"{__("master_label")}: {master_name}")
        # Include time in header when available so client sees booking time immediately
        if time_str:
            lines.append(f"{__("date_label")}: <b>{date_str} {time_str}</b>")
        else:
            lines.append(f"{__("date_label")}: <b>{date_str}</b>")
        try:
            lines.append(
                f"{__("slot_duration_label")}: {int(duration_minutes)} {__("minutes_short")}"
            )
        except Exception:
            pass
        status_str = str(status_raw).lower() if status_raw is not None else ""
        amount_label = (
            __("amount_paid_label") if (paid_at or status_str == "paid") else __("amount_label")
        )
        lines.append(f"{amount_label}: {human_price}")

        if str(role).lower() == "master":
            try:
                st_val = _get("status", None)
                if st_val:
                    lines.append(f"{__("status_label")}: {st_val}")
                client_display = _get("client_name", None)
                client_phone = _get("client_phone", None)
                client_tg = (
                    _get("client_telegram_id", None)
                    or _get("client_tid", None)
                    or _get("client_tg_id", None)
                )
                client_un = _get("client_username", None)
                if client_display:
                    if client_un:
                        lines.insert(1, f"{__("client_label")}: {client_display} (@{client_un})")
                    elif client_tg:
                        try:
                            lines.insert(
                                1,
                                f"{__("client_label")}: <a href='tg://user?id={int(client_tg)}'>{client_display}</a>",
                            )
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
        logger.debug(
            "shared_services.get_user_locale: repo lookup failed for %s: %s", telegram_id, e
        )
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


__all__ = [
    "is_telegram_payments_enabled",
    "toggle_telegram_payments",
    "is_telegram_miniapp_enabled",
    "toggle_telegram_miniapp",
    "get_telegram_provider_token",
    "is_online_payments_available",
    "resolve_online_payment_discount_percent",
    "apply_online_payment_discount",
    "format_money_cents",
    "status_to_emoji",
    "get_user_locale",
    "translate_for_user",
    "default_language",
    "get_env_int",
    "get_admin_ids",
    "get_master_ids",
    "format_booking_list_item",
    "format_booking_details_text",
    "format_slot_label",
    "BookingInfo",
    "booking_info_from_mapping",
    "get_global_currency",
]

# ---------------- New shared helpers (i18n, profiles, notifications) ---------------- #
from typing import Optional, Mapping
from aiogram.types import Message, CallbackQuery

# Provide type-only imports for optional third-party libs to satisfy Pylance
if TYPE_CHECKING:
    try:
        from babel.numbers import format_currency  # type: ignore
        from babel.core import Locale  # type: ignore
        from babel import default_locale as _babel_default_locale  # type: ignore
    except Exception:
        pass
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
    # Prefer a defensive extraction to avoid optional-member access warnings
    try:
        fu = getattr(obj, "from_user", None)
        if fu is None:
            return 0
        uid = getattr(fu, "id", None)
        return int(uid or 0)
    except Exception:
        return 0


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


async def _safe_send(
    bot: Bot, chat_id: int | str, text: str, reply_markup: Any = None, **kwargs: Any
) -> bool:
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
