from __future__ import annotations

import os


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_int_or_none(name: str) -> int | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _env_int_list(name: str) -> list[int]:
    raw = os.getenv(name, "")
    vals: list[int] = []
    for token in raw.replace(";", ",").split(","):
        tok = token.strip()
        if not tok:
            continue
        try:
            vals.append(int(tok))
        except Exception:
            continue
    return vals


def _normalize_currency(code: str | None) -> str | None:
    if not code:
        return None
    cleaned = str(code).strip().upper()
    if len(cleaned) == 3 and cleaned.isalpha():
        return cleaned
    return None


# Pagination
DEFAULT_PAGE_SIZE: int = _env_int("PAGINATION_PAGE_SIZE", 5)

# Scheduling window defaults (ENV overridable)
DEFAULT_DAY_START_HOUR: int = _env_int("SCHEDULE_START_HOUR", 1)
DEFAULT_DAY_END_HOUR: int = _env_int("SCHEDULE_END_HOUR", 23)
DEFAULT_TIME_STEP_MINUTES: int = _env_int("SCHEDULE_STEP_MINUTES", 30)

# Timezone defaults
DEFAULT_LOCAL_TIMEZONE: str = os.getenv("LOCAL_TIMEZONE", "Europe/Kyiv")
DEFAULT_BUSINESS_TIMEZONE: str = os.getenv("BUSINESS_TIMEZONE", DEFAULT_LOCAL_TIMEZONE)

# Service duration fallback (minutes)
DEFAULT_SERVICE_FALLBACK_DURATION: int = _env_int("SERVICE_FALLBACK_DURATION_MIN", 60)

# Locale / currency
DEFAULT_LANGUAGE: str = os.getenv("DEFAULT_LANGUAGE") or os.getenv("LANGUAGE") or "uk"
DEFAULT_CURRENCY: str = _normalize_currency(os.getenv("DEFAULT_CURRENCY") or os.getenv("CURRENCY")) or "USD"

# Admin / master IDs
PRIMARY_ADMIN_TG_ID: int | None = _env_int_or_none("PRIMARY_ADMIN_TG_ID")
ADMIN_IDS_LIST: list[int] = _env_int_list("ADMIN_IDS")
MASTER_IDS_LIST: list[int] = _env_int_list("MASTER_IDS")

# Feature flags / logging
LOG_LEVEL_NAME: str = os.getenv("LOG_LEVEL", "INFO").strip().upper()
RUN_BOOTSTRAP_ENABLED: bool = _env_bool("RUN_BOOTSTRAP", False)
REQUIRE_ROW_LOCK_STRICT: bool = _env_bool("REQUIRE_ROW_LOCK", False)

# Tokens
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
TELEGRAM_PROVIDER_TOKEN: str = os.getenv("TELEGRAM_PAYMENT_PROVIDER_TOKEN", "")

# Worker / cache intervals
DEFAULT_REMINDER_LEAD_MINUTES: int = _env_int("REMINDER_LEAD_MINUTES", _env_int("DEFAULT_REMINDER_LEAD_MINUTES", 1440))
DEFAULT_REMINDER_SAME_DAY_MINUTES: int = _env_int("REMINDER_SAME_DAY_MINUTES", _env_int("SAME_DAY_LEAD_MINUTES", 60))
DEFAULT_CANCEL_LOCK_MINUTES: int = _env_int("CANCEL_LOCK_MINUTES", _env_int("CLIENT_CANCEL_LOCK_HOURS", 3) * 60)
DEFAULT_RESCHEDULE_LOCK_MINUTES: int = _env_int("RESCHEDULE_LOCK_MINUTES", _env_int("CLIENT_RESCHEDULE_LOCK_HOURS", 3) * 60)
REMINDERS_CHECK_SECONDS_RAW: str = os.getenv("REMINDERS_CHECK_SECONDS", "60")
try:
    REMINDERS_CHECK_SECONDS: int = int(REMINDERS_CHECK_SECONDS_RAW)
    REMINDERS_CHECK_SECONDS_INVALID: bool = False
except ValueError:
    REMINDERS_CHECK_SECONDS = 60
    REMINDERS_CHECK_SECONDS_INVALID = True

SETTINGS_CACHE_TTL_SECONDS: int = _env_int("SETTINGS_CACHE_TTL_SECONDS", 60)

__all__ = [
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_DAY_START_HOUR",
    "DEFAULT_DAY_END_HOUR",
    "DEFAULT_TIME_STEP_MINUTES",
    "DEFAULT_LOCAL_TIMEZONE",
    "DEFAULT_BUSINESS_TIMEZONE",
    "DEFAULT_SERVICE_FALLBACK_DURATION",
    "DEFAULT_LANGUAGE",
    "DEFAULT_CURRENCY",
    "PRIMARY_ADMIN_TG_ID",
    "ADMIN_IDS_LIST",
    "MASTER_IDS_LIST",
    "LOG_LEVEL_NAME",
    "RUN_BOOTSTRAP_ENABLED",
    "REQUIRE_ROW_LOCK_STRICT",
    "BOT_TOKEN",
    "TELEGRAM_PROVIDER_TOKEN",
    "DEFAULT_REMINDER_LEAD_MINUTES",
    "DEFAULT_REMINDER_SAME_DAY_MINUTES",
    "DEFAULT_CANCEL_LOCK_MINUTES",
    "DEFAULT_RESCHEDULE_LOCK_MINUTES",
    "REMINDERS_CHECK_SECONDS_RAW",
    "REMINDERS_CHECK_SECONDS",
    "REMINDERS_CHECK_SECONDS_INVALID",
    "SETTINGS_CACHE_TTL_SECONDS",
]
