from __future__ import annotations

import os

# Pagination
DEFAULT_PAGE_SIZE: int = int(os.getenv("PAGINATION_PAGE_SIZE", "5"))

# Scheduling window defaults (ENV overridable)
DEFAULT_DAY_START_HOUR: int = int(os.getenv("SCHEDULE_START_HOUR", "6"))
DEFAULT_DAY_END_HOUR: int = int(os.getenv("SCHEDULE_END_HOUR", "22"))
DEFAULT_TIME_STEP_MINUTES: int = int(os.getenv("SCHEDULE_STEP_MINUTES", "30"))

# Service duration fallback (minutes)
DEFAULT_SERVICE_FALLBACK_DURATION: int = int(os.getenv("SERVICE_FALLBACK_DURATION_MIN", "60"))
DEFAULT_BUSINESS_TIMEZONE: str = os.getenv("BUSINESS_TIMEZONE", os.getenv("LOCAL_TIMEZONE", "UTC"))

__all__ = [
    "DEFAULT_PAGE_SIZE",
    "DEFAULT_DAY_START_HOUR",
    "DEFAULT_DAY_END_HOUR",
    "DEFAULT_TIME_STEP_MINUTES",
    "DEFAULT_SERVICE_FALLBACK_DURATION",
    "DEFAULT_BUSINESS_TIMEZONE",
]
