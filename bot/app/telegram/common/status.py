from __future__ import annotations
import logging
from typing import Any

from bot.app.domain.models import BookingStatus
from bot.app.translations import tr
from bot.app.services.master_services import ensure_booking_owner as _svc_ensure_booking_owner

logger = logging.getLogger(__name__)

__all__ = [
    "BookingStatus",
    "ACTIVE_BLOCKING_STATUSES",
    "status_label_map",
    "get_status_label",
    "ensure_booking_owner",
]


status_label_map: dict[Any, str] = {
    getattr(BookingStatus, "PAID", object()): "status_paid",
    getattr(BookingStatus, "PENDING_PAYMENT", object()): "status_pending_payment",
    getattr(BookingStatus, "RESERVED", object()): "status_reserved",
    getattr(BookingStatus, "CONFIRMED", object()): "status_confirmed",
    getattr(BookingStatus, "CANCELLED", object()): "status_cancelled",
    getattr(BookingStatus, "DONE", object()): "status_done",
    getattr(BookingStatus, "NO_SHOW", object()): "status_no_show",
}


async def get_status_label(status: Any, lang: str | None = None) -> str:
    """Return a localized label for a booking status.

    Uses translation keys so bot/TMA stay in sync. Falls back to raw string.
    """
    try:
        # Direct mapping by status object
        if status in status_label_map:
            return str(tr(status_label_map[status], lang=lang))

        # Try by underlying value (e.g., Enum.value)
        sval = getattr(status, "value", None)
        if sval is not None:
            for k, v in status_label_map.items():
                if getattr(k, "value", None) == sval:
                    return str(tr(v, lang=lang))

        # Fallback to string representation
        return str(status)
    except Exception as e:
        logger.error("Ошибка при получении метки статуса %s: %s", status, e)
        return str(status)


async def ensure_booking_owner(user_id: int, booking_id: int) -> object | None:
    """Проверяет, принадлежит ли запись пользователю.

    Делегирует работу в сервисный слой (`bot.app.services.master_services.ensure_booking_owner`).
    Это позволяет инкапсулировать доступ к БД в сервисах и упростить обработку циклических импортов.
    """
    try:
        return await _svc_ensure_booking_owner(user_id, booking_id)
    except Exception as e:
        logger.error("ensure_booking_owner (forward) failed: %s", e)
        return None


ACTIVE_BLOCKING_STATUSES = {
    BookingStatus.CONFIRMED,
    BookingStatus.PAID,
    BookingStatus.RESERVED,
}
