from __future__ import annotations
import logging
from typing import Any, Optional

from bot.app.domain.models import BookingStatus
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
    getattr(BookingStatus, "PAID", object()): "Оплачено",
    getattr(BookingStatus, "PENDING_PAYMENT", object()): "Очікує оплати",
    getattr(BookingStatus, "RESERVED", object()): "Зарезервовано",
    getattr(BookingStatus, "CONFIRMED", object()): "Підтверджено",
    getattr(BookingStatus, "AWAITING_CASH", object()): "Очікує готівку",
    getattr(BookingStatus, "CANCELLED", object()): "Скасовано",
    getattr(BookingStatus, "DONE", object()): "Завершено",
    getattr(BookingStatus, "NO_SHOW", object()): "Не з'явився",
    getattr(BookingStatus, "ACTIVE", object()): "Активно",
}


async def get_status_label(status: Any) -> str:
    """Return a localized label for a booking status.

    Simplified: status labels are constant and stored in-memory in
    ``status_label_map``. Avoid network calls (Redis) for such cheap lookups.
    """
    try:
        # Direct mapping by status object
        if status in status_label_map:
            return status_label_map[status]

        # Try by underlying value (e.g., Enum.value)
        sval = getattr(status, "value", None)
        if sval is not None:
            for k, v in status_label_map.items():
                if getattr(k, "value", None) == sval:
                    return v

        # Fallback to string representation
        return str(status)
    except Exception as e:
        logger.error("Ошибка при получении метки статуса %s: %s", status, e)
        return str(status)


async def ensure_booking_owner(user_id: int, booking_id: int) -> Optional[object]:
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
    BookingStatus.ACTIVE,
}