from __future__ import annotations
import logging
from typing import Any, Optional

import bot.config as cfg
from bot.app.domain.models import BookingStatus
from bot.app.services.master_services import ensure_booking_owner as _svc_ensure_booking_owner

logger = logging.getLogger(__name__)

__all__ = [
    "BookingStatus",
    "status_label_map",
    "get_status_label",
    "ensure_booking_owner",
]


_ADMIN_TEXT = getattr(cfg, "ADMIN_TEXT", {})
status_label_map: dict[Any, str] = {
    getattr(BookingStatus, "PAID", object()): _ADMIN_TEXT.get("status_paid", "Оплачено"),
    getattr(BookingStatus, "PENDING_PAYMENT", object()): _ADMIN_TEXT.get("status_pending_payment", "Очікує оплати"),
    getattr(BookingStatus, "RESERVED", object()): _ADMIN_TEXT.get("status_reserved", "Зарезервовано"),
    getattr(BookingStatus, "CONFIRMED", object()): _ADMIN_TEXT.get("status_confirmed", "Підтверджено"),
    getattr(BookingStatus, "AWAITING_CASH", object()): _ADMIN_TEXT.get("status_awaiting_cash", "Очікує готівку"),
    getattr(BookingStatus, "CANCELLED", object()): _ADMIN_TEXT.get("status_cancelled", "Скасовано"),
    getattr(BookingStatus, "DONE", object()): _ADMIN_TEXT.get("status_done", "Завершено"),
    getattr(BookingStatus, "NO_SHOW", object()): _ADMIN_TEXT.get("status_no_show", "Не з'явився"),
    getattr(BookingStatus, "ACTIVE", object()): _ADMIN_TEXT.get("status_active", "Активно"),
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