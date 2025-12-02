from __future__ import annotations
import logging
from typing import Any, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from bot.app.services.shared_services import safe_get_locale

logger = logging.getLogger(__name__)

__all__ = ["LocaleMiddleware"]


class LocaleMiddleware(BaseMiddleware):
    """Автоматически подставляет локаль пользователя в data.

    Kept as a small, focused middleware module so callers can import it
    directly without the old duplicated Admin/Master middleware classes.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Any],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user_id = int(getattr(getattr(event, "from_user", None), "id", 0) or 0)
        if user_id:
            # Delegate to the shared safe_get_locale helper which handles
            # DB failures and provides a default fallback.
            data["locale"] = await safe_get_locale(user_id)
            logger.info("LocaleMiddleware: set locale %s for user %s", data["locale"], user_id)
        return await handler(event, data)
