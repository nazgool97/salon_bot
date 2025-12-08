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
        # aiogram provides `from_user` for user-originated events; access directly.
        try:
            user = getattr(event, "from_user", None)
            if not user:
                # Non-user-originated events (channels, etc.) — skip locale.
                return await handler(event, data)
            user_id = getattr(user, "id", None)
            if not user_id:
                return await handler(event, data)
            # Delegate to the shared safe_get_locale helper which handles
            # DB failures and provides a default fallback.
            data["locale"] = await safe_get_locale(int(user_id))
            logger.info("LocaleMiddleware: set locale %s for user %s", data["locale"], int(user_id))
        except Exception:
            # Be defensive: do not prevent handlers from running if locale
            # resolution fails for any reason.
            logger.exception("LocaleMiddleware failed to resolve locale")
        return await handler(event, data)
