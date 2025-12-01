from __future__ import annotations

import logging
import os
from typing import Iterable

from aiogram import Bot

from bot.app.services.shared_services import _safe_send

logger = logging.getLogger(__name__)

__all__ = ["notify_admins"]


async def notify_admins(message: str, bot: Bot) -> None:
    """Send a notification message to configured admin IDs using the provided Bot.

    This function now requires an explicit aiogram.Bot instance. It will not
    attempt to import or create a Bot implicitly — callers must pass the
    running bot (for example, the instance created in `run_bot.py`).

    Args:
        message: Text to send to admins.
        bot: An initialized aiogram.Bot instance.
    """
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids: Iterable[int] = [
        int(part.strip()) for part in admin_ids_str.split(",") if part.strip().isdigit()
    ]
    if not admin_ids:
        logger.debug("notify_admins: no ADMIN_IDS configured; skipping")
        return

    for admin_id in admin_ids:
        try:
            # Use the centralized safe send helper; fall back to Bot.send_message
            try:
                await _safe_send(bot, admin_id, message)
            except Exception:
                await bot.send_message(admin_id, message)
        except Exception as e:
            logger.error("notify_admins: failed to send to %s: %s", admin_id, e)
