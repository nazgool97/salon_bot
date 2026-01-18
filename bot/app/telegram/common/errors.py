from __future__ import annotations
import logging
from typing import Any

from bot.app.core.notifications import notify_admins
from aiogram import Bot

logger = logging.getLogger(__name__)

__all__ = ["handle_db_error", "handle_telegram_error"]


async def handle_db_error(
    error: Exception, context: str = "database operation", bot: Bot | None = None
) -> None:
    """Обрабатывает ошибки базы данных: логирует и уведомляет админов.

    Args:
        error: Исключение, связанное с базой данных.
        context: Контекст ошибки (например, название операции).
    """
    logger.error("Ошибка базы данных в %s: %s", context, str(error))
    try:
        if bot is None:
            logger.debug("handle_db_error: no bot passed, skipping admin notification")
        else:
            await notify_admins(f"❌ DB Error in {context}: {str(error)}", bot)
    except Exception as notify_err:
        logger.error("Не удалось уведомить админов о DB Error: %s", notify_err)


async def handle_telegram_error(
    error: Exception, context: str = "Telegram API operation", bot: Bot | None = None
) -> None:
    """Обрабатывает ошибки Telegram API: логирует и уведомляет админов.

    Args:
        error: Исключение, связанное с Telegram API.
        context: Контекст ошибки (например, название операции).
    """
    logger.error("Ошибка Telegram API в %s: %s", context, str(error))
    try:
        if bot is None:
            logger.debug("handle_telegram_error: no bot passed, skipping admin notification")
        else:
            await notify_admins(f"⚠️ Telegram Error in {context}: {str(error)}", bot)
    except Exception as notify_err:
        logger.error("Не удалось уведомить админов о Telegram Error: %s", notify_err)
