"""Runtime entrypoint for Telegram bot."""
import asyncio
import logging
import os
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from rich.logging import RichHandler

from bot.app.telegram.main_router import build_main_router
from bot.app.workers.expiration import start_expiration_worker, start_cleanup_worker
from bot.app.workers.reminders import start_reminders_worker
from bot.app.core.db import get_session
from bot.app.domain.models import Master
from bot.app.services.shared_services import get_admin_ids, safe_get_locale
from bot.app.translations import t
import argparse
import sys


# ==============================================================
# LOGGING CONFIG (Variant B)
# ==============================================================

# Console: INFO / WARNING / ERROR (Rich)
console_handler = RichHandler(
    rich_tracebacks=True,
    markup=True,
    show_time=True,
    show_level=True,
    show_path=False,
    log_time_format="%H:%M:%S",
)

# File: WARNING+ only
file_handler = logging.FileHandler("bot.log", encoding="utf-8")
file_handler.setLevel(logging.WARNING)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
))

# Resolve root log level from environment (LOG_LEVEL), default INFO
_env_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
_level = getattr(logging, _env_level, logging.INFO)

logging.basicConfig(
    level=_level,       # configurable via LOG_LEVEL
    format="%(message)s",
    handlers=[console_handler, file_handler],
)

logger = logging.getLogger("bot")


# Reduce noisy logs — but keep WARNINGS
logging.getLogger("aiogram").setLevel(logging.INFO)
logging.getLogger("aiogram.dispatcher").setLevel(logging.INFO)
logging.getLogger("aiogram.event").setLevel(logging.INFO)

logging.getLogger("asyncpg").setLevel(logging.WARNING)
logging.getLogger("alembic").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


# ==============================================================
# BOOTSTRAP
# ==============================================================

async def maybe_seed() -> None:
    """Опционально заполняет данные через RUN_BOOTSTRAP."""
    if os.getenv("RUN_BOOTSTRAP", "0").lower() not in {"1", "true", "yes"}:
        return

    logger.info("[bootstrap] Running…")

    try:
        from bot.app.core.bootstrap import init_services, init_masters
        from bot.app.services.admin_services import invalidate_services_cache
        from bot.app.services.master_services import invalidate_masters_cache

        await init_services()
        await init_masters()

        try:
            invalidate_services_cache()
        except Exception:
            logger.exception("maybe_seed: invalidate_services_cache failed")

        try:
            invalidate_masters_cache()
        except Exception:
            logger.exception("maybe_seed: invalidate_masters_cache failed")

        logger.info("[bootstrap] Completed")
    except Exception as e:
        logger.error("[bootstrap] Failed: %s", e)


# ==============================================================
# ADMIN NOTIFY
# ==============================================================

async def _notify_admins(bot: Bot) -> None:
    admin_ids = get_admin_ids()

    for uid in admin_ids:
        try:
            lang = await safe_get_locale(int(uid or 0))
            msg = t("bot_started_notice", lang)

            if msg == "bot_started_notice":
                msg = {
                    "uk": "Бот запущено. Надішліть /start або /ping.",
                    "ru": "Бот запущен. Отправьте /start или /ping.",
                    "en": "Bot started. Send /start or /ping."
                }.get(lang, "Bot started. Send /start or /ping.")

            from bot.app.services.shared_services import _safe_send

            try:
                await _safe_send(bot, uid, msg)
            except Exception:
                await bot.send_message(uid, msg)
        except Exception:
            logger.exception("_notify_admins: failed to send admin notifications")


# ==============================================================
# MAIN
# ==============================================================

async def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN is not set")
        raise SystemExit(1)

    # Load settings BEFORE routers
    try:
        from bot.app.services.admin_services import load_settings_from_db
        await load_settings_from_db()
        logger.info("Loaded runtime settings from DB")
    except Exception as e:
        logger.warning("Could not load settings from DB: %s", e)

    bot = Bot(token=token, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    # Navigation first
    try:
        from bot.app.telegram.common.navigation import nav_router
        dp.include_router(nav_router)
        logger.info("Navigation router included")
    except Exception as e:
        logger.error("Failed to include nav_router: %s", e)

    # Main router
    try:
        main_router = build_main_router()
        dp.include_router(main_router)
        logger.info("Main router included")
    except Exception as e:
        logger.error("Failed to include main router: %s", e)

    # Ensure polling mode
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        logger.info("Webhook removed → polling enabled")
    except Exception:
        logger.exception("main: failed to delete webhook (continuing)")

    # Global error handlers
    try:
        from aiogram.filters import ExceptionTypeFilter
        from sqlalchemy.exc import SQLAlchemyError
        from aiogram.exceptions import TelegramAPIError
        from bot.app.telegram.common.errors import (
            handle_db_error, handle_telegram_error
        )

        async def _extract_exception(args, kwargs):
            """Helper: find an exception object from various aiogram error handler signatures.

            aiogram versions/passages may call registered error handlers with different
            signatures (for example: (update, exception) or a single ErrorEvent object
            with an .exception attribute). Make extraction robust.
            """
            # kwargs may contain 'exception'
            if kwargs.get("exception"):
                return kwargs.get("exception")

            # args might be (update, exception)
            if len(args) >= 2 and isinstance(args[1], Exception):
                return args[1]

            # args might be a single ErrorEvent-like object with .exception
            if len(args) >= 1:
                first = args[0]
                if hasattr(first, "exception"):
                    return getattr(first, "exception")

            # fallback: try to find any Exception instance in args
            for a in args:
                if isinstance(a, Exception):
                    return a

            return None


        async def _on_db_error(*args, **kwargs):
            exc = await _extract_exception(args, kwargs)
            if exc is None:
                # Nothing to do
                return
            await handle_db_error(exc)


        async def _on_telegram_error(*args, **kwargs):
            exc = await _extract_exception(args, kwargs)
            if exc is None:
                return
            await handle_telegram_error(exc)


        async def _on_unhandled(*args, **kwargs):
            exc = await _extract_exception(args, kwargs)
            logger.exception("Unhandled exception: %s", exc)
            if exc is not None:
                await handle_telegram_error(exc)

        dp.errors.register(_on_db_error, ExceptionTypeFilter(SQLAlchemyError))
        dp.errors.register(_on_telegram_error, ExceptionTypeFilter(TelegramAPIError))
        dp.errors.register(_on_unhandled)

        logger.info("Global error handlers registered")
    except Exception as e:
        logger.warning("Failed to register error handlers: %s", e)

    # Seed
    await maybe_seed()

    # Log update types
    try:
        used = dp.resolve_used_update_types()
        logger.info("Update types: %s", used)
    except Exception:
        logger.exception("main: resolve_used_update_types failed")

    # Notify admins
    await _notify_admins(bot)

    # Start background workers
    stop_exp = await start_expiration_worker()
    stop_rem = await start_reminders_worker(bot)
    stop_cleanup = await start_cleanup_worker(bot)

    logger.info("Starting polling…")

    try:
        await dp.start_polling(bot)
    finally:
        try:
            await stop_exp()
        except Exception:
            logger.exception("main: stop_exp failed during shutdown")
        try:
            await stop_rem()
        except Exception:
            logger.exception("main: stop_rem failed during shutdown")
        try:
            await stop_cleanup()
        except Exception:
            logger.exception("main: stop_cleanup failed during shutdown")


# ==============================================================
# CLI helper: create-master
# ==============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="run_bot.py")
    sub = parser.add_subparsers(dest="cmd")

    cm = sub.add_parser("create-master")
    cm.add_argument("--tg-id", type=int, required=True)
    cm.add_argument("--name", type=str, required=True)
    cm.add_argument("--admin-id", type=int, required=True)

    args = parser.parse_args()

    if args.cmd == "create-master":
        admin_ids = set(get_admin_ids())
        if args.admin_id not in admin_ids:
            print("Forbidden: admin_id not allowed", file=sys.stderr)
            raise SystemExit(2)

        async def _create(tg_id, name):
            async with get_session() as session:
                from sqlalchemy import select
                res = await session.execute(select(Master).where(Master.telegram_id == tg_id))
                if res.scalars().first():
                    print("Master already exists.")
                    return 1

                session.add(Master(telegram_id=tg_id, name=name))
                await session.commit()
                print("Master created.")
                return 0

        exit(asyncio.run(_create(args.tg_id, args.name)))

    # Default behavior — run bot
    with suppress(KeyboardInterrupt, SystemExit):
        asyncio.run(main())
