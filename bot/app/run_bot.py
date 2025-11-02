"""Runtime entrypoint for Telegram bot."""
import asyncio
import logging
import os
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from bot.app.telegram.main_router import build_main_router
from bot.app.workers.expiration import start_expiration_worker, stop_expiration_worker
from bot.app.workers.reminders import start_reminders_worker, stop_reminders_worker
import bot.config as bot_config
import argparse
import sys
from bot.app.core.db import get_session
from bot.app.domain.models import Master

logger = logging.getLogger("bot.run")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log")]
)

async def maybe_seed() -> None:
    """Опционально заполняет базовые данные, если установлен флаг RUN_BOOTSTRAP.

    Использует легковесные идемпотентные помощники из bot.app.core.bootstrap.
    """
    if os.getenv("RUN_BOOTSTRAP", "0").lower() not in {"1", "true", "yes"}:
        return
    logger.info("[bootstrap] Seeding baseline services/masters if missing…")
    try:
        from bot.app.core.bootstrap import init_services, init_masters
        import bot.config as cfg

        await init_services()
        await init_masters()
        # Очищаем кэш, чтобы меню сразу отображали свежие данные
        try:
            getattr(cfg, "invalidate_services_cache", (lambda: None))()
            getattr(cfg, "invalidate_masters_cache", (lambda: None))()
        except Exception:
            pass
        logger.info("[bootstrap] Seeding completed")
    except Exception as e:
        logger.error("Bootstrap seeding failed: %s", e)

async def _notify_admins(bot: Bot) -> None:
    import bot.config as cfg
    from bot.app.services.shared_services import safe_get_locale
    from bot.app.translations import t
    admin_ids = getattr(cfg, "ADMIN_IDS", []) or []
    for uid in admin_ids:
        try:
            # Use safe_get_locale which centralizes DB errors and provides a default
            lang = await safe_get_locale(int(uid or 0))
            msg = t("bot_started_notice", lang)
            if msg == "bot_started_notice":
                # Текст по умолчанию, если ключ отсутствует
                msg = {
                    "uk": "Бот запущено. Надішліть /start для меню або /ping для перевірки.",
                    "ru": "Бот запущен. Отправьте /start для меню или /ping для проверки.",
                    "en": "Bot started. Send /start for menu or /ping to check."
                }.get(lang, "Bot started. Send /start for menu or /ping to check.")
            try:
                from bot.app.services.shared_services import _safe_send
                await _safe_send(bot, uid, msg)
            except Exception:
                await bot.send_message(uid, msg)
        except Exception:
            # Игнорируем, если пользователь не запустил бот
            pass

async def main() -> None:
    TELEGRAM_TOKEN = getattr(bot_config, "BOT_TOKEN", "") or os.getenv("BOT_TOKEN", "")
    if not TELEGRAM_TOKEN:
        logger.error("BOT_TOKEN is not set")
        raise SystemExit("BOT_TOKEN env var is required")

    # Загружаем настройки из базы данных (переопределяют ENV) перед подключением роутеров/воркеров
    try:
        import bot.config as cfg
        fn = getattr(cfg, "load_settings_from_db", None)
        if callable(fn):
            res = fn()
            # Ожидаем, если это корутина
            if hasattr(res, "__await__"):
                await res  # type: ignore
            logger.info("Runtime settings loaded from DB at startup")
    except Exception as e:
        logger.warning("Could not load settings from DB at startup: %s", e)

    # Инициализируем Bot и Dispatcher
    bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    # Подключаем feature роутеры напрямую в Dispatcher.
    # Непосредственное включение каждого роутера в dp обходиt возможные
    # несовместимости версий aiogram при вложенных include_router.
    # Register admin router first so role-protected FSM handlers are matched
    # before more general client fallback handlers. This ordering prevents
    # the client catch-all from accidentally consuming stateful admin inputs.
    # Include a small global navigation router first so NavCB callbacks are
    # handled before other feature routers. This router should be included
    # into the Dispatcher PRIOR to admin/master/client routers.
    try:
        from bot.app.telegram.common.navigation import nav_router
        dp.include_router(nav_router)
        logger.info("Navigation router included into Dispatcher")
    except Exception as e:
        logger.error("Failed to include nav_router into Dispatcher: %s", e)

    try:
        from bot.app.telegram.admin.admin_handlers import admin_router
        dp.include_router(admin_router)
        logger.info("Admin router included into Dispatcher")
    except Exception as e:
        logger.error("Failed to include admin_router into Dispatcher: %s", e)

    # Include master router before client router so master role-protected
    # callback handlers and FSM flows are matched prior to more general
    # client handlers. This reduces fragile fallback parsing in master code.
    try:
        from bot.app.telegram.master.master_handlers import master_router
        dp.include_router(master_router)
        logger.info("Master router included into Dispatcher")
    except Exception as e:
        logger.error("Failed to include master_router into Dispatcher: %s", e)

    try:
        from bot.app.telegram.client.client_handlers import client_router
        dp.include_router(client_router)
        logger.info("Client router included into Dispatcher")
    except Exception as e:
        logger.error("Failed to include client_router into Dispatcher: %s", e)

    # Keep a composed main_router available for other tooling, but do not rely
    # on nested include_router behaviour for registration.
    try:
        main_router = build_main_router()
        logger.info("Main router built (feature routers already registered into Dispatcher)")
    except Exception:
        main_router = None
    # Ensure main_router's global handlers (e.g. NavCB) are registered in the
    # Dispatcher. build_main_router composes feature routers but here we also
    # include it into the Dispatcher so top-level handlers (global navigation)
    # are active.
    if main_router is not None:
        try:
            dp.include_router(main_router)
            logger.info("Main router included into Dispatcher")
        except Exception as e:
            logger.error("Failed to include main_router into Dispatcher: %s", e)

    # Убедимся, что polling получает обновления, если ранее был установлен вебхук
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        logger.info("Webhook deleted (if existed); switching to polling mode")
    except Exception as e:
        logger.debug("No webhook to delete or failed to delete webhook: %s", e)

    # Register global error handlers on the Dispatcher so aiogram will forward
    # uncaught exceptions from handlers here. This allows removing many
    # per-handler try/except blocks and centralizes logging/notifications.
    try:
        from aiogram.filters import ExceptionTypeFilter
        from sqlalchemy.exc import SQLAlchemyError
        from aiogram.exceptions import TelegramAPIError
        from bot.app.telegram.common.errors import (
            handle_db_error,
            handle_telegram_error,
        )

        async def _on_db_error(update, exception):
            # attempt best-effort context extraction
            ctx = f"update_id={getattr(update, 'update_id', None)}"
            await handle_db_error(exception, context=ctx)

        async def _on_telegram_error(update, exception):
            ctx = f"update_id={getattr(update, 'update_id', None)}"
            await handle_telegram_error(exception, context=ctx)

        async def _on_global_error(update, exception):
            # Fallback for any other exceptions — log and notify admins
            logger.exception("Unhandled exception in handler for update=%s: %s", getattr(update, 'update_id', None), exception)
            try:
                # reuse telegram error notifier for admin alerts
                await handle_telegram_error(exception, context=f"unhandled update {getattr(update, 'update_id', None)}")
            except Exception:
                # swallow
                pass

        dp.errors.register(_on_db_error, ExceptionTypeFilter(SQLAlchemyError))
        dp.errors.register(_on_telegram_error, ExceptionTypeFilter(TelegramAPIError))
        dp.errors.register(_on_global_error)
        logger.info("Registered global error handlers on Dispatcher")
    except Exception as e:
        logger.warning("Failed to register Dispatcher error handlers: %s", e)

    # Опционально заполняем данные перед началом polling, если установлен флаг
    await maybe_seed()

    # Логируем разрешенные типы обновлений для диагностики
    try:
        used = dp.resolve_used_update_types()
        logger.info("Resolved update types: %s", used)
    except Exception:
        pass

    # Уведомляем админов о запуске
    await _notify_admins(bot)

    # Запускаем фоновые воркеры
    stop_worker = await start_expiration_worker()
    stop_reminders = await start_reminders_worker(bot)
    logger.info("Starting polling...")

    try:
        # Позволяем aiogram определять разрешенные обновления (без ограничений)
        await dp.start_polling(bot)
    finally:
        try:
            await stop_worker()
        except Exception:
            pass
        try:
            await stop_reminders()
        except Exception:
            pass

if __name__ == "__main__":
    # Support a small management subcommand without adding new files.
    parser = argparse.ArgumentParser(prog="run_bot.py")
    sub = parser.add_subparsers(dest="cmd")
    cm = sub.add_parser("create-master", help="Create a Master record (admin-only)")
    cm.add_argument("--tg-id", required=True, type=int, help="Telegram ID for the new master")
    cm.add_argument("--name", required=True, type=str, help="Display name for the master")
    cm.add_argument("--admin-id", required=True, type=int, help="Telegram ID of the admin running this command (must be in ADMIN_IDS)")

    args = parser.parse_args()
    if args.cmd == "create-master":
        # Safety check: require the provided admin-id to be in configured ADMIN_IDS
        import bot.config as cfg
        if int(args.admin_id) not in getattr(cfg, "ADMIN_IDS", set()):
            print("Refusing to run: admin_id not in cfg.ADMIN_IDS", file=sys.stderr)
            raise SystemExit(2)

        async def _create_master(tg_id: int, name: str) -> int:
            async with get_session() as session:
                # Try to find existing by telegram_id
                from sqlalchemy import select
                res = await session.execute(select(Master).where(Master.telegram_id == tg_id))
                existing = res.scalars().first()
                if existing:
                    print("Master already exists.")
                    return 1
                session.add(Master(telegram_id=tg_id, name=name))
                await session.commit()
                print("Master created.")
                return 0

        rc = asyncio.run(_create_master(int(args.tg_id), args.name))
        raise SystemExit(rc)
    else:
        with suppress(KeyboardInterrupt, SystemExit):
            asyncio.run(main())