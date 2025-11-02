"""Telegram interfaces composition: include all feature routers here."""

from aiogram import Router
from aiogram import F
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from bot.app.telegram.common.callbacks import NavCB
from bot.app.telegram.common.navigation import nav_root, nav_pop, nav_role_root
import logging

logger = logging.getLogger(__name__)

def build_main_router() -> Router:
    router = Router()
    # NOTE: global navigation handler is registered after feature routers
    # to ensure typed CallbackData handlers in feature routers receive
    # callbacks first. The handler is attached later below.
    
    # Include public/client router first so general commands (like /start)
    # and fallback message handlers are evaluated before role-protected
    # routers. This prevents router-level filters (Admin/Master) from
    # unintentionally intercepting or affecting public commands.
    try:
        from .admin.admin_handlers import admin_router
        router.include_router(admin_router)
        logger.info("Admin router included")
    except Exception as e:
        logger.error("Failed to include admin_router: %s", e)

    try:
        from .master.master_handlers import master_router
        router.include_router(master_router)
        logger.info("Master router included")
    except Exception as e:
        logger.error("Failed to include master_router: %s", e)

    try:
        from .client.client_handlers import client_router
        router.include_router(client_router)
        logger.info("Client router included")
    except Exception as e:
        logger.error("Failed to include client_router: %s", e)
    # Register global navigation handler after feature routers so it doesn't
    # intercept typed callback_data handlers defined in feature routers.
    @router.callback_query(NavCB.filter())
    async def _global_nav_handler(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
        try:
            act = getattr(callback_data, "act", None)
            if act == "root":
                await nav_root(cb, state)
            elif act == "back":
                await nav_pop(cb, state)
            elif act == "role_root":
                await nav_role_root(cb, state)
            try:
                await cb.answer()
            except Exception:
                pass
        except Exception:
            try:
                logging.getLogger(__name__).exception("_global_nav_handler failed")
            except Exception:
                pass

    logger.info("Main router assembled")
    # Debug handlers: log unhandled messages/callbacks to help diagnose routing issues.
    # These are registered last so they only run when no other handler matched.
    @router.message()
    async def _debug_unhandled_message(message):
        try:
            import json, logging as _logging
            _logging.getLogger("bot.debug").info("Unhandled message: %s %s", getattr(message.from_user, 'id', None), getattr(message, 'text', repr(message)))
        except Exception:
            pass

    @router.callback_query()
    async def _debug_unhandled_callback(cb):
        try:
            import logging as _logging
            _logging.getLogger("bot.debug").info("Unhandled callback: from=%s data=%s", getattr(cb.from_user, 'id', None), getattr(cb, 'data', None))
            await cb.answer()  # acknowledge to avoid client 'loading' state
        except Exception:
            pass
    return router