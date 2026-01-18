"""Telegram interfaces composition: include all feature routers here."""

import contextlib
import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from bot.app.telegram.common.callbacks import NavCB
from bot.app.telegram.common.navigation import nav_root, nav_pop, nav_role_root

logger = logging.getLogger(__name__)


def build_main_router() -> Router:
    router = Router()
    # Register global navigation handler after feature routers so feature
    # CallbackData handlers are matched first.

    # Include public/client router first so general commands (like /start)
    # and fallback message handlers are evaluated before role-protected
    # routers. This prevents router-level filters (Admin/Master) from
    # unintentionally intercepting or affecting public commands.
    # Include public/client router first so general commands (like /start)
    # and fallback message handlers are evaluated before role-protected
    # routers. This prevents router-level filters (Admin/Master) from
    # unintentionally intercepting or affecting public commands.
    try:
        from .client.client_handlers import client_router

        router.include_router(client_router)
        logger.info("Client router included")
    except Exception as e:
        logger.error("Failed to include client_router: %s", e)

    # Then include admin and master routers which are protected by
    # role filters and should be evaluated after public handlers.
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

    # Register global navigation handler after feature routers so it doesn't
    # intercept typed callback_data handlers defined in feature routers.
    @router.callback_query(NavCB.filter(F.act.in_(["root", "back", "role_root", "noop"])))
    async def _global_nav_handler(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
        try:
            act = getattr(callback_data, "act", None)
            if act == "root":
                await nav_root(cb, state)
            elif act == "back":
                await nav_pop(cb, state)
            elif act == "role_root":
                await nav_role_root(cb, state)
            elif act == "noop":
                # explicit no-op to acknowledge label buttons
                pass
            with contextlib.suppress(Exception):
                await cb.answer()
        except Exception:
            with contextlib.suppress(Exception):
                logging.getLogger(__name__).exception("_global_nav_handler failed")

    logger.info("Main router assembled")
    return router
