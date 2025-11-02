from __future__ import annotations
"""Role-based access helpers centralized for admin/master checks.

Usage:
    from .roles import ensure_role, ensure_admin, ensure_master
"""
import logging
from typing import Literal

from aiogram.types import CallbackQuery, Message
from aiogram.filters import BaseFilter

import os
import bot.config as cfg
from bot.app.core.db import get_session
from ...domain.models import User, Master
from sqlalchemy import select
from bot.app.services.shared_services import safe_get_locale
from bot.app.translations import t

logger = logging.getLogger(__name__)

RoleType = Literal["admin", "master"]


async def ensure_role(obj: Message | CallbackQuery, role: RoleType) -> bool:
    """Check the sender has the given role and notify on denial (localized).

    Returns True if allowed; otherwise sends an access denied message/alert and returns False.
    """
    uid = getattr(getattr(obj, "from_user", None), "id", None)
    allowed = False
    try:
        if role == "admin":
            allowed = bool(uid) and bool(await is_admin(int(uid)))
        elif role == "master":
            allowed = bool(uid) and bool(await is_master(int(uid)))
        else:
            allowed = False
    except Exception as e:
        logger.debug("Role check error for %s: %s", role, e)
        allowed = False

    if allowed:
        return True

    # Localized denial notification (use safe_get_locale to centralize fallback)
    lang = await safe_get_locale(int(uid or 0))
    key = "admin_access_denied" if role == "admin" else "master_access_denied"
    text = t(key, lang)
    try:
        # For direct messages, send the localized denial so the user sees it in chat.
        # For callback queries (button presses) avoid showing alert popups which
        # can be noisy if multiple callbacks are fired; acknowledge silently.
        if isinstance(obj, Message):
            await obj.answer(text)
        else:
            # Acknowledge the callback without an alert text to avoid repeated
            # modal popups for regular clients. Admins typically use commands
            # (Messages) and will still receive the textual notice above.
            try:
                await obj.answer()
            except Exception:
                # Best-effort: if answering fails, ignore - we don't want to
                # spam logs for common race conditions between callback edits.
                pass
    except Exception as send_err:
        logger.warning("Failed to send access denied message: %s", send_err)
    return False


async def ensure_admin(obj: Message | CallbackQuery) -> bool:
    return await ensure_role(obj, "admin")


async def ensure_master(obj: Message | CallbackQuery) -> bool:
    return await ensure_role(obj, "master")


async def is_admin_user(obj: Message | CallbackQuery) -> bool:
    """Return True if the sender is an admin without sending denial messages.

    Useful when code needs to branch behavior for admins but should not
    notify/deny non-admins as a side-effect.
    """
    try:
        uid = getattr(getattr(obj, "from_user", None), "id", None)
        return bool(uid) and bool(await is_admin(int(uid)))
    except Exception:
        return False


async def is_master_user(obj: Message | CallbackQuery) -> bool:
    """Return True if the sender is a master without sending denial messages."""
    try:
        uid = getattr(getattr(obj, "from_user", None), "id", None)
        return bool(uid) and bool(await is_master(int(uid)))
    except Exception:
        return False


# =====================================================
# ENV + DB backed role checks (moved from core.db)
# =====================================================
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()]
MASTER_IDS = [int(x) for x in os.getenv("MASTER_IDS", "").split(",") if x.strip().isdigit()]


def is_admin_env(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def is_master_env(user_id: int) -> bool:
    return user_id in MASTER_IDS


async def is_admin_db(user_id: int) -> bool:
    async with get_session() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == user_id, User.is_admin == True)
        )
        return result.scalar_one_or_none() is not None


async def is_master_db(user_id: int) -> bool:
    async with get_session() as session:
        result = await session.execute(
            select(Master).where(Master.telegram_id == user_id)
        )
        return result.scalar_one_or_none() is not None


async def is_admin(user_id: int) -> bool:
    if is_admin_env(user_id):
        return True
    return await is_admin_db(user_id)


async def is_master(user_id: int) -> bool:
    if is_master_env(user_id):
        return True
    return await is_master_db(user_id)



class AdminRoleFilter(BaseFilter):
    """Aiogram filter that allows only admin users.

    This reuses the existing `ensure_admin` helper so the denial message
    behavior and localization remain consistent.
    """

    async def __call__(self, obj: Message | CallbackQuery) -> bool:  # type: ignore[override]
        try:
            return await ensure_admin(obj)
        except Exception:
            return False


class MasterRoleFilter(BaseFilter):
    """Aiogram filter that allows only master users.

    Delegates to `ensure_master` for consistent behavior.
    """

    async def __call__(self, obj: Message | CallbackQuery) -> bool:  # type: ignore[override]
        try:
            return await ensure_master(obj)
        except Exception:
            return False


__all__ = [
    "ensure_role",
    "ensure_admin",
    "ensure_master",
    "is_admin_user",
    "is_master_user",
    "is_admin_env",
    "is_master_env",
    "is_admin_db",
    "is_master_db",
    "is_admin",
    "is_master",
    "AdminRoleFilter",
    "MasterRoleFilter",
]
