from __future__ import annotations

"""Global navigation stack helpers for a single universal back button.

State keys used in FSMContext:
    nav_stack: list[dict{text:str, markup:dict|None}]
    current_text: str | None
    current_markup: dict | None

We serialize InlineKeyboardMarkup via model_dump/model_validate so it is safe
to restore later for editing the same message.
"""
from typing import Any
from aiogram.types import InlineKeyboardMarkup
from aiogram.fsm.context import FSMContext


def _dump_markup(markup: InlineKeyboardMarkup | None) -> dict[str, Any] | None:
    if not markup:
        return None
    try:
        return markup.model_dump()
    except Exception:
        return None


def _restore_markup(dump: dict[str, Any] | None) -> InlineKeyboardMarkup | None:
    if not dump:
        return None
    try:
        return InlineKeyboardMarkup.model_validate(dump)
    except Exception:
        return None


async def nav_reset(state: FSMContext) -> None:
    """Clear navigation stack (entering a root screen). Preserves current_lang if set."""
    data = await state.get_data()
    cur_lang = data.get("current_lang")
    await state.update_data(
        nav_stack=[], current_text=None, current_markup=None, current_lang=cur_lang
    )


async def nav_push(
    state: FSMContext,
    new_text: str,
    new_markup: InlineKeyboardMarkup | None,
    *,
    deduplicate: bool = True,
    lang: str | None = None,
    parse_mode: str | None = None,
) -> bool:
    """Push current screen onto stack and set new current screen.

    Args:
        state: FSM context.
        new_text: Text of the new screen.
        new_markup: Inline keyboard markup of the new screen.
        deduplicate: If True (default) and the new screen is identical to the current,
            nothing is pushed and the function returns False.

    Returns:
        True if a push occurred, False if skipped due to deduplication.
    """
    data = await state.get_data()
    stack: list[dict[str, Any]] = data.get("nav_stack", [])
    cur_text = data.get("current_text")
    cur_markup = data.get("current_markup")
    dumped_new = _dump_markup(new_markup)
    if deduplicate and cur_text == new_text and cur_markup == dumped_new:
        return False
    if cur_text is not None:
        stack.append(
            {"text": cur_text, "markup": cur_markup, "parse_mode": data.get("current_parse_mode")}
        )
    payload = {"nav_stack": stack, "current_text": new_text, "current_markup": dumped_new}
    # store parse_mode for the current screen so it can be restored when popping
    if parse_mode:
        payload["current_parse_mode"] = parse_mode
    if lang:
        payload["current_lang"] = lang
    # mypy: FSMContext.update_data accepts a Mapping[str, Any] | None as a single
    # argument; using the dict directly keeps the typing consistent.
    await state.update_data(payload)
    return True


async def nav_back(state: FSMContext) -> tuple[str | None, InlineKeyboardMarkup | None, bool]:
    """Pop one screen.

    Returns:
        (text, markup, popped?) where popped? is False if stack empty.
    """
    data = await state.get_data()
    stack: list[dict[str, Any]] = data.get("nav_stack", [])
    if not stack:
        return None, None, False
    frame = stack.pop()
    # Restore parse_mode for the now-current frame (might be None)
    await state.update_data(
        nav_stack=stack,
        current_text=frame.get("text"),
        current_markup=frame.get("markup"),
        current_parse_mode=frame.get("parse_mode"),
    )
    return frame.get("text"), _restore_markup(frame.get("markup")), True


async def nav_replace(
    state: FSMContext,
    new_text: str,
    new_markup: InlineKeyboardMarkup | None,
    *,
    lang: str | None = None,
) -> None:
    """Replace current screen without pushing previous state. Optionally set lang."""
    payload: dict[str, Any] = {"current_text": new_text, "current_markup": _dump_markup(new_markup)}
    if lang:
        payload["current_lang"] = lang
    # preserve parse_mode when replacing if provided in new_markup.model_dump()
    # Pass mapping directly to match FSMContext.update_data typing
    await state.update_data(payload)


async def nav_can_go_back(state: FSMContext) -> bool:
    """Return True if there is at least one frame to go back to."""
    data = await state.get_data()
    stack: list[dict[str, Any]] = data.get("nav_stack", [])
    return bool(stack)


async def nav_current(state: FSMContext) -> tuple[str | None, InlineKeyboardMarkup | None]:
    """Return the current (text, markup) pair without modifying the stack."""
    data = await state.get_data()
    return data.get("current_text"), _restore_markup(data.get("current_markup"))


async def nav_get_lang(state: FSMContext | None) -> str | None:
    """Get the current UI language stored in nav state (if any).

    NOTE: Locale is expected to be provided by middleware and passed to handlers
    as the `locale` argument. Returning `None` here ensures existing handler
    patterns that fall back to `locale` (e.g. `(await nav_get_lang(state)) or locale`)
    will consistently use the middleware-provided `locale` as the single source
    of truth. This reduces the need for handlers to read FSM data for locale.
    """
    return None


async def nav_set_lang(state: FSMContext, lang: str) -> None:
    """Set the current UI language in nav state."""
    await state.update_data(current_lang=lang)


__all__ = [
    "nav_push",
    "nav_back",
    "nav_reset",
    "nav_replace",
    "nav_can_go_back",
    "nav_current",
    "nav_get_lang",
    "nav_set_lang",
]


from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramAPIError
import logging

logger = logging.getLogger(__name__)


from aiogram import Router, F
from bot.app.telegram.common.callbacks import NavCB


async def show_main_client_menu(
    obj: Message | CallbackQuery,
    state: FSMContext | None = None,
    *,
    prefer_edit: bool = True,
) -> None:
    """Show the canonical client main menu.

    This helper centralizes the minimal flow used by both client and admin
    handlers to return a user to the main client menu. It performs:
      - optional nav_reset(state)
      - calls `get_main_menu(telegram_id)` to build the keyboard
      - attempts to edit the current message with `safe_edit` or sends a new
        message when edit is not possible
      - updates navigation state via `nav_replace`

    The implementation uses lazy imports of higher-level modules to avoid
    creating import cycles at module import time.
    """
    try:
        # Local imports to avoid top-level cycles
        from bot.app.telegram.client.client_keyboards import get_main_menu
        from bot.app.services.shared_services import safe_get_locale, tr
        from bot.app.telegram.common.ui_fail_safe import safe_edit
    except Exception:
        # If imports fail, log and abort silently to avoid crashing callers
        logger.debug("show_main_client_menu: required imports unavailable")
        return

    try:
        # Determine user id and target message
        # Both `Message` and `CallbackQuery` in aiogram expose `from_user` for user-originated updates.
        # Use direct access so exceptions surface instead of silently continuing with a default.
        user_id = obj.from_user.id
        target_msg = obj.message if isinstance(obj, CallbackQuery) else obj

        # Reset navigation stack when explicitly requested by caller
        if state is not None:
            try:
                await nav_reset(state)
            except Exception:
                logger.exception("nav_reset failed inside show_main_client_menu")

        kb = await get_main_menu(user_id)
        try:
            logger.debug("Отправлено меню: %s", getattr(kb, "inline_keyboard", None))
        except Exception:
            logger.exception("show_main_client_menu: logging debug failed")
        lang = await safe_get_locale(user_id)
        text_root = tr("menu", lang=lang) or "🏠 Menu"

        # Try to edit existing message when appropriate
        if target_msg is not None and prefer_edit and isinstance(obj, CallbackQuery):
            try:
                await safe_edit(target_msg, text_root, reply_markup=kb)
            except Exception as e:
                if "message is not modified" in str(e).lower():
                    try:
                        await target_msg.answer(text_root, reply_markup=kb)
                    except Exception:
                        logger.exception("show_main_client_menu: answer fallback failed")
                else:
                    # Reraise to let caller handle unexpected Telegram errors
                    raise
        elif target_msg is not None:
            try:
                await target_msg.answer(text_root, reply_markup=kb)
            except Exception:
                logger.exception("show_main_client_menu: send via target_msg failed")

        if state is not None:
            try:
                await nav_replace(state, text_root, kb)
                try:
                    await state.update_data(current_screen="main")
                except Exception:
                    logger.exception("show_main_client_menu: failed to update state.current_screen")
            except Exception:
                logger.exception("show_main_client_menu: failed to nav_replace or update state")
    except Exception:
        # Log unexpected exceptions; don't raise from helper
        logger.exception("show_main_client_menu: unexpected error")


async def nav_root(obj: Message | CallbackQuery, state: FSMContext | None) -> None:
    """Reset navigation stack and show the global client main menu.

    This is the canonical implementation for the "В МЕНЮ" button.
    """
    try:
        if state is not None:
            try:
                await nav_reset(state)
            except Exception:
                logger.exception("nav_root: nav_reset failed")
        await show_main_client_menu(obj, state)
    except Exception:
        logger.exception("nav_root: unexpected error")


async def nav_pop(obj: Message | CallbackQuery, state: FSMContext | None) -> None:
    """One-step back: pop nav stack and edit the message or show root when empty.

    This is the canonical implementation for the "Назад" (step) button.
    """
    if state is None:
        # Nothing we can do reliably without state; fall back to root
        await nav_root(obj, state)
        return
    try:
        text, markup, popped = await nav_back(state)
        if not popped:
            await nav_root(obj, state)
            return
        # We popped a frame — edit the message if possible
        try:
            if isinstance(obj, CallbackQuery) and obj.message is not None:
                from bot.app.telegram.common.ui_fail_safe import safe_edit

                # retrieve parse_mode that was stored for the restored frame
                try:
                    data = await state.get_data()
                    pm = data.get("current_parse_mode")
                except Exception:
                    pm = None
                # Pass keyword args explicitly so static type checkers don't
                # attempt to match positional parameters (e.g. fallback_text).
                if pm:
                    await safe_edit(obj.message, text or "", reply_markup=markup, parse_mode=pm)
                else:
                    await safe_edit(obj.message, text or "", reply_markup=markup)
                return
        except Exception:
            logger.exception("nav_pop: failed to pop or restore navigation frame")
        # Fallback: show root
        await nav_root(obj, state)
    except Exception:
        logger.exception("nav_pop: unexpected error")


async def nav_role_root(obj: Message | CallbackQuery, state: FSMContext | None) -> None:
    """Reset navigation and show the role-specific root menu.

    Priority: admin -> master -> client
    This implements the "Назад в своё меню" behaviour.
    """
    # If the user is already sitting on a role root (admin or master),
    # treat the 'role_root' button as a request to leave the role UI and
    # return to the global client root. We must check the current_text
    # before clearing the navigation stack (nav_reset) because nav_reset
    # clears the current_text value.
    cur_text = None
    cur_lang = None
    if state is not None:
        d = await state.get_data()
        cur_text = d.get("current_text")
        cur_lang = d.get("current_lang")

    # If current_text matches a role's root title, go to client root instead
    if cur_text is not None:
        from bot.app.services.shared_services import safe_get_locale
        from bot.app.translations import tr

        # Determine language for comparison (prefer stored lang)
        if not cur_lang:
            user_id = obj.from_user.id
            try:
                cur_lang = await safe_get_locale(user_id)
            except Exception:
                cur_lang = None

        admin_title = (
            tr("admin_panel_title", lang=cur_lang)
            if cur_lang is not None
            else tr("admin_panel_title")
        )
        master_title = (
            tr("master_menu_header", lang=cur_lang)
            if cur_lang is not None
            else tr("master_menu_header")
        )
        if cur_text in (admin_title, master_title):
            # User is already on a role's root screen -> go to client root
            await nav_root(obj, state)
            return

    # Reset nav stack now (role root should be the new root)
    if state is not None:
        await nav_reset(state)

    # Determine role and show the appropriate menu. Use lazy imports to
    # avoid circular imports at module load time.
    # If FSM state contains a preferred role hint (set by role-specific
    # handlers when they open their root screen), honor it first so the
    # 'role_root' button returns the user to the UI they came from.
    pref = None
    if state is not None:
        d = await state.get_data()
        pref = d.get("preferred_role")

    if pref == "master":
        from bot.app.telegram.master.master_handlers import show_master_menu

        if state is not None:
            await show_master_menu(obj if isinstance(obj, CallbackQuery) else obj, state)
            return

    if pref == "admin":
        from bot.app.telegram.admin.admin_keyboards import admin_menu_kb
        from bot.app.translations import tr
        from bot.app.services.shared_services import safe_get_locale

        user_id = obj.from_user.id
        lang = await safe_get_locale(user_id)
        text = tr("admin_panel_title", lang=lang) or "Admin"
        if isinstance(obj, CallbackQuery) and obj.message is not None:
            from bot.app.telegram.common.ui_fail_safe import safe_edit

            try:
                await safe_edit(obj.message, text, reply_markup=admin_menu_kb(lang))
            except TelegramAPIError as exc:
                # Only swallow Telegram API acknowledgement errors (network, flood, etc.)
                logger.debug("nav_role_root: TelegramAPIError while editing admin root: %s", exc)
        if state is not None:
            await nav_replace(state, text, admin_menu_kb(lang), lang=lang)
        return

    # If no preferred role hint or preferred handling failed, fall back
    # to dynamic role detection (existing behaviour).
    from bot.app.telegram.common.roles import is_admin_user, is_master_user

    # Prefer master menu first so a user who is both master and admin
    # returns to the master UI when coming from master flows.
    if await is_master_user(obj):
        from bot.app.telegram.master.master_handlers import show_master_menu

        if state is not None:
            await show_master_menu(obj if isinstance(obj, CallbackQuery) else obj, state)
            return

    if await is_admin_user(obj):
        from bot.app.telegram.admin.admin_keyboards import admin_menu_kb
        from bot.app.translations import tr
        from bot.app.services.shared_services import safe_get_locale

        user_id = obj.from_user.id
        lang = await safe_get_locale(user_id)
        text = tr("admin_panel_title", lang=lang) or "Admin"
        if isinstance(obj, CallbackQuery) and obj.message is not None:
            from bot.app.telegram.common.ui_fail_safe import safe_edit

            try:
                await safe_edit(obj.message, text, reply_markup=admin_menu_kb(lang))
            except TelegramAPIError as exc:
                logger.debug("nav_role_root: TelegramAPIError while editing admin root: %s", exc)
        if state is not None:
            await nav_replace(state, text, admin_menu_kb(lang), lang=lang)
        return

    # Default fallback: client main menu
    await nav_root(obj, state)


# Navigation router: central handler for NavCB callbacks (root/back/role_root).
# This should be registered into the Dispatcher before role-specific routers
# so navigation actions are handled globally.
nav_router = Router(name="navigation")


@nav_router.callback_query(NavCB.filter(F.act == "root"))
async def _handle_nav_root(cb: CallbackQuery, state: FSMContext) -> None:
    """Глобальный возврат в главное меню клиента."""
    await cb.answer()
    await nav_root(cb, state)


@nav_router.callback_query(NavCB.filter(F.act == "back"))
async def _handle_nav_back(cb: CallbackQuery, state: FSMContext) -> None:
    """Шаг назад по стеку навигации."""
    await cb.answer()
    await nav_pop(cb, state)


@nav_router.callback_query(NavCB.filter(F.act == "role_root"))
async def _handle_nav_role_root(cb: CallbackQuery, state: FSMContext) -> None:
    """Возврат в корневое меню роли (admin/master/client)."""
    try:
        await cb.answer()
    except TelegramAPIError as exc:
        logger.debug("nav_role_root: TelegramAPIError while answering callback: %s", exc)
    await nav_role_root(cb, state)


@nav_router.callback_query(NavCB.filter(F.act == "noop"))
async def _handle_nav_noop(cb: CallbackQuery, state: FSMContext) -> None:
    """No-op navigation handler used by UI labels (prevents unhandled callbacks).

    The pagination center label uses a harmless NavCB act='noop' so clicking it
    doesn't produce an error; this handler simply acknowledges the callback.
    """
    try:
        await cb.answer()
    except TelegramAPIError as exc:
        logger.debug("nav_noop: TelegramAPIError while acknowledging noop: %s", exc)
    except Exception:
        logger.exception("nav_noop: unexpected error while acknowledging noop")


__all__.extend(["nav_router"])


# (debug handler removed)


__all__.extend(
    [
        "nav_root",
        "nav_pop",
        "nav_role_root",
    ]
)
