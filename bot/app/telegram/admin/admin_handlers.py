from __future__ import annotations
import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional, Callable, Awaitable, Protocol
from bot.app.telegram.common.callbacks import (
    pack_cb,
    BookingsPageCB,
    DelMasterPageCB,
    ConfirmDelMasterCB,
    ExecDelMasterCB,
    ConfirmCancelAllMasterCB,
    ExecCancelAllMasterCB,
    DelServicePageCB,
    SelectLinkMasterCB,
    SelectLinkServiceCB,
    SelectUnlinkMasterCB,
    SelectUnlinkServiceCB,
    AdminSetHoldCB,
    AdminSetCancelCB,
    AdminSetExpireCB,
    AdminSetReminderCB,
    AdminMenuCB,
    AdminEditSettingCB,
    NavCB,
    ConfirmDelAdminCB,
    ExecDelAdminCB,
    AdminMasterCardCB,
    AdminLookupUserCB,
    ConfirmForceDelMasterCB,
    ExecForceDelMasterCB,
)

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.filters.state import StateFilter
from aiogram.types import CallbackQuery, Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, delete
from sqlalchemy.exc import SQLAlchemyError
from aiogram.exceptions import TelegramAPIError
from datetime import datetime

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, Master, MasterService, Service, User
from bot.app.core.constants import DEFAULT_PAGE_SIZE

# Structural Protocols for typed callback_data without relying on runtime CallbackData classes
class _HasAdminId(Protocol):
    admin_id: int

class _HasServiceId(Protocol):
    service_id: str

class _HasServiceDelta(Protocol):
    service_id: str
    delta: int

class _HasCode(Protocol):
    code: str

class _HasMinutes(Protocol):
    minutes: int

class _HasHour(Protocol):
    hour: int
from bot.app.core.constants import DEFAULT_PAGE_SIZE
from bot.app.services.admin_services import (
    AdminRepo,
    generate_bookings_csv,
    generate_unique_slug_from_name,
    validate_contact_phone,
    validate_instagram_handle,
)
from bot.app.services.shared_services import (
    toggle_telegram_payments,
    format_money_cents,
    get_telegram_provider_token,
    _msg as _shared_msg,
    safe_user_id,
    _safe_call,
    LOCAL_TZ as _shared_local_tz,
    is_telegram_payments_enabled,
    format_user_display_name,
    local_now,
)
from bot.app.telegram.client.client_keyboards import get_back_button
from bot.app.services.admin_services import (
    ServiceRepo,
    SettingsRepo,
    invalidate_services_cache,
)
from bot.app.telegram.admin.states import AdminStates
from bot.app.services.client_services import UserRepo, BookingRepo
import bot.app.services.admin_services as admin_services
from bot.app.services.shared_services import default_language
from bot.app.services.master_services import (
    MasterRepo,
    masters_cache,
    invalidate_masters_cache,
)
import bot.app.services.master_services as master_services
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bot.app.translations import t, tr
from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.roles import ensure_admin, AdminRoleFilter
from bot.app.telegram.admin.admin_keyboards import (
    admin_menu_kb, admin_settings_kb, admin_hold_menu_kb, pagination_kb,
    stats_menu_kb, biz_menu_kb,
    services_list_kb, edit_price_kb,
    admin_cancel_menu_kb, no_masters_kb, no_services_kb,
    masters_list_kb, services_select_kb, contacts_settings_kb,
    confirm_delete_service_kb,
    confirm_delete_master_kb, confirm_cancel_all_master_kb, confirm_force_delete_master_kb,
    admin_reminder_menu_kb,
)
from bot.app.telegram.common.navigation import (
    nav_reset,
    nav_push,
    nav_back,
    nav_replace,
    nav_get_lang,
    show_main_client_menu,
)
# Register centralized error handler for router-level exceptions
from bot.app.telegram.common.errors import handle_telegram_error
# NOTE: Avoid top-level import of client handlers here to prevent import cycles.
# Lazy-import `show_main_menu` inside handlers that need it.
# `get_back_button` was moved to shared services to keep keyboards UI-only.
from aiogram.types import FSInputFile

# Local text dictionary & helpers (static analyzer friendly)
logger = logging.getLogger(__name__)

admin_router = Router(name="admin")
# Attach locale middleware so handlers receive `locale` via data injection
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
admin_router.message.middleware(LocaleMiddleware())
admin_router.callback_query.middleware(LocaleMiddleware())
# Centralized router-level error handler will receive uncaught exceptions
# from handlers and can notify admins, log, etc.
# Error handlers centralized in run_bot.py; per-router registration removed for simplicity.
# Apply AdminRoleFilter at router level so individual handlers don't need to
# perform explicit role checks. The filter delegates to `ensure_admin` which
# sends localized denial messages when access is denied.
admin_router.message.filter(AdminRoleFilter())
# Also filter callback queries so callback handlers are protected as well.
admin_router.callback_query.filter(AdminRoleFilter())
# Access control is enforced by the router-level AdminRoleFilter.

# Local timezone for admin date/time display
LOCAL_TZ = _shared_local_tz or ZoneInfo("Europe/Kyiv")


@admin_router.message(Command("start"))
async def admin_cmd_start(message: Message, state: FSMContext, locale: str) -> None:
    """Handle /start for admins: clear FSM and show admin menu keyboard."""
    # Keep small, local fallbacks but allow unexpected exceptions to bubble
    # to the centralized router error handler registered on the router.
    try:
        await state.clear()
    except Exception:
        # best-effort: ignore state clear failures
        pass

    lang = await _lang_with_state(state, locale)
    kb = admin_menu_kb(lang)
    # reset navigation stack and show admin menu; nav_reset has its own safe guard
    try:
        await nav_reset(state)
    except Exception:
        logger.exception("admin_cmd_start: nav_reset failed")
        raise
    # Use safe_edit which already has internal fallbacks
    await safe_edit(message, text=t("admin_panel_title", lang), reply_markup=kb)



    # Ensure we leave any pending input/edit modes when navigating
    try:
        await state.clear()
    except Exception:
        logger.exception("admin_cmd_start: state.clear failed")
        raise
async def admin_cmd_start_plaintext(message: Message, state: FSMContext, locale: str) -> None:
    await admin_cmd_start(message, state, locale)


# ------------------------------------------------------------------
# Global forwarded-message helper for admins: "fast user lookup"
# Catches any forwarded message (when admin is not in an FSM state) and
# presents a contextual quick-action keyboard for the forwarded user.
# ------------------------------------------------------------------


@admin_router.message(F.forward_from)
async def admin_forwarded_user_lookup(message: Message, state: FSMContext, locale: str) -> None:
    """If an admin forwards any user's message, show a contextual menu.

    The router already applies `AdminRoleFilter()` so only admins reach here.
    We keep this handler tolerant: it only acts when a message contains
    `forward_from` and the admin is not currently inside another FSM flow.
    """
    # Only proceed for forwarded messages that include the original sender
    f = getattr(message, "forward_from", None)
    target_tid = int(getattr(f, "id", 0) or 0)
    if not target_tid:
        return

    username = getattr(f, "username", None)
    first_name = getattr(f, "first_name", None)
    last_name = getattr(f, "last_name", None)
    _remember_forwarded_user_info(target_tid, username, first_name, last_name)

    # Assemble display name
    name_parts = [p for p in (getattr(f, "first_name", None), getattr(f, "last_name", None)) if p]
    username = getattr(f, "username", None)
    display_name = " ".join(name_parts) or ("@" + username if username else str(target_tid))
    try:
        from bot.app.telegram.common.roles import is_admin
        # Use UserRepo for user lookup and masters_cache for master membership
        try:
            user_row = await UserRepo.get_by_telegram_id(target_tid)
        except Exception:
            user_row = None
        try:
            masters_map = await masters_cache()
            is_master_target = int(target_tid) in set(masters_map.keys())
        except Exception:
            is_master_target = False
        # Use roles helpers (env or DB) for admin check
        is_admin_target = await is_admin(target_tid)
    except Exception:
        # On DB/errors, fallback to conservative defaults
        is_admin_target = False
        is_master_target = False

    lang = await _lang_with_state(state, locale)
    # Build contextual status string
    if is_admin_target:
        status_key = "role_admin"
    elif is_master_target:
        status_key = "role_master"
    else:
        status_key = "role_client"
    status = tr(status_key, lang=lang)
    # Localized title with formatting
    text = t("forwarded_user_actions_title", lang).format(name=display_name, id=target_tid, status=status)

    # Build keyboard with quick actions using structured AdminLookupUserCB.
    # Backward compatibility: if packing fails for any reason, fall back to legacy "__fast__" string.
    kb = InlineKeyboardBuilder()
    try:
        kb.button(text=t("make_admin_label", lang), callback_data=pack_cb(AdminLookupUserCB, action="make_admin", user_id=target_tid))
    except Exception:
        kb.button(text=t("make_admin_label", lang), callback_data=f"__fast__:make_admin:{target_tid}")
    if not is_master_target:
        try:
            kb.button(text=t("make_master_label", lang), callback_data=pack_cb(AdminLookupUserCB, action="make_master", user_id=target_tid))
        except Exception:
            kb.button(text=t("make_master_label", lang), callback_data=f"__fast__:make_master:{target_tid}")
    else:
        try:
            kb.button(text=t("view_master_bookings_label", lang), callback_data=pack_cb(AdminLookupUserCB, action="view_master", user_id=target_tid))
        except Exception:
            kb.button(text=t("view_master_bookings_label", lang), callback_data=f"__fast__:view_master:{target_tid}")
    if user_row:
        try:
            kb.button(text=t("view_client_bookings_label", lang), callback_data=pack_cb(AdminLookupUserCB, action="view_client", user_id=target_tid))
        except Exception:
            kb.button(text=t("view_client_bookings_label", lang), callback_data=f"__fast__:view_client:{target_tid}")
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(2, 2)

    # Present as an edited message when possible; otherwise reply
    if m := _get_msg_obj(message):
        # safe_edit already handles its own fallbacks; no need for extra try/except
        ok = await safe_edit(m, text, reply_markup=kb.as_markup())
        if not ok:
            await message.answer(text, reply_markup=kb.as_markup())
    else:
        await message.answer(text, reply_markup=kb.as_markup())


@admin_router.message(F.forward_sender_name & ~F.forward_from)
async def admin_forwarded_privacy_notice(message: Message, state: FSMContext, locale: str) -> None:
    """Handle forwarded messages where the original sender's ID is hidden by privacy settings.

    Telegram supplies `forward_sender_name` when the user forbids linking their account.
    We surface an explicit notice so admins understand why quick actions are unavailable.
    """
    try:
        # Avoid interfering with active FSM flows
        cur_state = await state.get_state()
        if cur_state:
            return
        sender_display = getattr(message, "forward_sender_name", None) or tr("unknown_user", lang=locale)
        lang = await _lang_with_state(state, locale)
        # Localized explanation (fallback English/Ukrainian inline text if key missing)
        try:
            header = t("forward_privacy_header", lang)
            body = t("forward_privacy_body", lang)
        except Exception:
            header = "🔒 Privacy settings"
            body = (
                "Користувач приховав свій Telegram ID у налаштуваннях приватності пересилок. "
                "Адміністратор не може отримати ID з пересланого повідомлення. "
                "Попросіть користувача: (1) написати боту напряму (/start), або (2) надіслати свій @username / ID вручну."
            )
        text = f"{header}\n\n{sender_display}\n\n{body}"
        kb = InlineKeyboardBuilder()
        # Provide a deep-link suggestion button (bot username may be required; leave placeholder if not available)
        bot_username = None
        bot_username = None
        try:
            # aiogram Bot may expose .username (populated after getMe); be defensive
            bot_obj = getattr(message, "bot", None)
            bot_username = getattr(bot_obj, "username", None)
        except Exception:
            bot_username = None
        if bot_username:
            kb.button(text=t("request_user_start_label", lang), url=f"https://t.me/{bot_username}?start=register")
        kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
        kb.adjust(1)
        await message.answer(text, reply_markup=kb.as_markup())
    except Exception as e:
        logger.debug("admin_forwarded_privacy_notice failed: %s", e)


# --------------------------- Внутренние хелперы ---------------------------


def _get_msg_obj(obj: Any) -> Message | None:
    """Return the underlying message object for a callback or message.

    This delegates to the shared helper `_shared_msg` to keep behaviour stable.
    """
    return _shared_msg(obj)

# Cached metadata about recently forwarded users so callbacks can persist names.
@dataclass(frozen=True)
class ForwardedUserInfo:
    username: str | None
    first_name: str | None
    last_name: str | None

_FORWARDED_USER_CACHE_LIMIT = 512
_forwarded_user_info: OrderedDict[int, ForwardedUserInfo] = OrderedDict()


def _remember_forwarded_user_info(tid: int, username: str | None, first_name: str | None, last_name: str | None) -> None:
    if not tid:
        return
    info = ForwardedUserInfo(username=username, first_name=first_name, last_name=last_name)
    _forwarded_user_info[tid] = info
    _forwarded_user_info.move_to_end(tid)
    while len(_forwarded_user_info) > _FORWARDED_USER_CACHE_LIMIT:
        _forwarded_user_info.popitem(last=False)


def _get_forwarded_user_info(tid: int) -> ForwardedUserInfo | None:
    return _forwarded_user_info.get(tid)

# Note: prefer calling _get_msg_obj(obj) directly. The legacy alias _msg was removed
# to encourage consistent usage across the admin module.

# Note: legacy alias `_msg` removed. Use `_get_msg_obj(obj)` to obtain a Message object.


async def _language_default(locale: str | None = None) -> str:
    """Dynamic language fallback using settings or shared default_language()."""
    try:
        if locale:
            return locale
        # Try settings first (may be user-set), fallback to shared env-based default
        from bot.app.services.shared_services import default_language
        lang_setting = await SettingsRepo.get_setting("language", None)
        return str(lang_setting or default_language())
    except Exception:
        try:
            from bot.app.services.shared_services import default_language
            return default_language()
        except Exception:
            return "uk"


async def _lang_with_state(state: FSMContext | None, locale: str | None = None) -> str:
    # Prefer middleware-provided `locale` argument as the canonical source of truth.
    # If `locale` is not provided, fall back to the settings/default language.
    if locale:
        return locale
    return await _language_default(locale)


def _extract_user_id_from_ctx(obj: Any) -> int:
    """Safely extract the Telegram user id from CallbackQuery/Message-like objects."""
    # Prefer direct access so unexpected issues surface during development.
    return int(obj.from_user.id)


# Backwards-compatible no-op decorators so existing handler declarations
# that still use @admin_handler / @admin_safe() remain valid during the
# migration. They intentionally perform no work; locale injection and
# access control are handled by middleware and router filters.
def admin_handler(func):
    return func


def admin_safe(default_reply_markup=None):
    def deco(func):
        return func
    return deco


async def _show_paginated(
    callback: CallbackQuery,
    state: FSMContext,
    total_pages: int,
    title: str,
    prefix: str,
    lang: str = "uk",
    page_items: list[tuple[Any, str]] | None = None,
) -> None:
    """Отображает пагинированный список элементов.

    Args:
        callback: CallbackQuery для отображения.
        state: Контекст FSM с данными пагинации (delete_items, delete_page, delete_type).
        total_pages: Общее количество страниц.
        title: Заголовок списка.
        prefix: Префикс для callback_data кнопок пагинации.
    """
    data = await state.get_data()
    page = int(data.get("delete_page", 1) or 1)
    typ = data.get("delete_type", "item")
    # Accept externally provided page slice to avoid storing full list in FSM.
    paginated = page_items or []
    kb = pagination_kb(prefix, page, total_pages, lang)
    # Map common delete types to typed confirm CallbackData classes
    from bot.app.telegram.common.callbacks import ConfirmDelMasterCB, ConfirmDelServiceCB, GenericConfirmCB
    for key, name in paginated:
        cb_payload: str | None = None
        try:
            if typ == "master":
                cb_payload = pack_cb(ConfirmDelMasterCB, master_id=int(key))
            elif typ == "service":
                cb_payload = pack_cb(ConfirmDelServiceCB, service_id=str(key))
            else:
                cb_payload = pack_cb(GenericConfirmCB, model_type=str(typ), model_id=str(key))
        except Exception:
            logger.exception("_show_paginated: failed to build typed confirm callback for %s/%s", typ, key)
            cb_payload = None  # Skip unsafe legacy fallback
        if cb_payload:
            kb.inline_keyboard.insert(0, [InlineKeyboardButton(text=name, callback_data=cb_payload)])
    await safe_edit(_get_msg_obj(callback), f"{title} ({t('page_short', lang)} {page}/{total_pages}):", reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_admins"))
async def admin_manage_admins(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """List current admins and offer revoke actions (except self)."""
    lang = locale
    # Delegate admin list lookup to AdminRepo
    try:
        rows = await AdminRepo.list_admins()
    except Exception:
        rows = []

    if not rows:
        if m := _get_msg_obj(callback):
            from bot.app.telegram.client.client_keyboards import get_back_button
            await safe_edit(m, t("no_admins", lang) if t("no_admins", lang) != "no_admins" else "No admins found.", reply_markup=get_back_button())
        await callback.answer()
        return

    kb = InlineKeyboardBuilder()
    current_tid = callback.from_user.id if callback.from_user else None
    import os
    primary_admin_tid_raw = os.getenv("PRIMARY_ADMIN_TG_ID", "")
    try:
        primary_admin_tid = int(primary_admin_tid_raw) if primary_admin_tid_raw else None
    except Exception:
        primary_admin_tid = None
    for uid, tid, name in rows:
        label = f"{name} (@{tid})" if name else f"#{tid}"
        # Determine special states
        try:
            is_self = int(tid) == int(current_tid or 0)
        except Exception:
            is_self = False
        try:
            is_primary = primary_admin_tid is not None and int(tid) == int(primary_admin_tid)
        except Exception:
            is_primary = False
        # UI badges: self ✅, primary admin 🛡 (protected)
        if is_primary:
            kb.button(text=f"🛡 {label}", callback_data=pack_cb(NavCB, act="noop"))
        elif is_self:
            kb.button(text=f"✅ {label}", callback_data=pack_cb(NavCB, act="noop"))
        else:
            kb.button(text=f"{label}", callback_data=pack_cb(ConfirmDelAdminCB, admin_id=int(uid)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(1)
    title = t("manage_admins_label", lang) if t("manage_admins_label", lang) != "manage_admins_label" else "Admins"
    if m := _get_msg_obj(callback):
        await nav_push(state, title, kb.as_markup(), lang=lang)
        await safe_edit(m, title, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(ConfirmDelAdminCB.filter())
async def admin_confirm_del_admin(callback: CallbackQuery, callback_data: _HasAdminId, state: FSMContext, locale: str) -> None:
    """Show confirmation to revoke admin rights from a user id (DB id)."""
    lang = locale
    admin_id = int(callback_data.admin_id)
    # Use UserRepo to fetch by DB id
    user = await UserRepo.get_by_id(admin_id)
    if not user:
        await callback.answer(t("not_found", lang), show_alert=True)
        return
    # Build confirm keyboard
    kb = InlineKeyboardBuilder()
    kb.button(text=t("yes", lang), callback_data=pack_cb(ExecDelAdminCB, admin_id=admin_id))
    kb.button(text=t("no", lang), callback_data=pack_cb(AdminMenuCB, act="manage_admins"))
    kb.adjust(2)
    text = (t("confirm_revoke_admin", lang) if t("confirm_revoke_admin", lang) != "confirm_revoke_admin" else f"Revoke admin rights for {user.name}?")
    if m := _get_msg_obj(callback):
        await nav_push(state, text, kb.as_markup(), lang=lang)
        await safe_edit(m, text, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(ExecDelAdminCB.filter())
@admin_handler
@admin_safe()
async def admin_exec_del_admin(callback: CallbackQuery, callback_data: _HasAdminId, state: FSMContext, locale: str) -> None:
    """Revoke admin rights for selected DB user id."""
    lang = locale
    admin_id = int(callback_data.admin_id)
    # Prevent self-revocation
    current_tid = callback.from_user.id if callback.from_user else None
    user = await UserRepo.get_by_id(admin_id)
    if not user:
        await callback.answer(t("not_found", locale), show_alert=True)
        return
    if int(getattr(user, 'telegram_id', 0) or 0) == int(current_tid or 0):
        await callback.answer(t("cannot_revoke_self", locale) if t("cannot_revoke_self", locale) != "cannot_revoke_self" else "You cannot revoke your own admin rights.", show_alert=True)
        return
    ok = await AdminRepo.revoke_admin_by_id(admin_id)
    if not ok:
        await callback.answer(t("error", locale), show_alert=True)
        return
    # Refresh admins list view
    await admin_manage_admins(callback, state, locale)
    logger.info("Admin %s revoked admin rights for user id %s", callback.from_user.id, admin_id)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "analytics"))
async def admin_analytics_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show analytics submenu (quick reports / stats / biz)."""
    lang = locale
    services: list[tuple[str, str]] = []
    from bot.app.telegram.admin.admin_keyboards import analytics_kb
    text = t("admin_analytics_title", lang) if t("admin_analytics_title", lang) != "admin_analytics_title" else (t("analytics", lang) or "Аналитика")
    kb = analytics_kb(lang)
    if m := _get_msg_obj(callback):
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_crud"))
async def admin_manage_crud(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show CRUD management submenu (masters/services/linking/prices)."""
    lang = locale
    masters: list[Any] = []
    from bot.app.telegram.admin.admin_keyboards import management_crud_kb
    text = t("admin_menu_manage_crud", lang) if t("admin_menu_manage_crud", lang) != "admin_menu_manage_crud" else "Управление (CRUD)"
    kb = management_crud_kb(lang)
    if m := _get_msg_obj(callback):
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await callback.answer()



@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_masters"))
async def admin_manage_masters(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Open masters management submenu (Add/Delete/View links)."""
    from bot.app.telegram.admin.admin_keyboards import admin_masters_list_kb
    lang = locale
    # Fetch cached masters mapping {telegram_id: name}
    masters = await master_services.masters_cache()
    kb = admin_masters_list_kb(masters, lang=lang)
    if m := _get_msg_obj(callback):
        await nav_push(state, t("manage_masters_label", lang), kb, lang=lang)
        await safe_edit(m, t("manage_masters_label", lang), reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_services"))
async def admin_manage_services(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Open services management submenu (Add/Delete)."""
    from bot.app.telegram.admin.admin_keyboards import services_crud_kb
    lang = locale
    kb = services_crud_kb(lang)
    if m := _get_msg_obj(callback):
        await nav_push(state, t("manage_services_label", lang), kb, lang=lang)
        await safe_edit(m, t("manage_services_label", lang), reply_markup=kb)
    await callback.answer()



@admin_router.callback_query(AdminMasterCardCB.filter())
async def admin_show_master_card(callback: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Show object-like master card with actions for a selected master."""
    lang = locale
    if not callback_data:
        await callback.answer()
        return
    master_id = int(getattr(callback_data, "master_id", 0) or 0)
    if not master_id:
        await callback.answer()
        return

    # Resolve master display name from cache (best-effort)
    try:
        masters_map = await master_services.masters_cache()
        master_name = masters_map.get(master_id, str(master_id))
    except Exception:
        master_name = str(master_id)

    text = t("admin_master_card_title", lang).format(name=master_name)

    # Try to fetch brief per-master stats (month) and prepend to the card.
    try:
        stats = await AdminRepo.get_range_stats("month", master_id=master_id)
        revenue = await AdminRepo.get_revenue_total("month", master_id=master_id)
        try:
            revenue_fmt = format_money_cents(int(revenue or 0), "UAH")
        except Exception:
            revenue_fmt = f"{int(revenue or 0)}"
        # Build a short stats block
        s_bookings = int(stats.get("bookings", 0) or 0)
        s_unique = int(stats.get("unique_users", 0) or 0)
        try:
            bookings_line = t("admin_dashboard_total_bookings", lang).format(count=s_bookings)
        except Exception:
            bookings_line = f"📈 Всего записей: {s_bookings}"
        try:
            unique_line = f"👤 {t('unique_users', lang)}: {s_unique}"
        except Exception:
            unique_line = f"👤 Унікальних клієнтів: {s_unique}"
        try:
            revenue_line = t("admin_dashboard_revenue", lang).format(amount=revenue_fmt)
        except Exception:
            revenue_line = f"💰 {revenue_fmt}"
        stats_block = "\n".join([bookings_line, revenue_line, unique_line])
        text = f"{stats_block}\n\n{text}"
    except Exception:
        # best-effort: if stats fail, continue without them
        pass

    # Build master-specific action keyboard
    kb = InlineKeyboardBuilder()
    # Show bookings (admin bookings dashboard — may accept master filter)
    kb.button(text=(t("admin_master_bookings_button", lang) if t("admin_master_bookings_button", lang) != "admin_master_bookings_button" else "📅 Записи мастера"), callback_data=pack_cb(AdminMenuCB, act="show_bookings"))
    # View/manage services linked to this master — reuse SelectViewMasterCB which accepts master_id
    from bot.app.telegram.common.callbacks import SelectViewMasterCB
    kb.button(text=(t("admin_master_services_button", lang) if t("admin_master_services_button", lang) != "admin_master_services_button" else "🔗 Управление услугами"), callback_data=pack_cb(SelectViewMasterCB, master_id=int(master_id)))
    # Delete master confirmation
    kb.button(text=(t("admin_menu_delete_master", lang) if t("admin_menu_delete_master", lang) != "admin_menu_delete_master" else "🗑️ Удалить мастера"), callback_data=pack_cb(ConfirmDelMasterCB, master_id=int(master_id)))
    kb.button(text=t("back", lang), callback_data=pack_cb(AdminMenuCB, act="manage_masters"))
    kb.adjust(1)

    try:
        if m := _get_msg_obj(callback):
            await nav_push(state, text, kb.as_markup(), lang=lang)
            await safe_edit(m, text, reply_markup=kb.as_markup())
    except Exception as e:
        logger.exception("admin_show_master_card failed to render master card: %s", e)
        raise
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_links"))
async def admin_manage_links(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Open links management submenu (Link/Unlink/View)."""
    from bot.app.telegram.admin.admin_keyboards import links_crud_kb
    lang = locale
    kb = links_crud_kb(lang)
    if m := _get_msg_obj(callback):
        await nav_push(state, t("manage_links_label", lang), kb, lang=lang)
        try:
            await safe_edit(m, t("manage_links_label", lang), reply_markup=kb)
        except Exception:
            logger.exception("safe_edit failed in admin_manage_links")
            # Let higher-level error handler deal with business errors; only
            # swallow/log Telegram API failures here to avoid breaking UX.
            await safe_edit(m, t("error", locale), reply_markup=admin_menu_kb(locale))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links"))
async def admin_view_links_choice(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Ask admin whether to view links by Master or by Service."""
    lang = locale
    kb = InlineKeyboardBuilder()
    kb.button(text=(t("by_master", lang) if t("by_master", lang) != "by_master" else "По мастеру"), callback_data=pack_cb(AdminMenuCB, act="view_links_master"))
    kb.button(text=(t("by_service", lang) if t("by_service", lang) != "by_service" else "По услуге"), callback_data=pack_cb(AdminMenuCB, act="view_links_service"))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(2, 1)
    m = _get_msg_obj(callback)
    text = (t("admin_view_links_prompt", lang) if t("admin_view_links_prompt", lang) != "admin_view_links_prompt" else "Показать по (Мастеру) или (Услуге)?")
    if m:
        await nav_push(state, text, kb.as_markup(), lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb.as_markup())
        except Exception:
            logger.exception("safe_edit failed in admin_view_links_choice")
            await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links_master"))
async def admin_view_links_by_master(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show list of masters for admin to pick one to view linked services."""
    lang = locale
    # Use cached masters mapping (fast, avoids direct DB access in handlers)
    try:
        masters_map = await master_services.masters_cache()
        masters = [(int(k), v or f"#{k}") for k, v in masters_map.items()]
    except Exception:
        masters = []

    if not masters:
        try:
            await safe_edit(_get_msg_obj(callback), t("no_masters", lang) if t("no_masters", lang) != "no_masters" else "Нет мастеров.", reply_markup=no_masters_kb(lang))
        except Exception:
            logger.exception("safe_edit failed in admin_view_links_by_master (no masters)")
        await callback.answer()
        return
    kb = masters_list_kb(masters, lang=lang)
    text = (t("select_master_to_view_links", lang) if t("select_master_to_view_links", lang) != "select_master_to_view_links" else "Выберите мастера:")
    m = _get_msg_obj(callback)
    if m:
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except Exception:
            logger.exception("safe_edit failed in admin_view_links_by_master")
            await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links_service"))
async def admin_view_links_by_service(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show list of services for admin to pick one to view linked masters."""
    lang = locale
    try:
        services_dict = await ServiceRepo.services_cache()
        services = [(sid, name) for sid, name in services_dict.items()]
    except Exception:
        services = []
    if not services:
        try:
            await safe_edit(_get_msg_obj(callback), t("no_services", lang) if t("no_services", lang) != "no_services" else "Нет услуг.", reply_markup=no_services_kb(lang))
        except Exception:
            logger.exception("safe_edit failed in admin_view_links_by_service (no services)")
        await callback.answer()
        return
    kb = services_select_kb(services, lang=lang)
    text = (t("select_service_to_view_links", lang) if t("select_service_to_view_links", lang) != "select_service_to_view_links" else "Выберите услугу:")
    m = _get_msg_obj(callback)
    if m:
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except Exception:
            logger.exception("safe_edit failed in admin_view_links_by_service")
            await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(lambda q: q.data and q.data.startswith("__fast__:"))
async def admin_fast_user_callback(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Handle quick actions emitted by the forwarded-message quick menu.

    Expected callback_data format: __fast__:action:telegram_id
    Supported actions: make_admin, make_master, view_client, view_master
    """
    data = callback.data or ""
    if not data.startswith("__fast__:"):
        return

    parts = data.split(":", 2)
    if len(parts) < 3:
        await callback.answer()
        return
    _, action, tid_s = parts
    try:
        target_tid = int(tid_s)
    except Exception:
        await callback.answer()
        return

    lang = await _lang_with_state(state, locale)
    forwarded_info = _get_forwarded_user_info(target_tid)
    await _process_admin_lookup_action(action, target_tid, callback, lang, forwarded_user=forwarded_info)


async def _process_admin_lookup_action(
    action: str,
    target_tid: int,
    callback: CallbackQuery,
    lang: str,
    forwarded_user: ForwardedUserInfo | None = None,
) -> None:
    """Shared helper for handling forwarded-user quick actions."""

    msg_obj = _get_msg_obj(callback) or callback.message
    fwd_username = forwarded_user.username if forwarded_user else None
    fwd_first_name = forwarded_user.first_name if forwarded_user else None
    fwd_last_name = forwarded_user.last_name if forwarded_user else None

    if action == "make_admin":
        try:
            ok = await AdminRepo.set_user_admin(
                int(target_tid),
                username=fwd_username,
                first_name=fwd_first_name,
                last_name=fwd_last_name,
            )
            if ok:
                await safe_edit(msg_obj, t("make_admin_label", lang) + f" — OK (ID {target_tid})", reply_markup=admin_menu_kb(lang))
            else:
                logger.warning("AdminRepo.set_user_admin returned False for %s", target_tid)
                await callback.answer(t("error", lang), show_alert=True)
        except Exception:
            logger.exception("Failed to promote user to admin: %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    elif action == "make_master":
        try:
            added = await MasterRepo.add_master(
                int(target_tid),
                None,
                username=fwd_username,
                first_name=fwd_first_name,
                last_name=fwd_last_name,
            )
            if added:
                await safe_edit(msg_obj, t("make_master_label", lang) + f" — OK (ID {target_tid})", reply_markup=admin_menu_kb(lang))
            else:
                await safe_edit(msg_obj, t("make_master_label", lang) + " — already", reply_markup=admin_menu_kb(lang))
        except Exception:
            logger.exception("Failed to create master: %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    elif action == "view_client":
        try:
            user = await UserRepo.get_by_telegram_id(target_tid)
            if not user:
                await safe_edit(msg_obj, t("view_client_bookings_label", lang) + f" — {t('not_found', lang)}", reply_markup=admin_menu_kb(lang))
            else:
                rows = await BookingRepo.recent_by_user(user.id, limit=10)
                if not rows:
                    await safe_edit(msg_obj, t("view_client_bookings_label", lang) + f" — {t('no_bookings', lang)}", reply_markup=admin_menu_kb(lang))
                else:
                    from bot.app.services.shared_services import format_booking_list_item
                    lines: list[str] = []
                    for b in rows:
                        try:
                            txt, _bid = format_booking_list_item(b, role="admin", lang=lang)
                            lines.append(txt)
                        except Exception:
                            continue
                    text = t("view_client_bookings_label", lang) + "\n" + "\n".join(lines)
                    await safe_edit(msg_obj, text, reply_markup=admin_menu_kb(lang))
        except Exception:
            logger.exception("Failed to list client bookings for %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    elif action == "view_master":
        try:
            rows = await BookingRepo.recent_by_master(target_tid, limit=10)
            if not rows:
                await safe_edit(msg_obj, t("view_master_bookings_label", lang) + f" — {t('no_bookings', lang)}", reply_markup=admin_menu_kb(lang))
            else:
                from bot.app.services.shared_services import format_booking_list_item
                lines: list[str] = []
                for b in rows:
                    try:
                        txt, _bid = format_booking_list_item(b, role="admin", lang=lang)
                        lines.append(txt)
                    except Exception:
                        continue
                text = t("view_master_bookings_label", lang) + "\n" + "\n".join(lines)
                await safe_edit(msg_obj, text, reply_markup=admin_menu_kb(lang))
        except Exception:
            logger.exception("Failed to list master bookings for %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    else:
        await callback.answer()


@admin_router.callback_query(AdminLookupUserCB.filter())
async def admin_lookup_user_callback(callback: CallbackQuery, callback_data: CallbackData, state: FSMContext, locale: str) -> None:
    """Handle structured callback_data emitted by forwarded-user quick menu."""
    action = getattr(callback_data, "action", "") or ""
    target_tid = int(getattr(callback_data, "user_id", 0) or 0)
    if not action or not target_tid:
        await callback.answer()
        return

    lang = await _lang_with_state(state, locale)
    forwarded_info = _get_forwarded_user_info(target_tid)
    await _process_admin_lookup_action(action, target_tid, callback, lang, forwarded_user=forwarded_info)

@admin_router.callback_query(lambda q: q.data and q.data.startswith("select_view_master"))
async def admin_show_services_for_master(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Given SelectViewMasterCB, show services linked to the master."""
    lang = locale
    # parse via CallbackData class for safety
    from bot.app.telegram.common.callbacks import SelectViewMasterCB
    if not callback.data:
        await callback.answer()
        return
    payload = SelectViewMasterCB.unpack(str(callback.data))
    master_id = int(getattr(payload, "master_id", 0) or 0)
    if not master_id:
        await callback.answer()
        return
    # use module alias imported at top to avoid circular import issues
    services = await master_services.MasterRepo.get_services_for_master(master_id)  # type: ignore[attr-defined]
    # services: list of tuples (service_id, name)
    if not services:
        text = (t("master_no_services", lang) if t("master_no_services", lang) != "master_no_services" else "У мастера нет привязанных услуг.")
    else:
        try:
            # fetch master name from cache/repo instead of opening a session here
            mname = (await masters_cache()).get(master_id) or str(master_id)
        except Exception:
            mname = str(master_id)
        lines = [f"{mname} привязана к:"]
        for sid, sname in services:
            lines.append(f" - {sname}")
        text = "\n".join(lines)
    from bot.app.telegram.client.client_keyboards import get_back_button
    await safe_edit(_get_msg_obj(callback), text, reply_markup=get_back_button())
    await callback.answer()


@admin_router.callback_query(lambda q: q.data and q.data.startswith("select_view_service"))
async def admin_show_masters_for_service(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Given SelectViewServiceCB, show masters linked to the service."""
    lang = locale
    from bot.app.telegram.common.callbacks import SelectViewServiceCB
    if not callback.data:
        await callback.answer()
        return
    payload = SelectViewServiceCB.unpack(str(callback.data))
    service_id = str(getattr(payload, "service_id", "") or "")
    if not service_id:
        await callback.answer()
        return
    # use module alias imported at top to avoid circular import issues
    masters = await master_services.MasterRepo.get_masters_for_service(service_id)  # type: ignore[attr-defined]
    if not masters:
        text = (t("service_no_masters", lang) if t("service_no_masters", lang) != "service_no_masters" else "Нет мастеров, выполняющих эту услугу.")
    else:
        # masters is list of Master models
        try:
            svc_name = await ServiceRepo.get_service_name(service_id)
        except Exception:
            svc_name = str(service_id)
        lines = [f"Услуга {svc_name} выполняется у:"]
        for m in masters:
            lines.append(f" - {getattr(m, 'name', str(getattr(m, 'telegram_id', '?')))}")
        text = "\n".join(lines)
    await safe_edit(_get_msg_obj(callback), text, reply_markup=get_back_button())
    await callback.answer()


# --------------------- Панель / Выход / Отмена / Тест ----------------------

@admin_router.message(Command("admin"))
async def admin_panel_cmd(message: Message, state: FSMContext, locale: str) -> None:
    """Открывает админ-панель для пользователя с правами администратора.

    Args:
        message: Входящее сообщение с командой /admin.
    """
    
    try:
        # Locale is injected by LocaleMiddleware
        lang = locale
        await nav_reset(state)

        # Build a compact "today" salon summary using a service helper
        try:
            from bot.app.services.admin_services import get_admin_dashboard_summary
            text_root = await get_admin_dashboard_summary(lang=lang)
        except Exception:
            text_root = t("admin_panel_title", lang)

        markup_root = admin_menu_kb(lang)
        # Answer root screen (show full dashboard summary), but store
        # a short canonical title in nav state so navigation comparisons
        # (which expect the title string) continue to work.
        await message.answer(text_root, reply_markup=markup_root)
        # Store canonical title in nav state (not the full text)
        try:
            await nav_replace(state, t("admin_panel_title", lang), markup_root, lang=lang)
        except Exception as e:
            # best-effort: don't fail the handler on nav state update errors
            logger.exception("admin_panel_cmd: nav_replace failed: %s", e)
            raise
        # mark preferred role so role-root nav returns here
        try:
            await state.update_data(preferred_role="admin")
        except Exception as e:
            logger.exception("admin_panel_cmd: state.update_data failed: %s", e)
            raise
        logger.info("Админ-панель открыта для пользователя %s", safe_user_id(message))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_panel_cmd: %s", e)

@admin_router.message(F.text.regexp(r"(?i)^(admin|админ)$"))
async def admin_panel_plaintext(message: Message, state: FSMContext, locale: str) -> None:
    """Plaintext fallback for users typing 'admin' without slash."""
    await admin_panel_cmd(message, state, locale)


@admin_router.callback_query(AdminMenuCB.filter(F.act.in_({"panel", "cancel"})))
async def admin_panel_cb(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Обрабатывает возврат в админ-панель или отмену действия."""
    
    lang = locale
    # If user is already on the admin panel root, treating the 'panel' action
    # as a request to return to the client main menu is convenient for admins
    # who want to leave admin UI quickly. Detect that and delegate to
    # `show_main_menu` (lazy import) instead of re-opening admin panel.
    try:
        data = await state.get_data()
        current_text = data.get("current_text")
        if current_text == t("admin_panel_title", lang):
            # user is already at admin root — return them to client main menu
            try:
                await nav_reset(state)
                await show_main_client_menu(callback, state)
                await callback.answer()
                return
            except Exception:
                logger.debug("show_main_client_menu failed while handling admin panel back")
    except Exception as e:
        logger.exception("admin_panel_cb: failed to read state/current_text: %s", e)
        raise
    await nav_reset(state)
    try:
        m = _get_msg_obj(callback)
        if m and hasattr(m, "edit_text"):
            try:
                await m.edit_text(t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))
                try:
                    await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
                except Exception:
                    logger.debug("nav_replace failed when returning to admin panel")
                try:
                    await state.update_data(preferred_role="admin")
                except Exception as e:
                    logger.exception("admin_panel_cb: state.update_data failed: %s", e)
                    raise
            except Exception as ee:
                if "message is not modified" in str(ee).lower():
                    logger.debug("Ignored 'message is not modified' when returning to admin panel")
                    try:
                        await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
                    except Exception:
                        logger.debug("nav_replace failed after 'message not modified'")
                    try:
                        await state.update_data(preferred_role="admin")
                    except Exception as e:
                        logger.exception("admin_panel_cb (after msg not modified): state.update_data failed: %s", e)
                        raise
                else:
                    logger.debug("Failed to edit admin panel message in-place: %s", ee)
        else:
            await safe_edit(_get_msg_obj(callback), t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))
            try:
                await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
            except Exception:
                logger.debug("nav_replace failed when returning to admin panel in fallback branch")
            try:
                await state.update_data(preferred_role="admin")
            except Exception as e:
                logger.exception("admin_panel_cb (fallback): state.update_data failed: %s", e)
                raise
    except Exception as e:
        logger.exception("Unexpected error while returning to admin panel: %s", e)
    logger.info("Возврат в админ-панель для пользователя %s", callback.from_user.id)
    await callback.answer()


# --------------------- Управление ценами на услуги ---------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_prices"))
async def admin_manage_prices(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    # Use ServiceRepo cache to avoid opening a session in the handler
    try:
        _svc_map = await ServiceRepo.services_cache()
        services = [(sid, name) for sid, name in _svc_map.items()]
    except Exception:
        services = []
    lang = locale
    text = f"{t('manage_prices_title', lang)}\n\n{t('manage_prices_desc', lang)}"
    if m := _get_msg_obj(callback):
        kb = services_list_kb(services, lang)
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await callback.answer()


from bot.app.telegram.common.callbacks import AdminEditPriceCB, AdminSetPriceCB, AdminPriceAdjCB, AdminSetCurrencyCB, ExecDelServiceCB, ConfirmDelServiceCB


@admin_router.callback_query(AdminEditPriceCB.filter())
@admin_handler
@admin_safe()
async def admin_edit_price(callback: CallbackQuery, callback_data: _HasServiceId, state: FSMContext, locale: str) -> None:
    lang = locale
    sid = str(callback_data.service_id)
    svc = await ServiceRepo.get(sid)
    if not svc:
        await callback.answer(t("not_found", lang), show_alert=True)
        return
    price_cents = getattr(svc, 'final_price_cents', None) or getattr(svc, 'price_cents', None) or 0
    currency = getattr(svc, 'currency', None) or 'UAH'
    price_txt = format_money_cents(price_cents, currency)
    text = (f"<b>{svc.name}</b>\n"
            f"ID: <code>{svc.id}</code>\n"
            f"{t('current_price', lang)}: {price_txt}")
    if mmsg := _get_msg_obj(callback):
        kb = edit_price_kb(svc.id, lang)
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(mmsg, text, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminSetPriceCB.filter())
@admin_handler
@admin_safe()
async def admin_set_price(callback: CallbackQuery, callback_data: _HasServiceId, state: FSMContext, locale: str) -> None:
    lang = locale
    sid = str(callback_data.service_id)
    await state.update_data(price_service_id=sid)
    await state.set_state(AdminStates.set_price)
    if msg := _get_msg_obj(callback):
        try:
            from aiogram.utils.keyboard import InlineKeyboardBuilder
            kb = InlineKeyboardBuilder()
            kb.button(text=tr("cancel", lang=lang), callback_data=pack_cb(NavCB, act="back"))
            await msg.answer(t("enter_price", lang), reply_markup=kb.as_markup())
        except Exception:
            await msg.answer(t("enter_price", lang))
    await callback.answer()

@admin_router.callback_query(AdminPriceAdjCB.filter())
@admin_handler
@admin_safe()
async def admin_price_adjust(callback: CallbackQuery, callback_data: _HasServiceDelta, state: FSMContext, locale: str) -> None:
    """Adjust service price by delta (in UAH) via inline stepper.

    Callback data format: admin_price_adj_{service_id}:{delta}
    where delta is integer UAH, can be prefixed with + or -.
    """
    
    lang = locale
    sid = str(callback_data.service_id)
    delta_ua = int(callback_data.delta)
    delta_cents = delta_ua * 100

    # Use centralized service price updater
    # Read current price via repository (no session in handler)
    _svc = await ServiceRepo.get(sid)
    if not _svc:
        await callback.answer(t("not_found", lang), show_alert=True)
        return
    current_cents = getattr(_svc, 'final_price_cents', None) or getattr(_svc, 'price_cents', None) or 0
    new_cents = max(0, current_cents + delta_cents)

    svc = await ServiceRepo.update_price_cents(sid, new_cents)
    if not svc:
        await callback.answer(t("error", lang), show_alert=True)
        return
    currency = getattr(svc, 'currency', None) or 'UAH'
    price_txt = format_money_cents(new_cents, currency)
    text = (f"<b>{svc.name}</b>\n"
            f"ID: <code>{svc.id}</code>\n"
            f"{t('current_price', lang)}: {price_txt}")
    if mmsg := _get_msg_obj(callback):
        kb = edit_price_kb(sid, lang)
        await safe_edit(mmsg, text, reply_markup=kb)
    await callback.answer(t("price_updated", lang))

@admin_router.message(AdminStates.set_price, F.text.regexp(r"^\d{2,6}$"))
@admin_handler
@admin_safe()
async def admin_price_input(message: Message, state: FSMContext, locale: str) -> None:
    data = await state.get_data()
    sid = data.get("price_service_id")
    if not sid:
        return
    lang = locale
    # Validate numeric input explicitly; allow unexpected errors to bubble
    try:
        grn = int(message.text or "0")
    except Exception:
        # Inform user about invalid input and keep state so they can retry
        await message.answer(t("error", lang))
        return

    cents = grn * 100
    svc = await ServiceRepo.update_price_cents(sid, cents)
    if not svc:
        await message.answer(t("error", lang))
        await state.update_data(price_service_id=None)
        return
    await message.answer(t("price_updated", lang))
    await state.clear()


@admin_router.callback_query(AdminSetCurrencyCB.filter())
@admin_handler
@admin_safe()
async def admin_set_currency(callback: CallbackQuery, callback_data: _HasServiceId, state: FSMContext, locale: str) -> None:
    """Open per-service currency picker instead of free-text input."""
    lang = locale
    try:
        sid = str(callback_data.service_id)
        from bot.app.telegram.admin.admin_keyboards import service_currency_picker_kb
        kb = service_currency_picker_kb(sid, lang)
        if msg := _get_msg_obj(callback):
            title = t("choose_currency", lang) if t("choose_currency", lang) != "choose_currency" else "Choose currency"
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(msg, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_set_currency (service) failed: %s", e)
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("admin_set_currency: callback.answer failed: %s", e)
            raise
    await callback.answer()



from bot.app.telegram.common.callbacks import AdminSetGlobalCurrencyCB
from bot.app.telegram.common.callbacks import (
    AdminWorkHoursDayCB,
    AdminWorkHoursStartCB,
    AdminWorkHoursEndCB,
    AdminWorkHoursClosedCB,
)
from bot.app.telegram.common.callbacks import AdminSetWorkStartCB, AdminSetWorkEndCB


@admin_router.callback_query(
    AdminSetGlobalCurrencyCB.filter(),
    ~StateFilter(AdminStates.admin_misc),
)
@admin_handler
@admin_safe()
async def admin_set_global_currency(callback: CallbackQuery, callback_data: _HasCode, state: FSMContext, locale: str) -> None:
    """Persist the selected global currency to Settings (DB-first) with strict whitelist."""
    lang = locale
    code = str(getattr(callback_data, "code", "") or "").upper()
    allowed = {"UAH", "USD", "EUR"}
    if code not in allowed:
        await callback.answer("Invalid currency", show_alert=True)
        return
    saved = False
    try:
        from bot.app.services.admin_services import SettingsRepo
        try:
            ok = await SettingsRepo.update_setting("currency", code)
            saved = bool(ok)
        except TypeError:
            try:
                ok2 = await SettingsRepo.update_setting("currency", code)
                saved = bool(ok2)
            except Exception:
                saved = False
        except Exception:
            saved = False
    except Exception:
        saved = False

    # Removed env fallback: do not mutate os.environ; rely solely on SettingsRepo (DB) for consistency across workers.

    toast = None
    if saved:
        toast = t("currency_saved", lang) if t("currency_saved", lang) != "currency_saved" else "Currency saved"
    else:
        toast = t("error", lang)
    try:
        if saved:
            await callback.answer(toast)
        else:
            await callback.answer(toast, show_alert=True)
    except Exception as e:
        logger.exception("admin_set_global_currency: callback.answer failed: %s", e)
        raise




@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings"))
@admin_handler
@admin_safe()
async def admin_show_settings(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show top-level settings categories to reduce UI clutter."""
    lang = locale
    try:
        from bot.app.telegram.admin.admin_keyboards import settings_categories_kb
        kb = settings_categories_kb(lang)
        title = t("settings_title", lang) if t("settings_title", lang) != "settings_title" else "Settings"
        if m := _get_msg_obj(callback):
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_show_settings failed: %s", e)
        await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()



@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_wizard_start"))
@admin_handler
@admin_safe()
async def admin_settings_wizard_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Start the sequential setup wizard."""
    lang = locale
    try:
        await state.set_state(AdminStates.wizard_phone)
        uid = int(callback.from_user.id)
        logger.info("admin_settings_wizard_start: set state to %s for user=%s", AdminStates.wizard_phone.state, uid)
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        kb.button(text=t("skip", lang) or "Skip", callback_data=pack_cb(AdminMenuCB, act="wizard_skip_phone"))
        kb.button(text=t("cancel", lang) if t("cancel", lang) != "cancel" else "❌", callback_data=pack_cb(AdminMenuCB, act="wizard_cancel"))
        kb.adjust(1, 1)
        prompt = t("wizard_step_phone", lang) or "Enter phone (or Skip)"
        if (m := _get_msg_obj(callback)):
            await nav_push(state, t("wizard_start_title", lang) or "⚙️ Setup Wizard", kb.as_markup(), lang=lang)
            await safe_edit(m, prompt, reply_markup=kb.as_markup())
    except Exception as e:
        logger.exception("wizard_start failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()

@admin_router.callback_query(AdminMenuCB.filter(F.act == "wizard_skip_phone"))
@admin_handler
@admin_safe()
async def admin_wizard_skip_phone(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    await state.set_state(AdminStates.wizard_address)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text=t("skip", lang) or "Skip", callback_data=pack_cb(AdminMenuCB, act="wizard_skip_address"))
    kb.button(text=t("cancel", lang) if t("cancel", lang) != "cancel" else "❌", callback_data=pack_cb(AdminMenuCB, act="wizard_cancel"))
    kb.adjust(1,1)
    prompt = t("wizard_step_address", lang) or "Enter address (or Skip)"
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, prompt, reply_markup=kb.as_markup())
    await callback.answer()

@admin_router.callback_query(AdminMenuCB.filter(F.act == "wizard_skip_address"))
@admin_handler
@admin_safe()
async def admin_wizard_skip_address(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    await state.set_state(AdminStates.wizard_instagram)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text=t("skip", lang) or "Skip", callback_data=pack_cb(AdminMenuCB, act="wizard_skip_instagram"))
    kb.button(text=t("cancel", lang) if t("cancel", lang) != "cancel" else "❌", callback_data=pack_cb(AdminMenuCB, act="wizard_cancel"))
    kb.adjust(1,1)
    prompt = t("wizard_step_instagram", lang)
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, prompt, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "wizard_skip_instagram"))
@admin_handler
@admin_safe()
async def admin_wizard_skip_instagram(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    # Skip final instagram step — finish wizard and show Settings
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_wizard_skip_instagram: state.clear failed: %s", e)
        raise
    try:
        from bot.app.telegram.admin.admin_keyboards import settings_categories_kb
        kb = settings_categories_kb(lang)
        title = t("settings_title", lang) if t("settings_title", lang) != "settings_title" else "Settings"
        if (m := _get_msg_obj(callback)):
            try:
                await nav_push(state, title, kb, lang=lang)
            except Exception as e:
                logger.exception("admin_wizard_skip_instagram: nav_push failed: %s", e)
                raise
            await safe_edit(m, title, reply_markup=kb)
    except Exception:
        logger.exception("admin_wizard_skip_instagram failed")
    await callback.answer()


@dataclass
class WizardTextStep:
    next_state: State | None
    setting_key: str | None
    validator: Callable[[str, str], tuple[Any | None, str | None]]
    prompt_key: str | None
    skip_act: str | None
    show_keep_old_keyboard: bool = False
    post_action: Callable[[Message, FSMContext, str, "WizardTextStep"], Awaitable[None]] | None = None


def _validate_address(value: str, lang: str) -> tuple[str | None, str | None]:
    trimmed = value.strip()
    if not trimmed:
        return None, None
    return trimmed[:300], None


async def _reply_invalid_instagram(message: Message, lang: str) -> None:
    try:
        from bot.app.services.admin_services import SettingsRepo as _SR
        old_val = await _SR.get_setting("contact_instagram", None)
        if not old_val:
            old_val = await _SR.get_setting("instagram", None)
    except Exception:
        old_val = None
    kb = InlineKeyboardBuilder()
    kb.button(text=(t("retry", lang) if t("retry", lang) != "retry" else "Retry"), callback_data=pack_cb(NavCB, act="back"))
    if old_val:
        kb.button(text=(t("keep_old", lang) if t("keep_old", lang) != "keep_old" else f"Keep {old_val}"), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1)
    await message.answer("❌ Invalid Instagram username", reply_markup=kb.as_markup())


async def _wizard_send_next_prompt(message: Message, state: FSMContext, lang: str, step: WizardTextStep) -> None:
    if not step.next_state or not step.prompt_key or not step.skip_act:
        return
    await state.set_state(step.next_state)
    kb = InlineKeyboardBuilder()
    kb.button(text=t("skip", lang) or "Skip", callback_data=pack_cb(AdminMenuCB, act=step.skip_act))
    kb.button(text=t("cancel", lang) if t("cancel", lang) != "cancel" else "❌", callback_data=pack_cb(AdminMenuCB, act="wizard_cancel"))
    kb.adjust(1, 1)
    prompt = t(step.prompt_key, lang)
    await message.answer(prompt or step.prompt_key, reply_markup=kb.as_markup())


async def _wizard_show_currency_picker(message: Message, state: FSMContext, lang: str, step: WizardTextStep) -> None:
    from bot.app.telegram.admin.admin_keyboards import currency_picker_kb
    kb = currency_picker_kb(lang)
    await state.set_state(AdminStates.admin_misc)
    await message.answer(t("wizard_step_currency", lang), reply_markup=kb)


async def _wizard_finish_and_show_settings(message: Message, state: FSMContext, lang: str, step: WizardTextStep) -> None:
    """Finish the wizard and show the admin Settings screen.

    Clears the FSM state and replaces the current UI with the Settings categories.
    """
    try:
        await state.clear()
    except Exception as e:
        logger.exception("_wizard_finish_and_show_settings: state.clear failed: %s", e)
        raise
    try:
        from bot.app.telegram.admin.admin_keyboards import settings_categories_kb
        title = t("settings_title", lang) if t("settings_title", lang) != "settings_title" else "Settings"
        kb = settings_categories_kb(lang)
        # push settings as the current screen (fresh nav after clearing state)
        try:
            await nav_push(state, title, kb, lang=lang)
        except Exception:
            # If nav_push fails, ignore and attempt to at least send the KB
            pass
        try:
            await safe_edit(message, title, reply_markup=kb)
        except Exception:
            try:
                await message.answer(title, reply_markup=kb)
            except Exception:
                logger.exception("_wizard_finish_and_show_settings: failed to show settings")
    except Exception:
        logger.exception("_wizard_finish_and_show_settings failed")


WIZARD_TEXT_STEPS: dict[str | None, WizardTextStep] = {
    AdminStates.wizard_phone.state: WizardTextStep(
        next_state=AdminStates.wizard_address,
        setting_key="contact_phone",
        validator=validate_contact_phone,
        prompt_key="wizard_step_address",
        skip_act="wizard_skip_address",
    ),
    AdminStates.wizard_address.state: WizardTextStep(
        next_state=AdminStates.wizard_instagram,
        setting_key="contact_address",
        validator=_validate_address,
        prompt_key="wizard_step_instagram",
        skip_act="wizard_skip_instagram",
    ),
    AdminStates.wizard_instagram.state: WizardTextStep(
        next_state=None,
        setting_key="contact_instagram",
        validator=validate_instagram_handle,
        prompt_key=None,
        skip_act=None,
        show_keep_old_keyboard=True,
        post_action=_wizard_finish_and_show_settings,
    ),
}


async def _handle_wizard_input(message: Message, state: FSMContext, lang: str, raw: str) -> None:
    current_state = await state.get_state()
    if not current_state:
        return
    step = WIZARD_TEXT_STEPS.get(current_state)
    if not step:
        logger.warning("Wizard step not found for state: %s", current_state)
        return
    value, error_key = step.validator(raw or "", lang)
    if error_key:
        if step.show_keep_old_keyboard:
            await _reply_invalid_instagram(message, lang)
        else:
            error_msg = t(error_key, lang)
            await message.answer(error_msg if error_msg != error_key else "❌ Invalid input")
        return
    if step.setting_key and value is not None:
        try:
            await SettingsRepo.update_setting(step.setting_key, value)
        except Exception as e:
            logger.exception("admin_wizard_text_input: SettingsRepo.update_setting failed: %s", e)
            raise
    action = step.post_action or _wizard_send_next_prompt
    await action(message, state, lang, step)


@admin_router.message(AdminStates.wizard_phone, F.text)
@admin_handler
@admin_safe()
async def admin_wizard_text_input_phone(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    raw = message.text or ""
    try:
        cur = await state.get_state()
    except Exception:
        cur = None
    uid = int(message.from_user.id)
    logger.info("admin_wizard_text_input (phone): user=%s state=%s text=%r", uid, cur, raw)
    await _handle_wizard_input(message, state, lang, raw)


@admin_router.message(AdminStates.wizard_address, F.text)
@admin_handler
@admin_safe()
async def admin_wizard_text_input_address(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    raw = message.text or ""
    try:
        cur = await state.get_state()
    except Exception:
        cur = None
    uid = int(message.from_user.id)
    logger.info("admin_wizard_text_input (address): user=%s state=%s text=%r", uid, cur, raw)
    await _handle_wizard_input(message, state, lang, raw)


@admin_router.message(AdminStates.wizard_instagram, F.text)
@admin_handler
@admin_safe()
async def admin_wizard_text_input_instagram(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    raw = message.text or ""
    try:
        cur = await state.get_state()
    except Exception:
        cur = None
    uid = int(message.from_user.id)
    logger.info("admin_wizard_text_input (instagram): user=%s state=%s text=%r", uid, cur, raw)
    await _handle_wizard_input(message, state, lang, raw)





@admin_router.message(AdminStates.wizard_phone, F.contact)
@admin_handler
@admin_safe()
async def admin_wizard_contact_input(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    raw = (getattr(message.contact, "phone_number", "") or "")
    await _handle_wizard_input(message, state, lang, raw)

@admin_router.callback_query(AdminSetGlobalCurrencyCB.filter())
@admin_handler
@admin_safe()
async def admin_wizard_currency_pick(callback: CallbackQuery, callback_data: _HasCode, state: FSMContext, locale: str) -> None:
    # Intercept currency selection when wizard active (state admin_misc after instagram step)
    data = await state.get_data()
    lang = locale
    # Determine if wizard is active by absence of a flag wizard_done
    if data.get("wizard_done"):
        return  # normal handler already processed earlier
    code = getattr(callback_data, "code", "")
    if code and code.upper() in {"UAH", "EUR", "USD"}:
        try:
            await SettingsRepo.update_setting("currency", code.upper())
        except Exception as e:
            logger.exception("admin_wizard_currency_pick: SettingsRepo.update_setting failed: %s", e)
            raise
    # Move directly to picking working hours start
    from bot.app.telegram.admin.admin_keyboards import work_hours_start_kb
    kb = work_hours_start_kb(lang)
    await state.set_state(AdminStates.wizard_hours_start)
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, t("wizard_step_hours_start", lang), reply_markup=kb)
    await callback.answer()

@admin_router.callback_query(AdminSetWorkStartCB.filter())
@admin_handler
@admin_safe()
async def admin_wizard_hours_start(callback: CallbackQuery, callback_data: _HasHour, state: FSMContext, locale: str) -> None:
    lang = locale
    cur_state = await state.get_state()
    if "wizard_hours_start" not in str(cur_state):
        return
    start = int(getattr(callback_data, "hour", 0) or 0)
    if start < 0 or start > 23:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    await state.update_data(wizard_hours_start=start)
    from bot.app.telegram.admin.admin_keyboards import work_hours_end_kb
    kb = work_hours_end_kb(lang, start)
    await state.set_state(AdminStates.wizard_hours_end)
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, t("wizard_step_hours_end", lang), reply_markup=kb)
    await callback.answer()

@admin_router.callback_query(AdminSetWorkEndCB.filter())
@admin_handler
@admin_safe()
async def admin_wizard_hours_end(callback: CallbackQuery, callback_data: _HasHour, state: FSMContext, locale: str) -> None:
    lang = locale
    cur_state = await state.get_state()
    if "wizard_hours_end" not in str(cur_state):
        return
    end = int(getattr(callback_data, "hour", 0) or 0)
    data = await state.get_data()
    start = int(data.get("wizard_hours_start") or -1)
    if start < 0 or end <= start:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    # Persist hours
    try:
        await SettingsRepo.update_setting("work_hours_start", start)
        await SettingsRepo.update_setting("work_hours_end", end)
    except Exception as e:
        logger.exception("admin_wizard_hours_end: failed to persist work hours: %s", e)
        raise
    await state.update_data(wizard_hours_end=end)
    # Summary step
    phone = await SettingsRepo.get_setting("contact_phone", None)
    address = await SettingsRepo.get_setting("contact_address", None)
    instagram = await SettingsRepo.get_setting("contact_instagram", None)
    currency = await SettingsRepo.get_currency()
    slot = await SettingsRepo.get_slot_duration()
    summary = (
        f"<b>{t('wizard_summary_title', lang)}</b>\n"
        f"📞 {phone or '-'}\n"
        f"📍 {address or '-'}\n"
        f"📷 {instagram or '-'}\n"
        f"💱 {currency} | ⏱ {slot} {t('minutes_short', lang)}\n"
        f"🕘 {start:02d}:00–{end:02d}:00"
    )
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    kb.button(text=t("confirm", lang), callback_data=pack_cb(AdminMenuCB, act="wizard_confirm"))
    kb.button(text=t("cancel", lang) if t("cancel", lang) != "cancel" else "❌", callback_data=pack_cb(AdminMenuCB, act="wizard_cancel"))
    kb.adjust(1,1)
    await state.update_data(wizard_done=False)
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, summary, reply_markup=kb.as_markup(), parse_mode="HTML")
    await callback.answer()

@admin_router.callback_query(AdminMenuCB.filter(F.act == "wizard_confirm"))
@admin_handler
@admin_safe()
async def admin_wizard_confirm(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    await state.update_data(wizard_done=True)
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_wizard_confirm: state.clear failed: %s", e)
        raise
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, t("wizard_finish_success", lang) or "Settings saved!")
    await callback.answer()

@admin_router.callback_query(AdminMenuCB.filter(F.act == "wizard_cancel"))
@admin_handler
@admin_safe()
async def admin_wizard_cancel(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_wizard_cancel: state.clear failed: %s", e)
        raise
    if (m := _get_msg_obj(callback)):
        await safe_edit(m, t("action_cancelled", lang) or "Cancelled")
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_contacts"))
@admin_handler
@admin_safe()
async def admin_settings_contacts(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Contacts submenu: phone, address, Instagram."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo
        address = await SettingsRepo.get_setting("contact_address", None)
        instagram = await SettingsRepo.get_setting("contact_instagram", None)
        phone = await SettingsRepo.get_setting("contact_phone", None)
        from bot.app.telegram.admin.admin_keyboards import contacts_settings_kb
        kb = contacts_settings_kb(lang, phone=phone, address=address, instagram=instagram)
        if m := _get_msg_obj(callback):
            await nav_push(state, t("settings_category_contacts", lang) or "Contacts", kb, lang=lang)
            await safe_edit(m, t("settings_category_contacts", lang) or "Contacts", reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_contacts failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@dataclass(frozen=True)
class EditableSettingMeta:
    prompt_key: str
    success_key: str
    validator: Callable[[str, str], tuple[str | None, str | None]]
    invalid_key: str | None


EDITABLE_CONTACT_SETTINGS: dict[str, EditableSettingMeta] = {
    "contact_phone": EditableSettingMeta(
        prompt_key="enter_phone",
        success_key="phone_updated",
        validator=validate_contact_phone,
        invalid_key="invalid_phone",
    ),
    "contact_address": EditableSettingMeta(
        prompt_key="enter_address",
        success_key="address_updated",
        validator=_validate_address,
        invalid_key="invalid_address",
    ),
    "contact_instagram": EditableSettingMeta(
        prompt_key="enter_instagram",
        success_key="instagram_updated",
        validator=validate_instagram_handle,
        invalid_key="invalid_instagram",
    ),
}


async def _reply_invalid_setting_input(message: Message, lang: str, invalid_key: str | None, old_value: str | None) -> None:
    text = t(invalid_key or "invalid_data", lang)
    try:
        kb = InlineKeyboardBuilder()
        retry_label = t("retry", lang)
        kb.button(text=(retry_label if retry_label != "retry" else "Retry"), callback_data=pack_cb(NavCB, act="back"))
        if old_value:
            keep_label = t("keep_old", lang)
            kb.button(text=f"{keep_label if keep_label != 'keep_old' else 'Keep old'} {old_value}", callback_data=pack_cb(NavCB, act="back"))
        kb.adjust(1, 1)
        await message.answer(text, reply_markup=kb.as_markup())
    except Exception:
        await message.answer(text)


async def _refresh_contacts_menu(message: Message, lang: str) -> None:
    try:
        phone = await SettingsRepo.get_setting("contact_phone", None)
        address = await SettingsRepo.get_setting("contact_address", None)
        instagram = await SettingsRepo.get_setting("contact_instagram", None)
        kb = contacts_settings_kb(lang, phone=phone, address=address, instagram=instagram)
        await message.answer(t("settings_category_contacts", lang) or "Contacts", reply_markup=kb)
    except Exception as e:
        logger.exception("_refresh_contacts_menu: failed to refresh contacts menu: %s", e)
        raise


@admin_router.callback_query(AdminEditSettingCB.filter())
@admin_handler
@admin_safe()
async def admin_edit_contact_setting(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    setting_key = str(getattr(callback_data, "setting_key", "") or "")
    meta = EDITABLE_CONTACT_SETTINGS.get(setting_key)
    if not meta:
        await callback.answer(t("error", lang), show_alert=True)
        return
    try:
        current_value = await SettingsRepo.get_setting(setting_key, None)
    except Exception:
        current_value = None
    try:
        await state.update_data(edit_setting_key=setting_key, edit_setting_old=current_value)
    except Exception as e:
        logger.exception("admin_edit_contact_setting: failed to update FSM data: %s", e)
        raise
    await state.set_state(AdminStates.edit_setting_text)
    prompt = t(meta.prompt_key, lang)
    try:
        kb = InlineKeyboardBuilder()
        kb.button(text=tr("cancel", lang=lang), callback_data=pack_cb(NavCB, act="back"))
        kb.adjust(1)
        if m := _get_msg_obj(callback):
            await m.answer(prompt, reply_markup=kb.as_markup())
        elif callback.message:
            await callback.message.answer(prompt, reply_markup=kb.as_markup())
    except Exception:
        if m := _get_msg_obj(callback):
            await m.answer(prompt)
        elif callback.message:
            await callback.message.answer(prompt)
    await callback.answer()


@admin_router.message(AdminStates.edit_setting_text, F.text)
@admin_handler
@admin_safe()
async def admin_edit_setting_input(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    data = await state.get_data() or {}
    setting_key = str(data.get("edit_setting_key") or "")
    if not setting_key:
        return
    meta = EDITABLE_CONTACT_SETTINGS.get(setting_key)
    if not meta:
        return
    raw = message.text or ""
    value, error_key = meta.validator(raw, lang)
    if value is None or error_key:
        await _reply_invalid_setting_input(message, lang, error_key or meta.invalid_key, str(data.get("edit_setting_old")) if data.get("edit_setting_old") else None)
        return
    saved = False
    try:
        saved = await SettingsRepo.update_setting(setting_key, value)
    except Exception:
        saved = False
    if saved:
        await message.answer(t(meta.success_key, lang))
    else:
        await message.answer(t("error", lang))
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_settings_business: failed to render business settings: %s", e)
        raise
    await _refresh_contacts_menu(message, lang)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_business"))
@admin_handler
@admin_safe()
async def admin_settings_business(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Business submenu: payments state, hold/cancel menus."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo
        telegram_provider_token = await get_telegram_provider_token() or ""
        payments_enabled = await is_telegram_payments_enabled()
        hold_min = await SettingsRepo.get_reservation_hold_minutes()
        cancel_h = await SettingsRepo.get_client_cancel_lock_hours()
        reminder_min = await SettingsRepo.get_reminder_lead_minutes()
        timezone_val = await SettingsRepo.get_setting("timezone", "UTC")
        from bot.app.telegram.admin.admin_keyboards import business_settings_kb
        kb = business_settings_kb(lang, telegram_provider_token=telegram_provider_token, payments_enabled=payments_enabled, hold_min=hold_min, cancel_h=cancel_h, reminder_min=reminder_min, timezone=timezone_val)
        if m := _get_msg_obj(callback):
            await nav_push(state, t("settings_category_business", lang) or "Business", kb, lang=lang)
            await safe_edit(m, t("settings_category_business", lang) or "Business", reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_business failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_timezone"))
@admin_handler
@admin_safe()
async def admin_settings_timezone(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show timezone picker keyboard."""
    lang = locale
    try:
        from bot.app.telegram.admin.admin_keyboards import timezone_picker_kb
        kb = timezone_picker_kb(lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, t("settings_timezone_title", lang) or "Timezone", kb, lang=lang)
            await safe_edit(m, t("settings_timezone_title", lang) or "Timezone", reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_timezone failed: %s", e)
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("admin_settings_timezone: callback.answer failed: %s", e)
            raise
    await callback.answer()
from bot.app.telegram.common.callbacks import AdminSetTimezoneCB


@admin_router.callback_query(AdminSetTimezoneCB.filter())
@admin_handler
@admin_safe()
async def admin_set_timezone(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Persist selected timezone to settings."""
    lang = locale
    tz = str(getattr(callback_data, "tz", "") or "")
    if not tz:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    # Whitelist check (must match our keyboard)
    allowed = {"UTC", "Europe/Kyiv", "Europe/Moscow", "Europe/Warsaw", "Europe/Berlin", "Asia/Kiev", "Europe/London", "Europe/Paris", "America/New_York", "Asia/Tbilisi"}
    if tz not in allowed:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    saved = False
    try:
        saved = bool(await SettingsRepo.update_setting("timezone", tz))
    except Exception:
        saved = False
    if saved:
        try:
            await callback.answer(t("timezone_saved", lang) if t("timezone_saved", lang) != "timezone_saved" else "Timezone saved")
        except Exception as e:
            logger.exception("admin_set_timezone: callback.answer failed: %s", e)
            raise
        # After saving, return to business settings menu
        try:
            timezone_val = tz
            from bot.app.telegram.admin.admin_keyboards import business_settings_kb
            telegram_provider_token = await get_telegram_provider_token() or ""
            payments_enabled = await is_telegram_payments_enabled()
            hold_min = await SettingsRepo.get_reservation_hold_minutes()
            cancel_h = await SettingsRepo.get_client_cancel_lock_hours()
            reminder_min = await SettingsRepo.get_reminder_lead_minutes()
            kb = business_settings_kb(lang, telegram_provider_token=telegram_provider_token, payments_enabled=payments_enabled, hold_min=hold_min, cancel_h=cancel_h, reminder_min=reminder_min, timezone=timezone_val)
            if m := _get_msg_obj(callback):
                try:
                    await nav_replace(state, t("settings_category_business", lang) or "Business", kb, lang=lang)
                    await safe_edit(m, t("settings_category_business", lang) or "Business", reply_markup=kb)
                except Exception as e:
                    logger.exception("admin_set_timezone: nav_replace/safe_edit failed: %s", e)
                    raise
        except Exception as e:
            logger.exception("admin_set_timezone: preparing business settings failed: %s", e)
            raise
    else:
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("admin_set_timezone: callback.answer on error failed: %s", e)
            raise


@admin_router.callback_query(AdminSetWorkStartCB.filter())
@admin_handler
@admin_safe()
async def admin_set_work_start(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    try:
        start = int(getattr(callback_data, "hour", 0) or 0)
        # Push end-hour picker, store start in FSM so AdminSetWorkEndCB has context
        await state.update_data(work_hours_start=start)
        from bot.app.telegram.admin.admin_keyboards import work_hours_end_kb
        kb = work_hours_end_kb(lang, start_hour=start)
        title = t("pick_work_hours_end", lang) if t("pick_work_hours_end", lang) != "pick_work_hours_end" else "Pick end hour"
        if m := _get_msg_obj(callback):
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_set_work_start failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@admin_router.callback_query(AdminSetWorkEndCB.filter())
@admin_handler
@admin_safe()
async def admin_set_work_end(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    try:
        start = int(getattr(callback_data, "start", 0) or 0)
        end = int(getattr(callback_data, "hour", 0) or 0)
        if end <= start:
            await callback.answer(t("invalid_data", lang), show_alert=True)
            return
        # Persist to settings
        saved = False
        try:
            from bot.app.services.admin_services import SettingsRepo
            ok = await SettingsRepo.update_setting("work_hours_start", int(start))
            ok2 = await SettingsRepo.update_setting("work_hours_end", int(end))
            saved = bool(ok and ok2)
        except Exception:
            saved = False

        # Removed env fallback (WORK_HOURS_START/END); rely solely on SettingsRepo.

        if saved:
            try:
                await callback.answer(t("hours_saved", lang) if t("hours_saved", lang) != "hours_saved" else f"Working hours saved")
            except Exception as e:
                logger.exception("admin_set_work_end: callback.answer failed: %s", e)
                raise
        else:
            await callback.answer(t("error", lang), show_alert=True)
    except Exception as e:
        logger.exception("admin_set_work_end failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_reminder"))
@admin_handler
@admin_safe()
async def admin_settings_reminder(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show reminder lead-time selection menu."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo
        rem = await SettingsRepo.get_reminder_lead_minutes()
        kb = admin_reminder_menu_kb(lang)
        base_title = t("settings_reminder_title", lang) if t("settings_reminder_title", lang) != "settings_reminder_title" else "Reminder time"
        title = f"{base_title}\n\n{t('settings_reminder_desc', lang)}"
        if m := _get_msg_obj(callback):
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_reminder failed: %s", e)
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("admin_settings_reminder: callback.answer failed: %s", e)
            raise
    await callback.answer()


@admin_router.callback_query(AdminSetReminderCB.filter())
@admin_handler
@admin_safe()
async def admin_set_reminder(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Persist selected reminder lead-time (minutes)."""
    lang = locale
    try:
        minutes = int(getattr(callback_data, "minutes", 0) or 0)
    except Exception:
        minutes = 0
    if minutes <= 0:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    saved = False
    try:
        from bot.app.services.admin_services import SettingsRepo
        saved = bool(await SettingsRepo.update_setting("reminder_lead_minutes", int(minutes)))
    except Exception:
        saved = False
    try:
        if saved:
            await callback.answer(t("reminder_saved", lang) if t("reminder_saved", lang) != "reminder_saved" else "Reminder saved")
        else:
            await callback.answer(t("error", lang), show_alert=True)
    except Exception as e:
        logger.exception("admin_settings_work_hours: safe_edit failed: %s", e)
        raise

    # Return to Business settings menu to reflect updated value
    try:
        await admin_settings_business(callback, state, locale)
    except Exception as e:
        logger.exception("admin_settings_work_hours: callback.answer failed: %s", e)
        raise
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_work_hours"))
@admin_handler
@admin_safe()
async def admin_settings_work_hours(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Disabled: working hours now configured by masters individually."""
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_days: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_days: callback.answer failed: %s", e)
        raise


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_work_hours_days"))
@admin_handler
@admin_safe()
async def admin_work_hours_days(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Disabled: working hours now configured by masters individually."""
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_day_pick: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_day_pick: callback.answer failed: %s", e)
        raise


@admin_router.callback_query(AdminWorkHoursDayCB.filter())
@admin_handler
@admin_safe()
async def admin_work_hours_day_pick(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_start: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_start: callback.answer failed: %s", e)
        raise


@admin_router.callback_query(AdminWorkHoursStartCB.filter())
@admin_handler
@admin_safe()
async def admin_work_hours_start(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_end: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_end: callback.answer failed: %s", e)
        raise


@admin_router.callback_query(AdminWorkHoursEndCB.filter())
@admin_handler
@admin_safe()
async def admin_work_hours_end(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_closed: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_closed: callback.answer failed: %s", e)
        raise


@admin_router.callback_query(AdminWorkHoursClosedCB.filter())
@admin_handler
@admin_safe()
async def admin_work_hours_closed(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    lang = locale
    msg = t("work_hours_admin_disabled", lang) if t("work_hours_admin_disabled", lang) != "work_hours_admin_disabled" else "Настройка рабочих часов перенесена к мастерам"
    try:
        if (m := _get_msg_obj(callback)):
            await safe_edit(m, msg, reply_markup=None)
    except Exception as e:
        logger.exception("admin_work_hours_closed: safe_edit failed: %s", e)
        raise
    try:
        await callback.answer()
    except Exception as e:
        logger.exception("admin_work_hours_closed: callback.answer failed: %s", e)
        raise




from bot.app.telegram.common.callbacks import AdminSetServiceCurrencyCB


@admin_router.callback_query(AdminSetServiceCurrencyCB.filter())
@admin_handler
@admin_safe()
async def admin_set_service_currency(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Persist per-service currency via picker (UAH/EUR/USD)."""
    lang = locale
    sid = str(getattr(callback_data, "service_id", "") or "")
    code = str(getattr(callback_data, "code", "") or "").upper()
    if not sid or code not in {"UAH", "USD", "EUR"}:
        await callback.answer(t("invalid_data", lang), show_alert=True)
        return
    ok = await ServiceRepo.update_currency(sid, code)
    if ok:
        try:
            # Refresh the price edit view so admin sees updated currency immediately.
            mobj = _get_msg_obj(callback) or callback.message
            if mobj:
                # Re-fetch service and re-render the edit view
                svc = await ServiceRepo.get(sid)
                try:
                    if svc:
                        price_cents = getattr(svc, 'final_price_cents', None) or getattr(svc, 'price_cents', None) or 0
                        currency = getattr(svc, 'currency', None) or 'UAH'
                        price_txt = format_money_cents(price_cents, currency)
                        text = (f"<b>{svc.name}</b>\n"
                                f"ID: <code>{svc.id}</code>\n"
                                f"{t('current_price', lang)}: {price_txt}")
                        from bot.app.telegram.admin.admin_keyboards import edit_price_kb
                        kb = edit_price_kb(svc.id, lang)
                        await safe_edit(mobj, text, reply_markup=kb)
                        # Notify with localized confirmation
                        try:
                            await callback.answer(t("service_currency_updated", lang), show_alert=False)
                        except Exception as e:
                            logger.exception("admin_set_currency (refresh view): callback.answer failed: %s", e)
                            raise
                    else:
                        # If service vanished, fallback to a simple confirmation
                        await mobj.answer(t("service_currency_updated", lang))
                except Exception:
                    # On any edit failure, at least notify admin about success
                    try:
                        await mobj.answer(t("service_currency_updated", lang))
                    except Exception as e:
                        logger.exception("admin_set_currency (fallback notify): mobj.answer failed: %s", e)
                        raise
        except Exception as e:
            logger.exception("admin_set_currency: error while refreshing price view: %s", e)
            raise
    else:
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "exit"))
async def admin_exit(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Закрывает админ-панель и очищает состояние.

    Args:
        callback: CallbackQuery для выхода.
        state: Контекст FSM для очистки состояния.
    """
    
    await state.clear()
    lang = locale
    try:
        await safe_edit(
            _get_msg_obj(callback),
            t("exit_message", lang),
            reply_markup=None,
        )
    except Exception:
        # Best-effort: if edit fails, try to send a simple message via bot
        try:
            bot = getattr(callback, 'bot', None)
            if bot:
                await bot.send_message(callback.from_user.id, t("exit_message", lang))
        except Exception:
            pass
    logger.info("Выход из админ-панели для пользователя %s", callback.from_user.id)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "test"))
async def admin_test_button(callback: CallbackQuery, locale: str) -> None:
    """Тестовая кнопка для проверки работоспособности.

    Args:
        callback: CallbackQuery для теста.
    """
    
    try:
        lang = locale
        await callback.answer(t("test_ok", lang), show_alert=True)
        logger.info("Тестовая кнопка нажата пользователем %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_test_button: %s", e)


# --------------------------- Управление записями ---------------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "show_bookings"))
async def admin_show_bookings(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню фильтров для просмотра записей.

    Args:
        callback: CallbackQuery для отображения меню.
    """
    # Show the bookings dashboard immediately (same UI as master), rather
    # than the old filter screen. This unifies admin/master UIs.
    if m := _get_msg_obj(callback):
        lang = locale
        text, kb = await _build_admin_bookings_view(state, lang, mode="upcoming", page=1)
        # persist current mode/page in state
        try:
            await state.update_data(bookings_mode="upcoming", bookings_page=1)
        except Exception as e:
            logger.exception("admin_show_bookings: state.update_data(bookings_mode) failed: %s", e)
            raise
        # Ensure role hint is set so NavCB(role_root) returns to admin panel
        try:
            await state.update_data(preferred_role="admin")
        except Exception as e:
            logger.exception("admin_show_bookings: state.update_data(preferred_role) failed: %s", e)
            raise
        try:
            await nav_replace(state, text, kb)
        except Exception as e:
            try:
                await nav_replace(state, text, kb, lang=lang)
            except Exception as e2:
                logger.exception("admin_show_bookings: nav_replace failed (both attempts): %s / %s", e, e2)
                raise
        try:
            ok = await safe_edit(m, text=text, reply_markup=kb)
            if not ok:
                msg_obj = getattr(callback, 'message', None)
                if msg_obj is not None and hasattr(msg_obj, 'answer'):
                    new_msg = await msg_obj.answer(text, reply_markup=kb)
                    try:
                        bot_instance = getattr(msg_obj, 'bot', None)
                        if bot_instance is not None:
                            await bot_instance.delete_message(chat_id=msg_obj.chat.id, message_id=msg_obj.message_id)
                    except Exception as e:
                        logger.exception("admin_show_bookings: bot_instance.delete_message failed: %s", e)
                        raise
        except Exception:
            logger.exception("force redraw failed in admin_show_bookings")
    logger.info("Дашборд записей показан для пользователя %s", callback.from_user.id)
    await callback.answer()
    logger.info("Дашборд записей показан для пользователя %s", callback.from_user.id)
    await callback.answer()


from bot.app.telegram.common.callbacks import AdminBookingsCB
from bot.app.telegram.common.callbacks import NavCB


@admin_router.callback_query(NavCB.filter())
@admin_handler
@admin_safe()
async def admin_nav_clear_state(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Clear FSM on any admin navigation action to avoid input traps, then route.

    This handler mirrors the navigation helpers and should be kept lightweight.
    """
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_nav_clear_state: state.clear failed: %s", e)
        raise
    act = getattr(callback_data, "act", None)
    from bot.app.telegram.common.navigation import nav_root, nav_pop, nav_role_root
    if act == "root":
        await nav_root(callback, state)
    elif act == "back":
        await nav_pop(callback, state)
    elif act == "role_root":
        await nav_role_root(callback, state)
    else:
        # default to role_root for unknown actions
        await nav_role_root(callback, state)
    await callback.answer()


@admin_router.callback_query(AdminBookingsCB.filter())
@admin_router.callback_query(BookingsPageCB.filter())
async def admin_bookings_navigate(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Handle admin bookings tab changes and pagination (combined).

    Accepts AdminBookingsCB (mode switch) and BookingsPageCB (pagination).
    """
    lang = locale
    mode = getattr(callback_data, "mode", None)
    page = getattr(callback_data, "page", None)
    if page is not None:
        data = await state.get_data() or {}
        mode = data.get("bookings_mode", mode or "all")
        try:
            page = int(page)
        except Exception:
            page = int(data.get("bookings_page", 1) or 1)
        await state.update_data(bookings_page=page)
    elif mode is not None:
        page = 1
        await state.update_data(bookings_mode=mode, bookings_page=1)
    else:
        data = await state.get_data() or {}
        mode = data.get("bookings_mode", "all")
        try:
            page = int(data.get("bookings_page", 1) or 1)
        except Exception:
            page = 1

    text, kb = await _build_admin_bookings_view(state, lang, mode=mode, page=int(page or 1))
    if callback.message:
        await safe_edit(callback.message, text=text, reply_markup=kb)
    logger.info("Admin bookings navigate: user=%s mode=%s page=%s", getattr(callback.from_user, 'id', None), mode, page)
    await callback.answer()


async def _build_admin_bookings_view(state: FSMContext, lang: str, mode: str, page: int) -> tuple[str, InlineKeyboardMarkup | None]:
    """Fetch admin bookings data, build dynamic header and keyboard.

    Returns (text, markup) where text is dynamic header string and markup is InlineKeyboardMarkup.
    """
    from bot.app.services.admin_services import ServiceRepo
    from bot.app.services.shared_services import format_booking_list_item
    from bot.app.telegram.client.client_keyboards import build_my_bookings_keyboard
    from aiogram.types import InlineKeyboardMarkup

    rows, meta = await ServiceRepo.get_admin_bookings(mode=mode or "upcoming", page=int(page or 1), page_size=DEFAULT_PAGE_SIZE)
    # Format bookings inline using shared formatter (admin role)
    formatted_rows: list[tuple[str,int]] = []
    for r in rows:
        try:
            txt, bid = format_booking_list_item(r, role="admin", lang=lang)
            formatted_rows.append((txt, bid))
        except Exception:
            continue
    text_base = t("bookings_title", lang)
    try:
        meta = meta or {}
        mode_for_header = mode or "upcoming"
        mode_map = {
            "upcoming": (t("upcoming", lang), int(meta.get('upcoming_count', 0) or 0)),
            "done": (t("done_bookings", lang), int(meta.get('done_count', 0) or 0)),
            "cancelled": (t("cancelled_bookings", lang), int(meta.get('cancelled_count', 0) or 0)),
            "no_show": (t("no_show_bookings", lang), int(meta.get('noshow_count', 0) or 0)),
            "all": (t("all_bookings", lang), int(meta.get('total', 0) or 0)),
        }
        tab_name, tab_count = mode_map.get(mode_for_header, mode_map["upcoming"])
        page_val = int(meta.get('page', 1) or 1)
        total_pages = int(meta.get('total_pages', 1) or 1)
        try:
            dynamic_header = f"{tab_name} ({int(tab_count or 0)})"
            if total_pages > 1:
                dynamic_header += f" ({t('page_short', lang)} {page_val}/{total_pages})"
        except Exception:
            dynamic_header = str(tab_name or "")
    except Exception:
        dynamic_header = text_base

    total_pages = int(meta.get("total_pages", 1))
    completed_count = (
        int(meta.get('done_count', 0) or 0)
        + int(meta.get('cancelled_count', 0) or 0)
        + int(meta.get('noshow_count', 0) or 0)
    )
    kb = await build_my_bookings_keyboard(
        formatted_rows,
        int(meta.get('upcoming_count', 0)),
        completed_count,
        mode or "upcoming",
        int(meta.get('page', 1)),
        lang,
        items_per_page=DEFAULT_PAGE_SIZE,
        cancelled_count=int(meta.get('cancelled_count', 0)),
        noshow_count=int(meta.get('noshow_count', 0)),
        total_pages=total_pages,
        current_page=int(meta.get('page', 1)),
        role="admin",
    )
    return dynamic_header, kb


@admin_router.callback_query(AdminMenuCB.filter(F.act == "export_csv"))
async def admin_export_csv(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Export current month and current filter to CSV and send as a document."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = locale
    try:
        data = await state.get_data()
        mode = data.get("bookings_mode", "all")
        now_local = local_now()
        month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        month_end = next_month

        csv_path, file_name = await generate_bookings_csv(
            mode=mode,
            start=month_start.astimezone(ZoneInfo("UTC")),
            end=month_end.astimezone(ZoneInfo("UTC")),
            reference=now_local,
        )
        # Streamed file path returned; send as FSInputFile to avoid holding large CSV in RAM
        file = FSInputFile(csv_path, filename=file_name)
        m = _get_msg_obj(callback)
        if m:
            await m.answer_document(document=file)
        else:
            bot = getattr(callback, "bot", None)
            if bot:
                await bot.send_document(chat_id=callback.from_user.id, document=file)
    except Exception as e:
        logger.exception("Ошибка экспорта CSV: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    else:
        await callback.answer()


# ----------------------- CRUD мастеров ---------------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "add_master"))
@admin_handler
@admin_safe()
async def add_master_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует добавление нового мастера.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = locale
        await state.set_state(AdminStates.add_master_name)
        if m := _get_msg_obj(callback):
            await nav_push(state, t("enter_master_name", lang), None, lang=lang)
            await safe_edit(m, t("enter_master_name", lang))
        logger.info("Начало добавления мастера для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_start: %s", e)
    await callback.answer()


@admin_router.message(AdminStates.add_master_name, F.text)
@admin_handler
@admin_safe()
async def add_master_get_name(message: Message, state: FSMContext, locale: str) -> None:
    """Получает имя нового мастера и запрашивает Telegram ID.

    Args:
        message: Сообщение с именем мастера.
        state: Контекст FSM для сохранения имени.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    name = (message.text or "").strip()
    if not name:
        lang = locale
        await message.answer(t("invalid_name", lang))
        return
    try:
        await state.update_data(name=name)
        await state.set_state(AdminStates.add_master_id)
        lang = locale
        # Prompt allowing either numeric ID entry or forwarding a message from the master
        await message.answer(t("enter_master_id_or_forward", lang))
        logger.info("Имя мастера '%s' сохранено для пользователя %s", name, safe_user_id(message))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_get_name: %s", e)


@admin_router.message(AdminStates.add_master_id, F.text)
@admin_handler
@admin_safe()
async def add_master_finish(message: Message, state: FSMContext, locale: str) -> None:
    """Завершает добавление мастера, сохраняя его в базу.

    Args:
        message: Сообщение с Telegram ID мастера.
        state: Контекст FSM с сохраненным именем.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        tg_id = int(message.text or "")
    except ValueError:
        lang = locale
        await message.answer(t("invalid_id", lang))
        return
    data = await state.get_data()
    name = data.get("name", "Без імені")
    try:
        added = await MasterRepo.add_master(tg_id, name)
        lang = locale
        if added:
            logger.info("Админ %s добавил мастера %s (%s)", safe_user_id(message), tg_id, name)
            await message.answer(t("master_added", lang).format(name=name))
        else:
            await message.answer(t("admin_exists", lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_finish: %s", e)
    except Exception as e:
        logger.exception("Unexpected error in add_master_finish: %s", e)
    await state.clear()
    lang = locale
    try:
        from bot.app.services.admin_services import get_admin_dashboard_summary
        panel_text = await get_admin_dashboard_summary(lang=lang)
    except Exception:
        panel_text = t("admin_panel_title", lang)

    await message.answer(panel_text, reply_markup=admin_menu_kb(lang))
    try:
        await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
    except Exception as e:
        logger.exception("add_master_finish: nav_replace failed: %s", e)
        raise



@admin_router.message(AdminStates.add_master_id, F.forward_from)
@admin_handler
@admin_safe()
async def add_master_finish_forward(message: Message, state: FSMContext, locale: str) -> None:
    """Handle forwarded messages to extract master Telegram ID and finish add-master flow."""
    lang = locale
    # Try multiple strategies to extract a Telegram user id from the forwarded message.
    fwd_user = getattr(message, "forward_from", None)
    contact = getattr(message, "contact", None)
    username = None
    first_name = None
    last_name = None
    # If the admin forwarded a contact vCard with user_id, prefer that
    if contact and getattr(contact, "user_id", None):
        tg_id = int(contact.user_id)
        full_name = getattr(contact, "full_name", None) or getattr(contact, "phone_number", None) or "Без імені"
        username = getattr(contact, "username", None)
        first_name = getattr(contact, "first_name", None)
        last_name = getattr(contact, "last_name", None)
    elif fwd_user:
        try:
            tg_id = int(getattr(fwd_user, "id", 0) or 0)
        except Exception:
            await message.answer(t("invalid_id", lang))
            return
        full_name = getattr(fwd_user, "full_name", None) or getattr(fwd_user, "username", None) or "Без імені"
        username = getattr(fwd_user, "username", None)
        first_name = getattr(fwd_user, "first_name", None)
        last_name = getattr(fwd_user, "last_name", None)
    else:
        # Some forwarded messages (from channels or anonymous forwards) don't include a user id.
        # Provide a helpful instruction to the admin instead of a generic error.
        help_text = (
            "Неможливо визначити Telegram ID з цього пересланого повідомлення.\n"
            "Перешліть, будь ласка, приватне повідомлення від майстра або надішліть його числовий Telegram ID.\n"
            "Якщо у вас є контакт у телефонній книзі, перешліть контакт (vCard) з профілем майстра."
        )
        await message.answer(help_text)
        return
    _remember_forwarded_user_info(tg_id, username, first_name, last_name)
    data = await state.get_data()
    typed_name = data.get("name")
    display_name = format_user_display_name(username, first_name, last_name)
    name = typed_name or display_name or full_name
    try:
        added = await MasterRepo.add_master(
            tg_id,
            name,
            username=username,
            first_name=first_name,
            last_name=last_name,
        )
        if added:
            logger.info("Админ %s добавил мастера (forward) %s (%s)", safe_user_id(message), tg_id, name)
            await message.answer(t("master_added", lang).format(name=name))
        else:
            await message.answer(t("admin_exists", lang))
    except TelegramAPIError:
        # best-effort: ignore Telegram send errors here
        pass
    except Exception as e:
        logger.exception("Unexpected error in add_master_finish_forward: %s", e)
    await state.clear()
    try:
        from bot.app.services.admin_services import get_admin_dashboard_summary
        panel_text = await get_admin_dashboard_summary(lang=lang)
    except Exception:
        panel_text = t("admin_panel_title", lang)
    await message.answer(panel_text, reply_markup=admin_menu_kb(lang))
    try:
        await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
    except Exception as e:
        logger.exception("add_master_finish_forward: nav_replace failed: %s", e)
        raise


@admin_router.callback_query(AdminMenuCB.filter(F.act == "delete_master"))
@admin_handler
@admin_safe()
async def delete_master_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует удаление мастера с пагинацией.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения данных пагинации.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    # Fetch total count and first page slice without caching entire list in FSM.
    # We retain cache fallback for small deployments (< 200) for latency, but avoid storing list in FSM.
    try:
        from bot.app.services.master_services import MasterRepo
        total_count = await MasterRepo.count_masters()
    except Exception:
        masters = await masters_cache()
        total_count = len(masters)
    if total_count == 0:
        lang = locale
        await safe_edit(_get_msg_obj(callback), t("no_masters_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    from bot.app.core.constants import DEFAULT_PAGE_SIZE
    page_size = DEFAULT_PAGE_SIZE
    total_pages = (total_count + page_size - 1) // page_size
    # Fetch first page slice
    try:
        from bot.app.services.master_services import MasterRepo
        page_items = await MasterRepo.get_masters_page(page=1, page_size=page_size)
    except Exception:
        masters_fallback = await masters_cache()
        page_items = list(masters_fallback.items())[:page_size]
    await state.update_data(delete_page=1, delete_type="master")
    await _show_paginated(
        callback,
        state,
        total_pages,
        f"{t('select_master_to_delete', locale)}",
        "del_master",
        locale,
        page_items=page_items,
    )
    logger.info("Начало удаления мастера для пользователя %s", callback.from_user.id)


@admin_router.callback_query(DelMasterPageCB.filter())
async def delete_master_paginate(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Обрабатывает пагинацию при удалении мастера.

    Args:
        callback: CallbackQuery с номером страницы.
        state: Контекст FSM с данными пагинации.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        page = max(1, int(callback_data.page))
        lang = locale
        from bot.app.core.constants import DEFAULT_PAGE_SIZE
        page_size = DEFAULT_PAGE_SIZE
        try:
            from bot.app.services.master_services import MasterRepo
            total_count = await MasterRepo.count_masters()
            total_pages = (total_count + page_size - 1) // page_size
            if page > total_pages:
                page = total_pages or 1
            page_items = await MasterRepo.get_masters_page(page=page, page_size=page_size)
        except Exception:
            masters_fallback = await masters_cache()
            total_count = len(masters_fallback)
            total_pages = (total_count + page_size - 1) // page_size
            if page > total_pages:
                page = total_pages or 1
            all_items = list(masters_fallback.items())
            start = (page - 1) * page_size
            page_items = all_items[start:start+page_size]
        await state.update_data(delete_page=page)
        await _show_paginated(
            callback,
            state,
            total_pages,
            f"{t('select_master_to_delete', lang)}",
            "del_master",
            lang,
            page_items=page_items,
        )
        logger.info("Пагинация мастеров, страница %d, для пользователя %s", page, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка пагинации мастеров: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ConfirmDelMasterCB.filter())
@admin_handler
@admin_safe()
async def delete_master_confirm(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Запрашивает подтверждение удаления мастера.

    Args:
        callback: CallbackQuery с ID мастера.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        mid = int(callback_data.master_id)
        lang = locale
        # Reuse centralized keyboard
        kb_markup = confirm_delete_master_kb(mid, lang=lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, t("confirm_master_delete", lang).format(id=mid), kb_markup, lang=lang)
            await safe_edit(m, t("confirm_master_delete", lang).format(id=mid), reply_markup=kb_markup)
        logger.info("Запрос подтверждения удаления мастера %s для пользователя %s", mid, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления мастера: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()



@admin_router.callback_query(ConfirmCancelAllMasterCB.filter())
@admin_handler
@admin_safe()
async def confirm_cancel_all_master(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Ask admin to confirm cancelling all bookings for a master."""
    try:
        mid = int(callback_data.master_id)
        # Count current active bookings via AdminRepo (no session in handler)
        bids = await admin_services.AdminRepo.get_active_future_booking_ids_for_master(mid)  # type: ignore[attr-defined]
        lang = locale
        kb_markup = confirm_cancel_all_master_kb(mid, linked_count=len(bids), lang=lang)
        prompt = tr("cancel_all_bookings_prompt", lang=lang).format(count=len(bids), master_id=mid)
        if m := _get_msg_obj(callback):
            await nav_push(state, prompt, kb_markup, lang=lang)
            await safe_edit(m, prompt, reply_markup=kb_markup)
        logger.info("Confirm cancel all bookings for master %s requested by %s", mid, callback.from_user.id)
    except Exception as e:
        logger.exception("confirm_cancel_all_master failed: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ExecCancelAllMasterCB.filter())
@admin_handler
@admin_safe()
async def exec_cancel_all_master(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Cancel all active bookings for a master, notify clients and then delete the master."""
    try:
        mid = int(callback_data.master_id)
        # Fetch all booking ids for this master (regardless of status)
        rows = await admin_services.AdminRepo.get_booking_ids_for_master(mid)  # type: ignore[attr-defined]
        all_bids = [int(r[0]) for r in rows]
        status_map = {int(r[0]): r[1] for r in rows}

        # Cancel bookings and notify via centralized master service
        bot = getattr(callback, "bot", None)
        try:
            cancelled = await master_services.cancel_bookings_and_notify(bot, all_bids)
        except Exception:
            logger.exception("Failed to cancel and notify bookings for master %s", mid)
            cancelled = 0

        # After cancelling and notifying, DO NOT delete booking rows — preserve history.
        # Re-check whether any bookings still reference this master; if so,
        # inform the admin that deletion was not performed. If no bookings
        # reference the master, allow deletion.
        try:
            # Re-check active/future bookings via AdminRepo
            remaining = await admin_services.AdminRepo.get_active_future_booking_ids_for_master(mid)  # type: ignore[attr-defined]
            lang = locale
            if remaining:
                text = tr("cancel_all_bookings_dependencies", lang=lang).format(cancelled=cancelled, remaining=len(remaining))
                logger.info("Mass cancel for master %s completed; remaining dependencies: %s", mid, remaining)
            else:
                # No active/future bookings reference the master; safe to delete the master record via MasterRepo
                deleted = await master_services.MasterRepo.delete_master(mid)  # type: ignore[attr-defined]
                if deleted:
                    text = t("master_deleted", lang)
                    logger.info("Master %s deleted after mass-cancel by admin %s", mid, safe_user_id(callback))
                else:
                    text = t("not_found", lang)
        except Exception:
            logger.exception("Failed to finalize master deletion check after mass-cancel for master %s", mid)
            lang = locale
            text = t("db_error", lang)

        if m := _get_msg_obj(callback):
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        logger.exception("exec_cancel_all_master failed: %s", e)
        try:
            lang = locale
        except Exception:
            lang = locale
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(ExecDelMasterCB.filter())
@admin_handler
@admin_safe()
async def delete_master_exec(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Удаляет мастера.

    Args:
        callback: CallbackQuery с ID мастера.
        state: Контекст FSM.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        mid = int(callback_data.master_id)
        # Perform checks and deletion via admin services
        ok, blocking = await admin_services.delete_master_with_checks(mid)
        lang = locale
        if ok:
            text = t("master_deleted", lang)
            logger.info("Админ %s удалил мастера %s", safe_user_id(callback), mid)
        else:
            if blocking and blocking > 0:
                text = (
                    f"Cannot delete master: {blocking} active/future booking(s) reference this master. "
                    "Please cancel or reassign them before deletion."
                )
                logger.info(
                    "Admin %s attempted to delete master %s but %d active/future bookings reference it",
                    safe_user_id(callback), mid, blocking,
                )
            else:
                text = t("db_error", lang)
        if m := _get_msg_obj(callback):
            lang = locale
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в delete_master_exec: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при удалении мастера: %s", e)
            # lang might not be set if the error happened before we resolved it above
            try:
                lang = locale
            except Exception:
                lang = locale
            if m := _get_msg_obj(callback):
                await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
        else:
            logger.exception("Unexpected error in delete_master_exec: %s", e)
    await callback.answer()


@admin_router.callback_query(ConfirmForceDelMasterCB.filter())
@admin_handler
@admin_safe()
async def confirm_force_delete_master(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Show the destructive force-delete confirmation keyboard."""
    try:
        mid = int(callback_data.master_id)
        lang = locale
        kb_markup = confirm_force_delete_master_kb(mid, lang=lang)
        text = t("confirm_force_delete_title", lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, text, kb_markup, lang=lang)
            await safe_edit(m, text, reply_markup=kb_markup)
    except Exception as e:
        logger.exception("confirm_force_delete_master failed: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", locale), reply_markup=admin_menu_kb(locale))
    await callback.answer()


@admin_router.callback_query(ExecForceDelMasterCB.filter())
@admin_handler
@admin_safe()
async def exec_force_delete_master(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Execute physical deletion of master (force delete)."""
    try:
        mid = int(callback_data.master_id)
        # Perform force delete without writing backups as requested
        success, meta = await master_services.MasterRepo.force_delete_master(mid, backup=False)
        lang = locale
        if success:
            text = t("master_force_deleted", lang)
            logger.info("Admin %s force-deleted master %s (meta=%s)", safe_user_id(callback), mid, meta)
        else:
            text = t("db_error", lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        logger.exception("exec_force_delete_master failed: %s", e)
        if m := _get_msg_obj(callback):
            try:
                await safe_edit(m, t("db_error", locale), reply_markup=admin_menu_kb(locale))
            except Exception:
                # If safe_edit itself fails, log and let the error propagate
                logger.exception("Failed to notify admin about exec_force_delete_master failure")
        # Propagate exception to centralized error handler
        raise
    await callback.answer()


# ----------------------- CRUD услуг ---------------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "add_service"))
@admin_handler
@admin_safe()
async def add_service_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует добавление новой услуги.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = locale
        # Start by asking for the service name only; id (slug) will be auto-generated
        await state.set_state(AdminStates.add_service_name)
        try:
            cur = await state.get_state()
            logger.debug("add_service_start: FSM state after set_state -> %r", cur)
        except Exception:
            logger.exception("add_service_start: failed to read FSM state after set_state")
        if m := _get_msg_obj(callback):
            text = t("enter_service_name", lang)
            await nav_push(state, text, None, lang=lang)
            await safe_edit(m, text)
        logger.info("Начало добавления услуги для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_service_start: %s", e)
    await callback.answer()





@admin_router.message(AdminStates.add_service_name, F.text)
@admin_handler
@admin_safe()
async def add_service_finish(message: Message, state: FSMContext, locale: str) -> None:
    """Завершает добавление услуги — генерирует уникальный slug и сохраняет запись.

    Теперь админ вводит только человекочитаемое название, а бот сам генерирует
    уникальный идентификатор (slug) для Service.id.

    Args:
        message: Сообщение с названием услуги.
        state: Контекст FSM (не содержит больше id).
    """
    # Access is enforced by AdminRoleFilter applied on the router
    # Defensive debug logging: record incoming message and FSM state to help
    # diagnose cases where the handler is not triggered or message is ignored.
    try:
        cur_state = await state.get_state()
    except Exception:
        cur_state = None
    logger.debug("add_service_finish invoked: from=%s cur_state=%r text=%r", message.from_user.id, cur_state, message.text)
    name = (message.text or "(без назви)").strip()
    try:
        sid = await generate_unique_slug_from_name(name)
        # Delegate creation to ServiceRepo to centralize DB logic and caching
        created = await ServiceRepo.add_service(sid, name)
        lang = locale
        if created:
            logger.info("Админ %s добавил услугу %s (%s)", safe_user_id(message), sid, name)
            await message.answer(t("service_added", lang))
        else:
            await message.answer(t("service_exists", lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в add_service_finish: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при добавлении услуги: %s", e)
            _lang = locale
            await message.answer(t("db_error", _lang))
        else:
            logger.exception("Unexpected error in add_service_finish: %s", e)
    await state.clear()
    lang = locale
    await message.answer(t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))




@admin_router.callback_query(AdminMenuCB.filter(F.act == "delete_service"))
@admin_handler
@admin_safe()
async def delete_service_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует удаление услуги с пагинацией.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения данных пагинации.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    # Paginated approach: avoid storing full services mapping in FSM.
    try:
        total_count = await ServiceRepo.count_services()
    except Exception:
        services_cache_map = await ServiceRepo.services_cache()
        total_count = len(services_cache_map)
    if total_count == 0:
        lang = locale
        await safe_edit(_get_msg_obj(callback), t("no_services_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    from bot.app.core.constants import DEFAULT_PAGE_SIZE
    page_size = DEFAULT_PAGE_SIZE
    total_pages = (total_count + page_size - 1) // page_size
    try:
        page_items = await ServiceRepo.get_services_page(page=1, page_size=page_size)
    except Exception:
        services_cache_map = await ServiceRepo.services_cache()
        page_items = list(services_cache_map.items())[:page_size]
    await state.update_data(delete_page=1, delete_type="service")
    await _show_paginated(
        callback,
        state,
        total_pages,
        f"{t('select_service_to_delete', locale)}",
        "del_service",
        locale,
        page_items=page_items,
    )
    logger.info("Начало удаления услуги для пользователя %s", callback.from_user.id)


@admin_router.callback_query(DelServicePageCB.filter())
async def delete_service_paginate(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Обрабатывает пагинацию при удалении услуги.

    Args:
        callback: CallbackQuery с номером страницы.
        state: Контекст FSM с данными пагинации.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        page = max(1, int(callback_data.page))
        lang = locale
        from bot.app.core.constants import DEFAULT_PAGE_SIZE
        page_size = DEFAULT_PAGE_SIZE
        try:
            total_count = await ServiceRepo.count_services()
            total_pages = (total_count + page_size - 1) // page_size
            if page > total_pages:
                page = total_pages or 1
            page_items = await ServiceRepo.get_services_page(page=page, page_size=page_size)
        except Exception:
            services_cache_map = await ServiceRepo.services_cache()
            total_count = len(services_cache_map)
            total_pages = (total_count + page_size - 1) // page_size
            if page > total_pages:
                page = total_pages or 1
            all_items = list(services_cache_map.items())
            start = (page - 1) * page_size
            page_items = all_items[start:start+page_size]
        await state.update_data(delete_page=page)
        await _show_paginated(
            callback,
            state,
            total_pages,
            f"{t('select_service_to_delete', lang)}",
            "del_service",
            lang,
            page_items=page_items,
        )
        logger.info("Пагинация услуг, страница %d, для пользователя %s", page, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка пагинации услуг: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ConfirmDelServiceCB.filter())
@admin_handler
@admin_safe()
async def delete_service_confirm(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Запрашивает подтверждение удаления услуги.

    Args:
        callback: CallbackQuery с ID услуги.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        sid = str(callback_data.service_id)
        lang = locale
        # Show how many masters reference this service so admin can make an
        # informed decision.
        try:
            linked = await ServiceRepo.count_linked_masters(sid)
        except Exception:
            linked = 0

        kb = InlineKeyboardBuilder()
        kb.button(text=t("confirm_delete", lang), callback_data=pack_cb(ExecDelServiceCB, service_id=str(sid)))
        kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_service"))
        message_text = t("confirm_service_delete", lang).format(id=sid)
        if linked:
            # Localized linked masters line
            try:
                linked_txt = t("linked_masters", lang).format(count=linked)
            except Exception:
                linked_txt = f"Linked masters: {linked}"
            message_text = message_text + "\n\n" + linked_txt
        # Reuse centralized keyboard factory
        kb_markup = confirm_delete_service_kb(sid, lang=lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, message_text, kb_markup, lang=lang)
            await safe_edit(m, message_text, reply_markup=kb_markup)
        logger.info("Запрос подтверждения удаления услуги %s (linked=%d) для пользователя %s", sid, linked, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления услуги: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ExecDelServiceCB.filter())
@admin_handler
@admin_safe()
async def delete_service_exec(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Выполняет удаление услуги из базы.

    Args:
        callback: CallbackQuery с ID услуги.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        sid = str(callback_data.service_id)
        # Attempt unlink-and-delete atomically. The helper returns (deleted, unlinked_count).
        deleted, unlinked = await ServiceRepo.unlink_from_all_and_delete(sid)
        if deleted:
            logger.info("Админ %s удалил услугу %s (unlinked=%d)", safe_user_id(callback), sid, unlinked)
            lang = locale
            text = t("service_deleted", lang) + (f"\n\nUnlinked from {unlinked} masters." if unlinked else "")
        else:
            lang = locale
            text = t("not_found", lang)
        if m := _get_msg_obj(callback):
            lang = locale
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в delete_service_exec: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при удалении услуги: %s", e)
            if m := _get_msg_obj(callback):
                # ensure lang is available
                _lang = locals().get("lang", locale)
                await safe_edit(m, t("db_error", _lang), reply_markup=admin_menu_kb(_lang))
        else:
            logger.exception("Unexpected error in delete_service_exec: %s", e)
    await callback.answer()


# ----------------- Привязка и отвязка мастеров к услугам -----------------

async def _start_master_service_flow(callback: CallbackQuery, state: FSMContext, action: str, locale: str) -> None:
    """Инициирует процесс привязки/отвязки мастера и услуги.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
        action: Действие ("link" или "unlink").
    """
    # Access is enforced by AdminRoleFilter applied on the router
    masters = await masters_cache()
    lang = locale
    if not masters:
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("no_masters_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    kb = InlineKeyboardBuilder()
    for mid, name in masters.items():
        if action == "link":
            kb.button(text=name, callback_data=pack_cb(SelectLinkMasterCB, master_id=int(mid)))
        else:
            kb.button(text=name, callback_data=pack_cb(SelectUnlinkMasterCB, master_id=int(mid)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="panel"))
    kb.adjust(1)
    if m := _get_msg_obj(callback):
        await safe_edit(m, t("select_master", lang), reply_markup=kb.as_markup())
    await state.set_state(AdminStates.link_master_service_select_master)
    await state.update_data(action=action)
    logger.info("Начало %s мастера и услуги для пользователя %s", action, callback.from_user.id)
    await callback.answer()


async def _select_master_for_service_flow(callback: CallbackQuery, state: FSMContext, action: str, callback_data: Any = None, locale: str | None = None) -> None:
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        if callback_data is not None and getattr(callback_data, "master_id", None) is not None:
            master_tid = int(callback_data.master_id)
        else:
            master_tid = int((callback.data or "").split("_")[-1])
        await state.update_data(master_tid=master_tid)
    except (ValueError, IndexError):
        lang = locale or default_language()
        await callback.answer(t("invalid_id", lang), show_alert=True)
        return

    lang = locale or default_language()
    
    # Получаем список услуг
    if action == "unlink":
        # Для отвязки: получаем услуги через MasterRepo
        services = await master_services.MasterRepo.get_services_for_master(master_tid)  # type: ignore[attr-defined]
    else:
        # Для привязки: все доступные услуги
        services_dict = await ServiceRepo.services_cache()
        logger.debug("Services data from cache for link: %s", services_dict)
        services = [(sid, name) for sid, name in services_dict.items()]

    if not services:
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("no_services_linked" if action == "unlink" else "no_services_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return

    kb = InlineKeyboardBuilder()
    for sid, name in services:
        if action == "link":
            kb.button(text=name, callback_data=pack_cb(SelectLinkServiceCB, service_id=str(sid)))
        else:
            kb.button(text=name, callback_data=pack_cb(SelectUnlinkServiceCB, service_id=str(sid)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="panel"))
    kb.adjust(1)
    
    if m := _get_msg_obj(callback):
        await nav_push(state, t("select_service", lang), kb.as_markup(), lang=lang)
        await safe_edit(m, t("select_service", lang), reply_markup=kb.as_markup())
    await state.set_state(AdminStates.link_master_service_select_service)
    logger.info("Выбор услуги для %s мастера %s пользователем %s", action, master_tid, callback.from_user.id)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "link_ms"))
async def link_master_service_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует привязку мастера к услуге.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    await _start_master_service_flow(callback, state, "link", locale)


@admin_router.callback_query(SelectLinkMasterCB.filter())
async def link_master_select(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор мастера для привязки.

    Args:
        callback: CallbackQuery с ID мастера.
        state: Контекст FSM для сохранения состояния.
    """
    await _select_master_for_service_flow(callback, state, "link", callback_data=callback_data, locale=locale)


@admin_router.callback_query(SelectLinkServiceCB.filter())
async def link_master_finish(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Завершает привязку мастера к услуге.

    Args:
        callback: CallbackQuery с ID услуги.
        state: Контекст FSM с сохраненным ID мастера.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    service_id = str(callback_data.service_id)
    lang = locale
    data = await state.get_data()
    master_tid = data.get("master_tid")
    try:
        # Delegate linking to MasterRepo to centralize DB access
        # Ensure master exists first
        master_tid_int = int(master_tid or 0)
        master_name = (await masters_cache()).get(master_tid_int)
        if master_name is None:
            await callback.answer(t("master_not_found", lang), show_alert=True)
            return
        linked = await master_services.MasterRepo.link_service(master_telegram_id=master_tid_int, service_id=service_id)  # type: ignore[attr-defined]
        if linked:
            try:
                invalidate_masters_cache()
            except Exception as e:
                logger.exception("link_master_finish: invalidate_masters_cache failed: %s", e)
                raise
            logger.info("Админ %s привязал мастера %s к услуге %s", safe_user_id(callback), master_tid, service_id)
            text = t("link_added", lang)
        else:
            text = t("already_linked", lang)
        await safe_edit(_get_msg_obj(callback), text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в link_master_finish: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при привязке: %s", e)
            if m := _get_msg_obj(callback):
                _lang = locals().get("lang", locale)
                await safe_edit(m, t("db_error", _lang), reply_markup=admin_menu_kb(_lang))
        else:
            logger.exception("Unexpected error in link_master_finish: %s", e)
    await state.clear()
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "unlink_ms"))
async def unlink_master_service_start(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Инициирует отвязку мастера от услуги.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    await _start_master_service_flow(callback, state, "unlink", locale)


@admin_router.callback_query(SelectUnlinkMasterCB.filter())
async def unlink_master_select(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор мастера для отвязки.

    Args:
        callback: CallbackQuery с ID мастера.
        state: Контекст FSM для сохранения состояния.
    """
    await _select_master_for_service_flow(callback, state, "unlink", callback_data=callback_data, locale=locale)


@admin_router.callback_query(SelectUnlinkServiceCB.filter())
async def unlink_master_finish(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    # Access is enforced by AdminRoleFilter applied on the router
    service_id = str(callback_data.service_id)
    lang = locale
    data = await state.get_data()
    master_tid = data.get("master_tid")
    try:
        master_tid_int = int(master_tid or 0)
        master_name = (await masters_cache()).get(master_tid_int)
        if master_name is None:
            await callback.answer(t("master_not_found", lang), show_alert=True)
            return
        removed = await master_services.MasterRepo.unlink_service(master_telegram_id=master_tid_int, service_id=service_id)  # type: ignore[attr-defined]
        if removed:
            try:
                invalidate_masters_cache()
            except Exception as e:
                logger.exception("unlink_master_finish: invalidate_masters_cache failed: %s", e)
                raise
            logger.info("Админ %s отвязал мастера %s от услуги %s", safe_user_id(callback), master_tid, service_id)
            text = t("link_removed", lang)
        else:
            text = t("link_not_found", lang)
        await safe_edit(_get_msg_obj(callback), text, reply_markup=admin_menu_kb(lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при отвязке: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в unlink_master_finish: %s", e)
    await state.clear()
    await callback.answer()


# ----------------------------- Настройки ---------------------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings"))
async def admin_settings(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает меню настроек админ-панели.

    Args:
        callback: CallbackQuery для отображения настроек.
    """
    user_id = callback.from_user.id
    logger.info("Меню настроек открыто для пользователя %s", user_id)
    # Access is enforced by AdminRoleFilter applied on the router
    lang = await _lang_with_state(state, locale)
    # Prefetch settings and provider token in handler (keyboard must not query DB)
    try:
        token = (await get_telegram_provider_token()) or ""
        enabled = await is_telegram_payments_enabled()
        try:
            hold_min = int(await SettingsRepo.get_setting("reservation_hold_minutes", 10) or 10)
        except Exception:
            hold_min = 10
        try:
            cancel_h = int(await SettingsRepo.get_setting("client_cancel_lock_hours", 3) or 3)
        except Exception:
            cancel_h = 3
        try:
            expire_sec = int(await SettingsRepo.get_setting("reservation_expire_check_seconds", 30) or 30)
        except Exception:
            expire_sec = 30
    except Exception:
        token = ""
        enabled = False
        hold_min = 10
        cancel_h = 3
        expire_sec = 30

    # Fetch new settings for redesigned UI
    hours_summary = await SettingsRepo.get_setting("working_hours_summary", None)
    kb = admin_settings_kb(
        lang,
        telegram_provider_token=token,
        payments_enabled=enabled,
        hold_min=hold_min,
        cancel_h=cancel_h,
        hours_summary=hours_summary,
    )
    msg = _get_msg_obj(callback)
    if msg:
        await nav_push(state, t("settings_title", lang), kb, lang=lang)
        await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
    else:
        if callback.message:
            await callback.message.answer(t("settings_title", lang), reply_markup=kb)
    logger.info("Меню настроек отображено для пользователя %s", user_id)
    await callback.answer()


async def apply_setting_change(key: str, value: Any, callback: CallbackQuery, locale: str) -> bool:
    """Apply a single setting change via update_setting and show a localized toast.

    Returns True on success, False on failure.
    """
    try:
        await SettingsRepo.update_setting(key, value)
    except Exception:
        logger.warning("Failed to update %s via settings API", key)
        lang = await _language_default(locale)
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("apply_setting_change: callback.answer failed: %s", e)
            try:
                await callback.answer("Error")
            except Exception as e2:
                logger.exception("apply_setting_change: secondary callback.answer failed: %s", e2)
                raise
        return False

    lang = await _language_default(locale)
    try:
        if key == "reservation_expire_check_seconds":
            seconds = int(value)
            if seconds >= 86400 and seconds % 86400 == 0:
                label = f"{seconds // 86400} {t('day', lang) if t('day', lang) != 'day' else 'day'}"
            elif seconds >= 3600 and seconds % 3600 == 0:
                label = f"{seconds // 3600} {t('hours_short', lang) or 'h'}"
            elif seconds >= 60 and seconds % 60 == 0:
                label = f"{seconds // 60} {t('minutes_short', lang) or 'min'}"
            else:
                label = f"{seconds} s"
            try:
                await callback.answer(f"✅ {t('expire_check_frequency', lang) if t('expire_check_frequency', lang) != 'expire_check_frequency' else 'Frequency updated'}: каждые {label}")
            except Exception as e:
                logger.exception("apply_setting_change: primary callback.answer failed for expire_check_frequency: %s", e)
                try:
                    await callback.answer(f"✅ Частота проверки обновлена: каждые {label}")
                except Exception as e2:
                    logger.exception("apply_setting_change: secondary callback.answer failed for expire_check_frequency: %s", e2)
                    raise
        elif key == "reservation_hold_minutes":
            minutes = int(value)
            try:
                await callback.answer(t("hold_label", lang).format(minutes=minutes))
            except Exception as e:
                logger.exception("apply_setting_change: callback.answer failed for reservation_hold_minutes: %s", e)
                try:
                    await callback.answer(f"✅ hold minutes set: {minutes}")
                except Exception as e2:
                    logger.exception("apply_setting_change: secondary callback.answer failed for reservation_hold_minutes: %s", e2)
                    raise
        elif key == "client_cancel_lock_hours":
            hours = int(value)
            try:
                await callback.answer(t("cancel_lock_label", lang).format(hours=hours))
            except Exception as e:
                logger.exception("apply_setting_change: callback.answer failed for client_cancel_lock_hours: %s", e)
                try:
                    await callback.answer(f"✅ cancel lock set: {hours}")
                except Exception as e2:
                    logger.exception("apply_setting_change: secondary callback.answer failed for client_cancel_lock_hours: %s", e2)
                    raise
        else:
            try:
                await callback.answer(t("settings_saved", lang))
            except Exception as e:
                logger.exception("apply_setting_change: callback.answer failed for settings_saved: %s", e)
                try:
                    await callback.answer("✅ Saved")
                except Exception as e2:
                    logger.exception("apply_setting_change: secondary callback.answer failed for settings_saved: %s", e2)
                    raise
    except Exception as e:
        logger.exception("apply_setting_change: unexpected error when applying setting %s=%s: %s", key, value, e)
        try:
            await callback.answer(t("settings_saved", locale))
        except Exception as e2:
            logger.exception("apply_setting_change: fallback callback.answer failed: %s", e2)
            try:
                await callback.answer("✅ Saved")
            except Exception as e3:
                logger.exception("apply_setting_change: final fallback callback.answer failed: %s", e3)
                raise

    return True


@admin_router.callback_query(AdminMenuCB.filter(F.act == "toggle_telegram_payments"))
async def admin_toggle_telegram_payments_handler(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Переключает состояние Telegram Payments.

    Args:
        callback: CallbackQuery для переключения.
    """
    user_id = callback.from_user.id
    logger.info("Переключение Telegram Payments для пользователя %s", user_id)
    # Access is enforced by AdminRoleFilter applied on the router
    lang = locale
    try:
        # Prevent enabling when provider token missing
        token = await get_telegram_provider_token() or ""
        if not token:
            await callback.answer(
                t("payments_token_missing", lang),
                show_alert=True,
            )
            # show settings with current token/enabled state
            try:
                from bot.app.services.shared_services import is_telegram_payments_enabled as _is_enabled
                enabled_now = bool(_is_enabled())
            except Exception:
                enabled_now = False
            from bot.app.telegram.admin.admin_keyboards import business_settings_kb
            hold_min = None
            cancel_h = None
            try:
                hold_min = await SettingsRepo.get_reservation_hold_minutes()
            except Exception as e:
                logger.exception("admin_toggle_payments: get_reservation_hold_minutes failed: %s", e)
                raise
            try:
                cancel_h = await SettingsRepo.get_client_cancel_lock_hours()
            except Exception as e:
                logger.exception("admin_toggle_payments: get_client_cancel_lock_hours failed: %s", e)
                raise
            kb = business_settings_kb(
                lang,
                telegram_provider_token=token,
                payments_enabled=enabled_now,
                hold_min=hold_min,
                cancel_h=cancel_h,
            )
            msg = _get_msg_obj(callback)
            if msg:
                title = t("settings_category_business", lang) or "Business"
                await nav_push(state, title, kb, lang=lang)
                await safe_edit(msg, title, reply_markup=kb)
            return
        new_val = await toggle_telegram_payments()
        status = t("enabled", lang) if new_val else t("disabled", lang)
        logger.info("Админ %s переключил Telegram Payments на %s", user_id, status)
        await callback.answer(t("payments_toggled", lang).format(status=status))
        # Re-fetch token to ensure freshness and show updated toggle state
        try:
            token_now = await get_telegram_provider_token() or ""
            payments_now = bool(new_val)
        except Exception:
            token_now = await get_telegram_provider_token() or ""
            payments_now = bool(new_val)
        from bot.app.telegram.admin.admin_keyboards import business_settings_kb
        hold_min = None
        cancel_h = None
        try:
            hold_min = await SettingsRepo.get_reservation_hold_minutes()
        except Exception as e:
            logger.exception("admin_toggle_payments (refresh): get_reservation_hold_minutes failed: %s", e)
            raise
        try:
            cancel_h = await SettingsRepo.get_client_cancel_lock_hours()
        except Exception as e:
            logger.exception("admin_toggle_payments (refresh): get_client_cancel_lock_hours failed: %s", e)
            raise
        kb = business_settings_kb(
            lang,
            telegram_provider_token=token_now,
            payments_enabled=payments_now,
            hold_min=hold_min,
            cancel_h=cancel_h,
        )
        msg = _get_msg_obj(callback)
        if msg:
            title = t("settings_category_business", lang) or "Business"
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(msg, title, reply_markup=kb)
        else:
            if callback.message:
                await callback.message.answer(t("settings_title", lang), reply_markup=kb)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_toggle_telegram_payments_handler: %s", e)
        await callback.answer(t("telegram_error", lang))
    except Exception as e:
        logger.exception("Неожиданная ошибка в admin_toggle_telegram_payments_handler: %s", e)



@admin_router.callback_query(AdminMenuCB.filter(F.act == "hold_menu"))
async def admin_hold_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора времени удержания резерва."""
    # Business logic: let exceptions bubble to global handler.
    if m := getattr(callback, "message", None):
        lang = locale
        kb = admin_hold_menu_kb(lang)
        text = f"{t('settings_title', lang)}\n\n{t('hold_desc', lang)}"
        await nav_push(state, text, kb, lang=lang)
        # Only catch Telegram errors for the editing call
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_hold_menu")
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "cancel_menu"))
async def admin_cancel_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора окна запрета отмены (в часах)."""
    # Business logic: let exceptions bubble to global handler.
    if m := getattr(callback, "message", None):
        lang = locale
        kb = admin_cancel_menu_kb(lang)
        text = f"{t('settings_title', lang)}\n\n{t('cancel_desc', lang)}"
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_cancel_menu")
    await callback.answer()




@admin_router.callback_query(AdminSetExpireCB.filter())
async def admin_set_expire(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Set reservation_expire_check_seconds and refresh settings UI via admin_settings."""
    # Let failures propagate to global error handler; parse input and perform update.
    seconds = int(callback_data.seconds)
    await SettingsRepo.update_setting("reservation_expire_check_seconds", seconds)
    await admin_settings_business(callback, state, locale)


@admin_router.callback_query(AdminSetHoldCB.filter())
async def admin_set_hold(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Set reservation_hold_minutes and refresh settings UI via admin_settings."""
    minutes = int(callback_data.minutes)
    await SettingsRepo.update_setting("reservation_hold_minutes", minutes)
    await admin_settings_business(callback, state, locale)


@admin_router.callback_query(AdminSetCancelCB.filter())
async def admin_set_cancel_lock(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Set client_cancel_lock_hours and refresh settings UI via admin_settings."""
    hours = int(callback_data.hours)
    await SettingsRepo.update_setting("client_cancel_lock_hours", hours)
    await admin_settings_business(callback, state, locale)


# ---------------------------- Статистика и Аналитика ----------------------------

async def _format_and_send_stats(
    callback: CallbackQuery,
    title: str,
    data: list[dict[str, Any]],
    format_str: str,
    lang: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    """Форматирует и отправляет статистику в сообщении."""
    try:
        lines = [title, ""]
        for item in data:
            try:
                formatted = format_str.format(**item)
                lines.append(formatted)
            except KeyError as ke:
                logger.warning("Отсутствует ключ в данных статистики: %s, item: %s", ke, item)
                continue
        body = "\n".join(lines)
        logger.debug("_format_and_send_stats: sending %d lines, preview: %s", len(lines), body[:200])
        if m := _get_msg_obj(callback):
            await safe_edit(m, body, reply_markup=reply_markup)
        logger.info("Статистика '%s' отправлена для пользователя %s", title, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в _format_and_send_stats: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=reply_markup)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "stats"))
async def show_stats_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    try:
        totals = await AdminRepo.get_basic_totals()
        text = (
            f"{t('total_bookings', lang)}: {totals.get('total_bookings', 0)}\n"
            f"{t('total_users', lang)}: {totals.get('total_users', 0)}\n"
            f"{t('select_filter', lang)}"
        )
        markup = stats_menu_kb(lang)  # Добавил переменную для удобства
        if m := _get_msg_obj(callback):
            await safe_edit(m, text, reply_markup=markup)
        await nav_replace(state, text, markup, lang=lang)  # Добавьте это: обновляем state
        logger.info("Меню статистики показано для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в show_stats_menu: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=markup)
    await callback.answer(cache_time=1, show_alert=False)


@admin_router.callback_query(AdminMenuCB.filter(F.act.in_({"stats_range_week", "stats_range_month"})))
async def show_stats_range(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    kind = "week" if "week" in (callback.data or "") else "month"
    lang = locale
    try:
        stats = await AdminRepo.get_range_stats(kind)
        title = f"📈 {t('stats_week', lang) if kind == 'week' else t('stats_month', lang)}"
        lines = [
            title,
            f"{t('bookings', lang)}: {stats.get('bookings', 0)}",
            f"{t('unique_users', lang)}: {stats.get('unique_users', 0)}",
            f"{t('masters', lang)}: {stats.get('masters', 0)}",
            f"{t('avg_per_day', lang)}: {stats.get('avg_per_day', 0):.1f}",
        ]
        text = "\n".join(lines)  # Добавил переменную
        markup = stats_menu_kb(lang)
        if m := _get_msg_obj(callback):
            await safe_edit(m, text, reply_markup=markup)
        await nav_replace(state, text, markup, lang=lang)  # Добавьте это
        logger.info("Статистика за %s отображена для пользователя %s", kind, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в show_stats_range: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=markup)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "stats_by_master"))
async def show_stats_by_master(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику по мастерам."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = locale
    try:
        await _format_and_send_stats(
            callback,
            t("top_masters", lang),
                await AdminRepo.get_top_masters(limit=10),
            "{name}: {count}",
            lang,
            stats_menu_kb(lang),
        )
        logger.info("Статистика по мастерам отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в show_stats_by_master: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=stats_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "stats_by_service"))
async def show_stats_by_service(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику по услугам."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = locale
    try:
        services = await AdminRepo.get_top_services(limit=10)
        await _format_and_send_stats(
            callback,
            t("top_services", lang),
            services,
            "{service}: {count}",
            lang,
            stats_menu_kb(lang),
        )
        logger.info("Статистика по услугам отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в show_stats_by_service: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=stats_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz"))
async def admin_biz_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню бизнес-аналитики.

    Args:
        callback: CallbackQuery для отображения меню.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        if m := _get_msg_obj(callback):
            lang = locale

            # Fetch key business metrics for the last 30 days (month range)
            try:
                revenue_month = await AdminRepo.get_revenue_total("month")
            except Exception:
                revenue_month = 0
            try:
                retention_month = await AdminRepo.get_retention("month")
            except Exception:
                retention_month = {"repeaters": 0, "total": 0, "rate": 0.0}
            try:
                noshow_month = await AdminRepo.get_no_show_rates("month")
            except Exception:
                noshow_month = {"no_show": 0, "total": 0, "rate": 0.0}

            # Compose a compact business summary. Use existing translation keys
            # where appropriate and fall back to readable labels.
            summary_title = t("biz_analytics_title", lang)
            try:
                revenue_txt = format_money_cents(revenue_month)
            except Exception:
                revenue_txt = str(revenue_month)

            text = (
                f"<b>Бизнес-сводка (за 30 дней)</b>\n\n"
                f"💰 {t('admin_dashboard_revenue', lang)}: {revenue_txt}\n"
                f"🔄 {t('admin_dashboard_retention', lang)}: {retention_month.get('rate', 0) * 100:.1f}% "
                f"({retention_month.get('repeaters', 0)}/{retention_month.get('total', 0)})\n"
                f"👻 {t('admin_dashboard_no_shows', lang)}: "
                f"{noshow_month.get('rate', 0) * 100:.1f}% ({noshow_month.get('no_show', 0)}/{noshow_month.get('total', 0)})\n\n"
                f"{summary_title}"
            )

            kb = biz_menu_kb(lang)
            await nav_push(state, text, kb, lang=lang)
            await safe_edit(m, text, reply_markup=kb)
        logger.info("Меню бизнес-аналитики показано для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_biz_menu: %s", e)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_menu: %s", e)
    await callback.answer()


# Quick analytics shortcuts: map the one-tap buttons from `analytics_kb` to
# the existing handlers so the quick buttons work even if a more elaborate
# analytics flow exists.
@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_top_masters"))
async def admin_quick_top_masters(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Delegate quick top masters button to the full stats handler."""
    try:
        await show_stats_by_master(callback, state, locale)
    except Exception:
        lang = locale
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_revenue"))
async def admin_quick_revenue(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Delegate quick revenue button to the biz revenue handler."""
    try:
        await admin_biz_revenue(callback, state, locale)
    except Exception:
        lang = locale
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_retention"))
async def admin_quick_retention(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Delegate quick retention button to the biz retention handler."""
    try:
        await admin_biz_retention(callback, state, locale)
    except Exception:
        lang = locale
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_compare"))
async def admin_quick_compare(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Quick compare delegates to the business analytics menu to choose ranges."""
    try:
        await admin_biz_menu(callback, state, locale)
    except Exception:
        lang = locale
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_rev"))
async def admin_biz_revenue(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику выручки."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = locale
    try:
        lines = [t("revenue_title", lang), ""]
        lines.append(
            f"{t('month', lang)}: {format_money_cents(await AdminRepo.get_revenue_total('month'))}"
        )
        lines.append(
            f"{t('week', lang)}: {format_money_cents(await AdminRepo.get_revenue_total('week'))}"
        )

        masters = await AdminRepo.get_revenue_by_master("month", limit=5)
        if masters:
            lines.append(f"\n{t('top_masters', lang)}:")
            lines.extend(
                f"- {m['name']}: {format_money_cents(m['revenue_cents'])} "
                f"({m['bookings']} {t('bookings_short', lang)})"
                for m in masters
            )

        services = await AdminRepo.get_revenue_by_service("month", limit=5)
        if services:
            lines.append(f"\n{t('top_services', lang)}:")
            lines.extend(
                f"- {s['service']}: {format_money_cents(s['revenue_cents'])} "
                f"({s['bookings']} {t('bookings_short', lang)})"
                for s in services
            )

        if m := _get_msg_obj(callback):
            # только обновляем сообщение, остаёмся в бизнес‑меню
            body = "\n".join(lines)
            logger.debug(
                "admin_biz_revenue: editing message with %d chars, preview: %s",
                len(body),
                body[:200],
            )
            await safe_edit(m, body, reply_markup=biz_menu_kb(lang))
        logger.info(
            "Статистика выручки отображена для пользователя %s", callback.from_user.id
        )
    except Exception as e:
        logger.exception("Ошибка в admin_biz_revenue: %s", e)
        if m := _get_msg_obj(callback):
            logger.debug(
                "admin_biz_revenue: encountered exception, sending error text to message"
            )
            await safe_edit(m, t("error", lang), reply_markup=biz_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_ret"))
async def admin_biz_retention(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику удержания клиентов."""
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = locale
        ret_m = await AdminRepo.get_retention("month")
        ret_w = await AdminRepo.get_retention("week")
        lines = [t("retention_title", lang), ""]
        lines.append(
            f"{t('month', lang)}: "
            f"{ret_m.get('repeaters', 0)}/{ret_m.get('total', 0)} "
            f"({ret_m.get('rate', 0) * 100:.1f}% {t('repeaters', lang)})"
        )
        lines.append(
            f"{t('week', lang)}: "
            f"{ret_w.get('repeaters', 0)}/{ret_w.get('total', 0)} "
            f"({ret_w.get('rate', 0) * 100:.1f}% {t('repeaters', lang)})"
        )
        if m := _get_msg_obj(callback):
            await safe_edit(m, "\n".join(lines), reply_markup=biz_menu_kb(lang))
        logger.info("Статистика удержания отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_retention: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=biz_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_ns"))
async def admin_biz_no_show(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику no-show."""
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = locale
        ns = await AdminRepo.get_no_show_rates("month")
        lines = [t("no_show_title", lang), ""]
        lines.append(
            f"{t('total', lang)}: "
            f"{ns.get('no_show', 0)}/{ns.get('total', 0)} "
            f"({ns.get('rate', 0) * 100:.1f}%)"
        )
        if m := _get_msg_obj(callback):
            await safe_edit(m, "\n".join(lines), reply_markup=biz_menu_kb(lang))
        logger.info("Статистика no-show отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_no_show: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=biz_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_ltv"))
async def admin_biz_ltv(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику LTV топ-клиентов."""
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = locale
        topc = await AdminRepo.get_top_clients_ltv("month", limit=5)
        format_str = "- {name}: {money} ({bookings} {bookings_short})"
        formatted_data = []
        try:
            default_currency = await SettingsRepo.get_setting("currency", "UAH") or "UAH"
        except Exception:
            default_currency = "UAH"

        for row in topc:
            if not all(key in row for key in ["name", "revenue_cents", "bookings"]):
                logger.warning("Некорректная структура данных в get_top_clients_ltv: %s", row)
                continue
            money = format_money_cents(row["revenue_cents"], row.get("currency", default_currency))
            formatted_data.append({
                "name": row["name"],
                "money": money,
                "bookings": row["bookings"],
                "bookings_short": t("bookings_short", lang),
            })

        if not topc:
            logger.info("Данные LTV отсутствуют для пользователя %s", callback.from_user.id)

        await _format_and_send_stats(
            callback,
            t("top_ltv", lang),
            formatted_data,
            format_str,
            lang,
            biz_menu_kb(lang),
        )
        logger.info("Статистика LTV отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_ltv: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("error", lang), reply_markup=biz_menu_kb(lang))
    await callback.answer()


# -------------------------- Управление локалью ---------------------------

def _parse_locale_command(raw: str | None) -> tuple[str | None, str | None, str | None]:
    """Разбирает команду /set_locale.

    Args:
        raw: Текст команды.

    Returns:
        Кортеж (target, locale, error_message).
    """
    parts = (raw or "").split()
    if len(parts) < 3:
        return None, None, "locale_usage"
    return parts[1], parts[2], None


def _resolve_target_id(target: str | None, message: Message) -> int | str | None:
    """Определяет ID цели для установки локали.

    Args:
        target: Цель команды (telegram_id, 'me', 'global').
        message: Входящее сообщение.

    Returns:
        ID пользователя, 'global' или None при ошибке.
    """
    if target == "me":
        return message.from_user.id
    if target == "global":
        return None
    try:
        return int(target or "")
    except ValueError:
        return "error"


@admin_router.message(Command("set_locale"))
async def cmd_set_locale(message: Message, locale: str) -> None:
    """Устанавливает локаль для пользователя или глобально.

    Args:
        message: Сообщение с командой /set_locale.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    lang = await _language_default(locale)
    target, new_locale, error = _parse_locale_command(message.text)
    if error:
        await message.reply(t("locale_usage", lang))
        return
    if new_locale not in ['uk', 'en', 'ru']:
        await message.reply(t("invalid_locale", lang))
        return
    target_id = _resolve_target_id(target, message)
    if target_id == "error":
        await message.reply(t("invalid_id", lang))
        return
    try:
        if target_id is None:  # Global
            success = False
            try:
                success = await SettingsRepo.update_setting("language", new_locale)
            except Exception as exc:
                logger.warning("Failed to update global locale: %s", exc)
            if not success:
                await message.reply(t("error", lang))
                return
            await message.reply(t("global_locale_set", lang).format(locale=new_locale))
            logger.info("Админ %s установил глобальную локаль %s", safe_user_id(message), new_locale)
            return
        # Use UserRepo to avoid opening sessions in the handler
        try:
            tid = int(target_id)
            user = await UserRepo.get_by_telegram_id(tid)
            if not user:
                user = await UserRepo.get_or_create(tid, name=str(tid))
                action = t("user_created", lang)
            else:
                ok = await UserRepo.set_locale(tid, new_locale)
                action = t("user_updated", lang) if ok else t("error", lang)

            await message.reply(t("user_locale_set_fmt", lang).format(action=action, id=tid, locale=new_locale))
            logger.info("Админ %s установил локаль %s для пользователя %s", safe_user_id(message), new_locale, tid)
        except Exception:
            await message.reply(t("error", lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в cmd_set_locale: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при установке локали: %s", e)
            _lang = locale
            await message.reply(t("db_error", _lang))
        else:
            logger.exception("Unexpected error in cmd_set_locale: %s", e)






__all__ = ["admin_router"]