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
    AdminSetRescheduleCB,
    AdminSetExpireCB,
    AdminSetReminderCB,
    AdminSetReminderSameDayCB,
    AdminMenuCB,
    AdminEditSettingCB,
    NavCB,
    ConfirmDelAdminCB,
    ExecDelAdminCB,
    AdminMasterCardCB,
    AdminLookupUserCB,
    ConfirmForceDelMasterCB,
    ExecForceDelMasterCB,
    PricePageCB,
)
from bot.app.telegram.common.callbacks import AdminEnterCurrencyCB

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.filters.state import StateFilter
from aiogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    FSInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select, delete
from sqlalchemy.exc import SQLAlchemyError
from aiogram.exceptions import TelegramAPIError
from datetime import datetime

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, Master, MasterService, Service, User


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


from bot.app.services.admin_services import (
    AdminRepo,
    generate_bookings_csv,
    export_month_bookings_csv,
    generate_unique_slug_from_name,
    validate_contact_phone,
    validate_instagram_handle,
)
from bot.app.core.constants import DEFAULT_PAGE_SIZE
from bot.app.services.shared_services import (
    toggle_telegram_payments,
    toggle_telegram_miniapp,
    format_money_cents,
    format_minutes_short,
    get_telegram_provider_token,
    _msg as _shared_msg,
    safe_user_id,
    get_local_tz,
    is_telegram_payments_enabled,
    is_telegram_miniapp_enabled,
    format_user_display_name,
    local_now,
    get_env_int,
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
    invalidate_masters_cache,
)
from bot.app.services.master_services import masters_cache
import bot.app.services.master_services as master_services
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bot.app.translations import t, tr
from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.roles import AdminRoleFilter
from bot.app.telegram.admin.admin_keyboards import (
    admin_menu_kb,
    admin_settings_kb,
    admin_hold_menu_kb,
    pagination_kb,
    stats_menu_kb,
    biz_menu_kb,
    services_list_kb,
    services_prices_kb,
    edit_price_kb,
    admin_cancel_menu_kb,
    admin_reschedule_menu_kb,
    no_masters_kb,
    no_services_kb,
    masters_list_kb,
    services_select_kb,
    contacts_settings_kb,
    confirm_delete_service_kb,
    confirm_delete_master_kb,
    confirm_cancel_all_master_kb,
    confirm_force_delete_master_kb,
    admin_reminder_menu_kb,
    admin_expire_menu_kb,
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
# Avoid top-level import of client handlers to prevent import cycles; lazy-import where needed.

# Local text dictionary & helpers (static analyzer friendly)
logger = logging.getLogger(__name__)

admin_router = Router(name="admin")
# Attach locale middleware so handlers receive `locale` via data injection
from bot.app.telegram.common.locale_middleware import LocaleMiddleware

admin_router.message.middleware(LocaleMiddleware())
admin_router.callback_query.middleware(LocaleMiddleware())
from bot.app.telegram.common.ui_fail_safe import SafeUIMiddleware

# Attach Safe UI middleware to centralize user checks and error handling
admin_router.message.middleware(SafeUIMiddleware())
admin_router.callback_query.middleware(SafeUIMiddleware())
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

# Prefer resolving local timezone at render time. Call `get_local_tz()` where needed
# to avoid capturing a static TZ at module import time.


@admin_router.message(Command("start"))
async def admin_cmd_start(message: Message, state: FSMContext, locale: str) -> None:
    """Handle /start for admins: clear FSM and show admin menu keyboard."""
    # Let non-Telegram exceptions bubble to SafeUIMiddleware; avoid broad catches.
    await state.clear()

    lang = await _lang_with_state(state, locale)
    kb = admin_menu_kb(lang)
    # reset navigation stack and show admin menu; nav_reset has its own safe guard
    await nav_reset(state)
    # Use safe_edit which already has internal fallbacks
    await safe_edit(message, text=t("admin_panel_title", lang), reply_markup=kb)

    # Ensure we leave any pending input/edit modes when navigating
    await state.clear()


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

        # Use UserRepo for user lookup
        try:
            user_row = await UserRepo.get_by_telegram_id(target_tid)
        except Exception:
            user_row = None
        # Determine master membership using direct repo resolution (no cache)
        try:
            resolved_mid = await MasterRepo.resolve_master_id(int(target_tid))
            is_master_target = bool(resolved_mid)
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
    text = t("forwarded_user_actions_title", lang).format(
        name=display_name, id=target_tid, status=status
    )

    # Build keyboard with quick actions using structured AdminLookupUserCB.
    # Backward compatibility: if packing fails for any reason, fall back to legacy "__fast__" string.
    kb = InlineKeyboardBuilder()
    kb.button(
        text=t("make_admin_label", lang),
        callback_data=pack_cb(AdminLookupUserCB, action="make_admin", user_id=target_tid),
    )
    if not is_master_target:
        kb.button(
            text=t("make_master_label", lang),
            callback_data=pack_cb(AdminLookupUserCB, action="make_master", user_id=target_tid),
        )
    else:
        kb.button(
            text=t("view_master_bookings_label", lang),
            callback_data=pack_cb(AdminLookupUserCB, action="view_master", user_id=target_tid),
        )
    if user_row:
        kb.button(
            text=t("view_client_bookings_label", lang),
            callback_data=pack_cb(AdminLookupUserCB, action="view_client", user_id=target_tid),
        )
    # Sequential back instead of jump to root
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(2, 2)

    # Present as an edited message when possible; otherwise reply
    if m := _shared_msg(message):
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
        sender_display = getattr(message, "forward_sender_name", None) or tr(
            "unknown_user", lang=locale
        )
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
            kb.button(
                text=t("request_user_start_label", lang),
                url=f"https://t.me/{bot_username}?start=register",
            )
        # Follow nav stack instead of root
        kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
        kb.adjust(1)
        await message.answer(text, reply_markup=kb.as_markup())
    except Exception as e:
        logger.debug("admin_forwarded_privacy_notice failed: %s", e)


# --------------------------- Внутренние хелперы ---------------------------


# Legacy wrapper `_get_msg_obj` removed; use shared `_msg` helper imported
# as `_shared_msg` from `bot.app.services.shared_services` directly.


# Cached metadata about recently forwarded users so callbacks can persist names.
@dataclass(frozen=True)
class ForwardedUserInfo:
    username: str | None
    first_name: str | None
    last_name: str | None


FORWARDED_USER_CACHE_LIMIT = get_env_int("FORWARDED_USER_CACHE_LIMIT", 512)
_forwarded_user_info: OrderedDict[int, ForwardedUserInfo] = OrderedDict()


def _remember_forwarded_user_info(
    tid: int, username: str | None, first_name: str | None, last_name: str | None
) -> None:
    if not tid:
        return
    info = ForwardedUserInfo(username=username, first_name=first_name, last_name=last_name)
    _forwarded_user_info[tid] = info
    _forwarded_user_info.move_to_end(tid)
    while len(_forwarded_user_info) > FORWARDED_USER_CACHE_LIMIT:
        _forwarded_user_info.popitem(last=False)


def _get_forwarded_user_info(tid: int) -> ForwardedUserInfo | None:
    return _forwarded_user_info.get(tid)


# Use `_shared_msg(obj)` (legacy alias `_msg` removed).


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


# (No-op admin decorators removed — access control handled by middleware)


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
    from bot.app.telegram.common.callbacks import (
        ConfirmDelMasterCB,
        ConfirmDelServiceCB,
        GenericConfirmCB,
    )

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
            logger.exception(
                "_show_paginated: failed to build typed confirm callback for %s/%s", typ, key
            )
            cb_payload = None  # Skip unsafe legacy fallback
        if cb_payload:
            kb.inline_keyboard.insert(
                0, [InlineKeyboardButton(text=name, callback_data=cb_payload)]
            )
    await safe_edit(
        _shared_msg(callback),
        f"{title} ({t('page_short', lang)} {page}/{total_pages}):",
        reply_markup=kb,
    )
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
        if m := _shared_msg(callback):
            from bot.app.telegram.client.client_keyboards import get_back_button

            await safe_edit(m, t("no_admins", lang), reply_markup=get_back_button())
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
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1)
    title = t("manage_admins_label", lang)
    if m := _shared_msg(callback):
        await nav_push(state, title, kb.as_markup(), lang=lang)
        await safe_edit(m, title, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(ConfirmDelAdminCB.filter())
async def admin_confirm_del_admin(
    callback: CallbackQuery, callback_data: _HasAdminId, state: FSMContext, locale: str
) -> None:
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
    text = t("confirm_revoke_admin", lang).format(name=user.name)
    if m := _shared_msg(callback):
        await nav_push(state, text, kb.as_markup(), lang=lang)
        await safe_edit(m, text, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(ExecDelAdminCB.filter())
async def admin_exec_del_admin(
    callback: CallbackQuery, callback_data: _HasAdminId, state: FSMContext, locale: str
) -> None:
    """Revoke admin rights for selected DB user id."""
    lang = locale
    admin_id = int(callback_data.admin_id)
    # Prevent self-revocation
    current_tid = callback.from_user.id if callback.from_user else None
    user = await UserRepo.get_by_id(admin_id)
    if not user:
        await callback.answer(t("not_found", locale), show_alert=True)
        return
    if int(getattr(user, "telegram_id", 0) or 0) == int(current_tid or 0):
        await callback.answer(t("cannot_revoke_self", locale), show_alert=True)
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

    text = t("admin_analytics_title", lang)
    if not text or text == "admin_analytics_title":
        text = tr("admin_analytics_title", lang=default_language())
    if not text or text == "admin_analytics_title":
        fallback_analytics = t("analytics", lang)
        if fallback_analytics == "analytics":
            fallback_analytics = tr("analytics", lang=default_language())
        text = fallback_analytics or ""
    kb = analytics_kb(lang)
    if m := _shared_msg(callback):
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_crud"))
async def admin_manage_crud(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show CRUD management submenu (masters/services/linking/prices)."""
    lang = locale
    masters: list[Any] = []
    from bot.app.telegram.admin.admin_keyboards import management_crud_kb

    text = t("admin_menu_manage_crud", lang)
    if not text or text == "admin_menu_manage_crud":
        text = tr("admin_menu_manage_crud", lang=default_language())
    kb = management_crud_kb(lang)
    if m := _shared_msg(callback):
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
    if m := _shared_msg(callback):
        await nav_push(state, t("manage_masters_label", lang), kb, lang=lang)
        await safe_edit(m, t("manage_masters_label", lang), reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_services"))
async def admin_manage_services(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Open services management submenu (Add/Delete)."""
    from bot.app.telegram.admin.admin_keyboards import services_crud_kb

    lang = locale
    kb = services_crud_kb(lang)
    if m := _shared_msg(callback):
        await nav_push(state, t("manage_services_label", lang), kb, lang=lang)
        await safe_edit(m, t("manage_services_label", lang), reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMasterCardCB.filter())
async def admin_show_master_card(
    callback: CallbackQuery, callback_data, state: FSMContext, locale: str
) -> None:
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
            # Resolve canonical currency via shared_services helper to keep
            # a single source of truth and avoid hardcoded fallbacks.
            try:
                from bot.app.services.shared_services import get_global_currency

                currency = await get_global_currency()
            except Exception:
                from bot.app.services.shared_services import _default_currency

                currency = _default_currency()
            revenue_fmt = format_money_cents(int(revenue or 0), currency)
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
            unique_line = f"{t('unique_users', lang)}: {s_unique}"
        except Exception:
            unique_line = f"Унікальних клієнтів: {s_unique}"
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
    # Show bookings specifically for this master (quick view)
    kb.button(
        text=t("admin_master_bookings_button", lang),
        callback_data=pack_cb(AdminLookupUserCB, action="view_master", user_id=int(master_id)),
    )
    # View/manage services linked to this master — reuse SelectViewMasterCB which accepts master_id
    from bot.app.telegram.common.callbacks import SelectViewMasterCB

    kb.button(
        text=t("admin_master_services_button", lang),
        callback_data=pack_cb(SelectViewMasterCB, master_id=int(master_id)),
    )
    # Delete master confirmation
    kb.button(
        text=t("admin_menu_delete_master", lang),
        callback_data=pack_cb(ConfirmDelMasterCB, master_id=int(master_id)),
    )
    # Back from a master card should return to the CRUD management hub
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb.button(text=t("back", lang), callback_data=pack_cb(AdminMenuCB, act="manage_crud"))
    kb.adjust(1)

    if m := _shared_msg(callback):
        await nav_push(state, text, kb.as_markup(), lang=lang)
        await safe_edit(m, text, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_links"))
async def admin_manage_links(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Open links management submenu (Link/Unlink/View)."""
    from bot.app.telegram.admin.admin_keyboards import links_crud_kb

    lang = locale
    kb = links_crud_kb(lang)
    if m := _shared_msg(callback):
        await nav_push(state, t("manage_links_label", lang), kb, lang=lang)
        await safe_edit(m, t("manage_links_label", lang), reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links"))
async def admin_view_links_choice(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Ask admin whether to view links by Master or by Service."""
    lang = locale
    kb = InlineKeyboardBuilder()
    kb.button(
        text=t("by_master", lang), callback_data=pack_cb(AdminMenuCB, act="view_links_master")
    )
    kb.button(
        text=t("by_service", lang), callback_data=pack_cb(AdminMenuCB, act="view_links_service")
    )
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(2, 1)
    m = _shared_msg(callback)
    text = t("admin_view_links_prompt", lang)
    if m:
        await nav_push(state, text, kb.as_markup(), lang=lang)
        await safe_edit(m, text, reply_markup=kb.as_markup())
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links_master"))
async def admin_view_links_by_master(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
    """Show list of masters for admin to pick one to view linked services."""
    lang = locale
    # Use cached masters mapping (fast, avoids direct DB access in handlers)
    try:
        masters_map = await master_services.masters_cache()
        masters = [(int(k), v or f"#{k}") for k, v in masters_map.items()]
    except Exception:
        masters = []

    if not masters:
        await safe_edit(
            _shared_msg(callback), t("no_masters", lang), reply_markup=no_masters_kb(lang)
        )
        await callback.answer()
        return
    kb = masters_list_kb(masters, lang=lang)
    text = t("select_master_to_view_links", lang)
    m = _shared_msg(callback)
    if m:
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "view_links_service"))
async def admin_view_links_by_service(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
    """Show list of services for admin to pick one to view linked masters."""
    lang = locale
    try:
        services_dict = await ServiceRepo.services_cache()
        services = [(sid, name) for sid, name in services_dict.items()]
    except Exception:
        services = []
    if not services:
        await safe_edit(
            _shared_msg(callback), t("no_services", lang), reply_markup=no_services_kb(lang)
        )
        await callback.answer()
        return
    kb = services_select_kb(services, lang=lang)
    text = t("select_service_to_view_links", lang)
    m = _shared_msg(callback)
    if m:
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
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
    await _process_admin_lookup_action(
        action, target_tid, callback, lang, state, forwarded_user=forwarded_info
    )


async def _process_admin_lookup_action(
    action: str,
    target_tid: int,
    callback: CallbackQuery,
    lang: str,
    state: FSMContext,
    forwarded_user: ForwardedUserInfo | None = None,
) -> None:
    """Shared helper for handling forwarded-user quick actions."""

    msg_obj = _shared_msg(callback) or callback.message
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
                await safe_edit(
                    msg_obj,
                    t("make_admin_label", lang) + f" — OK (ID {target_tid})",
                    reply_markup=admin_menu_kb(lang),
                )
        except Exception:
            logger.exception("Failed to create master: %s", target_tid)
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
                await safe_edit(
                    msg_obj,
                    t("make_master_label", lang) + f" — OK (ID {target_tid})",
                    reply_markup=admin_menu_kb(lang),
                )
            else:
                await safe_edit(
                    msg_obj,
                    t("make_master_label", lang) + " — already",
                    reply_markup=admin_menu_kb(lang),
                )
        except Exception:
            logger.exception("Failed to create master: %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    elif action == "view_client":
        try:
            user = await UserRepo.get_by_telegram_id(target_tid)
            if not user:
                await safe_edit(
                    msg_obj,
                    t("view_client_bookings_label", lang) + f" — {t('not_found', lang)}",
                    reply_markup=admin_menu_kb(lang),
                )
            else:
                rows = await BookingRepo.recent_by_user(user.id, limit=10)
                if not rows:
                    await safe_edit(
                        msg_obj,
                        t("view_client_bookings_label", lang) + f" — {t('no_bookings', lang)}",
                        reply_markup=admin_menu_kb(lang),
                    )
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
        # Show the interactive bookings dashboard filtered to a specific master
        try:
            try:
                resolved_mid = await MasterRepo.resolve_master_id(int(target_tid))
            except Exception:
                resolved_mid = None

            if not resolved_mid:
                await safe_edit(
                    msg_obj,
                    t("view_master_bookings_label", lang) + f" — {t('no_bookings', lang)}",
                    reply_markup=admin_menu_kb(lang),
                )
                return

            # Persist master filter and default mode/page in state so pagination works
            try:
                await state.update_data(
                    bookings_mode="upcoming",
                    bookings_page=1,
                    bookings_master_id=int(resolved_mid),
                    preferred_role="admin",
                )
            except Exception:
                # best-effort: at minimum set mode and page
                try:
                    await state.update_data(
                        bookings_mode="upcoming", bookings_page=1, preferred_role="admin"
                    )
                except Exception:
                    pass

            # Build and render the dashboard (delegates filtering to ServiceRepo)
            text, kb = await _build_admin_bookings_view(
                state, lang, mode="upcoming", page=1, master_id=int(resolved_mid)
            )

            try:
                await nav_replace(state, text, kb)
            except Exception:
                try:
                    await nav_replace(state, text, kb, lang=lang)
                except Exception:
                    logger.exception("view_master: nav_replace failed")

            try:
                ok = await safe_edit(msg_obj, text=text, reply_markup=kb)
                if not ok and msg_obj is not None and hasattr(msg_obj, "answer"):
                    new_msg = await msg_obj.answer(text, reply_markup=kb)
                    try:
                        bot_instance = getattr(msg_obj, "bot", None)
                        if bot_instance is not None:
                            await bot_instance.delete_message(
                                chat_id=msg_obj.chat.id, message_id=msg_obj.message_id
                            )
                    except Exception:
                        logger.exception("view_master: bot_instance.delete_message failed")
            except Exception:
                logger.exception("view_master: failed to render bookings dashboard")
        except Exception:
            logger.exception("Failed to list master bookings for %s", target_tid)
            await callback.answer(t("error", lang), show_alert=True)

    else:
        await callback.answer()


@admin_router.callback_query(AdminLookupUserCB.filter())
async def admin_lookup_user_callback(
    callback: CallbackQuery, callback_data: CallbackData, state: FSMContext, locale: str
) -> None:
    """Handle structured callback_data emitted by forwarded-user quick menu."""
    action = getattr(callback_data, "action", "") or ""
    target_tid = int(getattr(callback_data, "user_id", 0) or 0)
    if not action or not target_tid:
        await callback.answer()
        return

    lang = await _lang_with_state(state, locale)
    forwarded_info = _get_forwarded_user_info(target_tid)
    await _process_admin_lookup_action(
        action, target_tid, callback, lang, state, forwarded_user=forwarded_info
    )


@admin_router.callback_query(lambda q: q.data and q.data.startswith("select_view_master"))
async def admin_show_services_for_master(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
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
        text = t("no_services_for_master", lang)
    else:
        # Use master_id as label; avoid cache dependency
        mname = str(master_id)
        # Use localized label for "linked to"
        try:
            lines = [t("master_linked_to", lang).format(name=mname)]
        except Exception:
            lines = [f"{mname} привязана к:"]
        for sid, sname in services:
            lines.append(f" - {sname}")
        text = "\n".join(lines)
    from bot.app.telegram.client.client_keyboards import get_back_button

    await safe_edit(_shared_msg(callback), text, reply_markup=get_back_button())
    await callback.answer()


@admin_router.callback_query(lambda q: q.data and q.data.startswith("select_view_service"))
async def admin_show_masters_for_service(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
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
        text = (
            t("service_no_masters", lang)
            if t("service_no_masters", lang) != "service_no_masters"
            else "Нет мастеров, выполняющих эту услугу."
        )
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
    await safe_edit(_shared_msg(callback), text, reply_markup=get_back_button())
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
        await nav_replace(state, t("admin_panel_title", lang), markup_root, lang=lang)
        # mark preferred role so role-root nav returns here
        await state.update_data(preferred_role="admin")
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
    await nav_reset(state)
    try:
        m = _shared_msg(callback)
        if m and hasattr(m, "edit_text"):
            try:
                await m.edit_text(t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))
                try:
                    await nav_replace(
                        state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang
                    )
                except Exception:
                    logger.debug("nav_replace failed when returning to admin panel")
                await state.update_data(preferred_role="admin")
            except Exception as ee:
                if "message is not modified" in str(ee).lower():
                    logger.debug("Ignored 'message is not modified' when returning to admin panel")
                    try:
                        await nav_replace(
                            state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang
                        )
                    except Exception:
                        logger.debug("nav_replace failed after 'message not modified'")
                    await state.update_data(preferred_role="admin")
                else:
                    logger.debug("Failed to edit admin panel message in-place: %s", ee)
        else:
            await safe_edit(
                _shared_msg(callback),
                t("admin_panel_title", lang),
                reply_markup=admin_menu_kb(lang),
            )
            try:
                await nav_replace(
                    state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang
                )
            except Exception:
                logger.debug("nav_replace failed when returning to admin panel in fallback branch")
            await state.update_data(preferred_role="admin")
    except Exception as e:
        logger.exception("Unexpected error while returning to admin panel: %s", e)
    logger.info("Возврат в админ-панель для пользователя %s", callback.from_user.id)
    await callback.answer()


# --------------------- Управление ценами на услуги ---------------------


async def _render_manage_prices(
    callback: CallbackQuery,
    state: FSMContext,
    lang: str,
    page: int = 1,
    *,
    push: bool = False,
) -> None:
    """Show paginated services list for price editing."""
    from bot.app.core.constants import DEFAULT_PAGE_SIZE

    page_size = DEFAULT_PAGE_SIZE
    try:
        total_count = await ServiceRepo.count_services()
    except Exception:
        services_cache_map = await ServiceRepo.services_cache()
        total_count = len(services_cache_map)

    total_pages = max(1, (total_count + page_size - 1) // page_size)
    if page > total_pages:
        page = total_pages
    if page < 1:
        page = 1

    try:
        services_page = await ServiceRepo.get_services_page(page=page, page_size=page_size)
    except Exception:
        services_cache_map = await ServiceRepo.services_cache()
        all_items = list(services_cache_map.items())
        start = (page - 1) * page_size
        services_page = all_items[start : start + page_size]

    if total_count <= 0 or not services_page:
        msg_text = t("no_services", lang)
        kb = no_services_kb(lang)
        if m := _shared_msg(callback):
            if push:
                await nav_push(state, msg_text, kb, lang=lang)
            await safe_edit(m, msg_text, reply_markup=kb)
        return

    # Use concise selection title (same as other selection menus) and show page indicator
    try:
        title = t("select_service", lang)
        page_label = f" ({t('page_short', lang)} {page}/{total_pages}):" if total_pages > 0 else ""
        text = f"{title} {page_label}"
    except Exception:
        text = f"{t('select_service', lang)} ({t('page_short', lang)} {page}/{total_pages}):"
    kb = services_prices_kb(services_page, page=page, total_pages=total_pages, lang=lang)
    if m := _shared_msg(callback):
        if push:
            await nav_push(state, text, kb, lang=lang)
        await safe_edit(m, text, reply_markup=kb)
    await state.update_data(prices_page=page)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_prices"))
async def admin_manage_prices(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    await _render_manage_prices(callback, state, lang, page=1, push=True)
    await callback.answer()


@admin_router.callback_query(PricePageCB.filter())
async def admin_manage_prices_paginate(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    lang = locale
    try:
        page = max(1, int(getattr(callback_data, "page", 1) or 1))
    except Exception:
        page = 1
    await _render_manage_prices(callback, state, lang, page=page, push=False)
    await callback.answer()


from bot.app.telegram.common.callbacks import (
    AdminEditPriceCB,
    AdminSetPriceCB,
    AdminPriceAdjCB,
    ExecDelServiceCB,
    ConfirmDelServiceCB,
)


@admin_router.callback_query(AdminEditPriceCB.filter())
async def admin_edit_price(
    callback: CallbackQuery, callback_data: _HasServiceId, state: FSMContext, locale: str
) -> None:
    lang = locale
    sid = str(callback_data.service_id)
    svc = await ServiceRepo.get(sid)
    if not svc:
        await callback.answer(t("not_found", lang), show_alert=True)
        return
    price_cents = getattr(svc, "final_price_cents", None) or getattr(svc, "price_cents", None) or 0
    from bot.app.services.shared_services import normalize_currency

    cur_code = normalize_currency(getattr(svc, "currency", None))
    if cur_code:
        currency = cur_code
    else:
        currency = await SettingsRepo.get_currency()
    price_txt = format_money_cents(price_cents, currency)
    text = (
        f"<b>{svc.name}</b>\n"
        f"ID: <code>{svc.id}</code>\n"
        f"{t('current_price', lang)}: {price_txt}"
    )
    if mmsg := _shared_msg(callback):
        kb = edit_price_kb(svc.id, lang)
        await nav_push(state, text, kb, lang=lang)
        await safe_edit(mmsg, text, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminSetPriceCB.filter())
async def admin_set_price(
    callback: CallbackQuery, callback_data: _HasServiceId, state: FSMContext, locale: str
) -> None:
    lang = locale
    sid = str(callback_data.service_id)
    await state.update_data(price_service_id=sid)
    await state.set_state(AdminStates.set_price)
    if msg := _shared_msg(callback):
        try:
            from aiogram.utils.keyboard import InlineKeyboardBuilder

            kb = InlineKeyboardBuilder()
            kb.button(text=tr("cancel", lang=lang), callback_data=pack_cb(NavCB, act="back"))
            await msg.answer(t("enter_price", lang), reply_markup=kb.as_markup())
        except Exception:
            await msg.answer(t("enter_price", lang))
    await callback.answer()


@admin_router.callback_query(AdminPriceAdjCB.filter())
async def admin_price_adjust(
    callback: CallbackQuery, callback_data: _HasServiceDelta, state: FSMContext, locale: str
) -> None:
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
    current_cents = (
        getattr(_svc, "final_price_cents", None) or getattr(_svc, "price_cents", None) or 0
    )
    new_cents = max(0, current_cents + delta_cents)

    svc = await ServiceRepo.update_price_cents(sid, new_cents)
    if not svc:
        await callback.answer(t("error", lang), show_alert=True)
        return
    from bot.app.services.shared_services import normalize_currency

    cur_code = normalize_currency(getattr(svc, "currency", None))
    if cur_code:
        currency = cur_code
    else:
        currency = await SettingsRepo.get_currency()
    price_txt = format_money_cents(new_cents, currency)
    text = (
        f"<b>{svc.name}</b>\n"
        f"ID: <code>{svc.id}</code>\n"
        f"{t('current_price', lang)}: {price_txt}"
    )
    if mmsg := _shared_msg(callback):
        kb = edit_price_kb(sid, lang)
        await safe_edit(mmsg, text, reply_markup=kb)
    await callback.answer(t("price_updated", lang))


@admin_router.message(AdminStates.set_price, F.text)
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
        # Inform user about invalid numeric format and keep state so they can retry
        await message.answer(t("invalid_price_format", lang))
        return
    # Reject negative or abnormally large prices
    # Business rule: allow prices from 0..100000 (in main currency units)
    MAX_PRICE_GRN = 100_000
    if grn < 0 or grn > MAX_PRICE_GRN:
        await message.answer(t("invalid_price_range", lang))
        return

    cents = grn * 100
    svc = await ServiceRepo.update_price_cents(sid, cents)
    if not svc:
        await message.answer(t("error", lang))
        await state.update_data(price_service_id=None)
        return

    # Build updated service card and show edit-price keyboard so admin returns
    # to the price selection/adjust menu after a manual update.
    from bot.app.telegram.admin.admin_keyboards import edit_price_kb
    from bot.app.telegram.common.navigation import nav_replace
    from bot.app.services.shared_services import normalize_currency

    cur_code = normalize_currency(getattr(svc, "currency", None))
    if cur_code:
        currency = cur_code
    else:
        currency = await SettingsRepo.get_currency()
    price_txt = format_money_cents(cents, currency)
    text = (
        f"<b>{svc.name}</b>\n"
        f"ID: <code>{svc.id}</code>\n"
        f"{t('current_price', lang)}: {price_txt}"
    )

    kb = edit_price_kb(sid, lang)
    try:
        await message.answer(text, reply_markup=kb)
    except Exception:
        # As a fallback, still notify success
        await message.answer(t("price_updated", lang))

    try:
        await nav_replace(state, text, kb, lang=lang)
    except Exception:
        # nav state updates are best-effort; don't block the handler on failure
        pass

    await state.clear()


# Per-service currency callback removed: per-service currency is ignored by policy.
# Admins should use global currency setting in Settings -> Currency.


from bot.app.telegram.common.callbacks import AdminSetGlobalCurrencyCB


@admin_router.callback_query(
    AdminSetGlobalCurrencyCB.filter(),
    ~StateFilter(AdminStates.admin_misc),
)
async def admin_set_global_currency(
    callback: CallbackQuery, callback_data: _HasCode, state: FSMContext, locale: str
) -> None:
    """Persist the selected global currency to Settings (DB-first) with strict whitelist."""
    lang = locale
    from bot.app.services.shared_services import normalize_currency

    code_raw = str(getattr(callback_data, "code", "") or "")
    code = normalize_currency(code_raw)
    if not code:
        await callback.answer("Invalid currency", show_alert=True)
        return
    # Delegate policy check to SettingsRepo (centralized business logic).
    try:
        from bot.app.services.admin_services import SettingsRepo

        can_update = await SettingsRepo.can_update_currency()
        if not can_update:
            blocked_msg = tr("currency_change_blocked_provider", lang=lang)
            if blocked_msg == "currency_change_blocked_provider":
                blocked_msg = tr("currency_change_blocked_provider", lang=default_language())
            try:
                await callback.answer(blocked_msg, show_alert=True)
            except Exception:
                try:
                    if (m := _shared_msg(callback)) and getattr(m, "chat", None):
                        await m.answer(blocked_msg)
                except Exception:
                    pass
            return
    except Exception:
        # On error while checking policy, be conservative and block the change.
        try:
            blocked_msg = tr("currency_change_blocked_provider", lang=lang)
            if blocked_msg == "currency_change_blocked_provider":
                blocked_msg = tr("currency_change_blocked_provider", lang=default_language())
            await callback.answer(blocked_msg, show_alert=True)
        except Exception:
            pass
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
        toast = (
            t("currency_saved", lang)
            if t("currency_saved", lang) != "currency_saved"
            else "Currency saved"
        )
    else:
        toast = t("error", lang)
    try:
        if saved:
            await callback.answer(toast)
        else:
            await callback.answer(toast, show_alert=True)
    except Exception as e:
        logger.exception("admin_set_global_currency: callback.answer failed: %s", e)
    # If a payments provider token is configured, warn admin to verify provider supports this currency
    try:
        token = await get_telegram_provider_token()
        if token:
            warn = tr("currency_provider_warning", lang=lang)
            if warn == "currency_provider_warning":
                warn = (
                    f"Вы изменили валюту. Убедитесь, что ваш платежный токен поддерживает {code}."
                )
            # Allow translation strings to include a {currency} or {code} placeholder
            try:
                warn = warn.format(currency=code, code=code)
            except Exception:
                pass
            try:
                await callback.answer(warn, show_alert=True)
            except Exception:
                try:
                    if (m := _shared_msg(callback)) and getattr(m, "chat", None):
                        await m.answer(warn)
                except Exception:
                    pass
    except Exception:
        pass


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings"))
async def admin_show_settings(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show top-level settings categories to reduce UI clutter."""
    lang = locale
    from bot.app.telegram.admin.admin_keyboards import settings_categories_kb

    kb = settings_categories_kb(lang)
    title = t("admin_menu_settings", lang)
    if m := _shared_msg(callback):
        await nav_push(state, title, kb, lang=lang)
        await safe_edit(m, title, reply_markup=kb)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_contacts"))
async def admin_settings_contacts(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Contacts submenu: phone, address, Instagram."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo

        address = await SettingsRepo.get_setting("contact_address", None)
        instagram = await SettingsRepo.get_setting("contact_instagram", None)
        phone = await SettingsRepo.get_setting("contact_phone", None)
        webapp_title = await SettingsRepo.get_setting("webapp_title", None)
        from bot.app.telegram.admin.admin_keyboards import contacts_settings_kb

        kb = contacts_settings_kb(
            lang, phone=phone, address=address, instagram=instagram, webapp_title=webapp_title
        )
        if m := _shared_msg(callback):
            title = t("settings_category_contacts", lang)
            if not title or title == "settings_category_contacts":
                title = tr("settings_category_contacts", lang=default_language())
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_contacts failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@dataclass(frozen=True)
class EditableSettingMeta:
    prompt_key: str
    success_key: str
    validator: Callable[[str], tuple[str | None, str | None]]
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
        validator=admin_services.validate_contact_address,
        invalid_key="invalid_address",
    ),
    "contact_instagram": EditableSettingMeta(
        prompt_key="enter_instagram",
        success_key="instagram_updated",
        validator=validate_instagram_handle,
        invalid_key="invalid_instagram",
    ),
    "webapp_title": EditableSettingMeta(
        prompt_key="enter_webapp_title",
        success_key="webapp_title_updated",
        validator=admin_services.validate_webapp_title,
        invalid_key="invalid_data",
    ),
}


# Business-editable single-value settings (uses same edit flow as contacts)
def validate_discount_percent(value: str) -> tuple[str | None, str | None]:
    try:
        v = int(str(value).strip())
        if v < 0 or v > 100:
            return None, "invalid_data"
        return str(v), None
    except Exception:
        return None, "invalid_data"


EDITABLE_BUSINESS_SETTINGS: dict[str, EditableSettingMeta] = {
    "online_payment_discount_percent": EditableSettingMeta(
        prompt_key="enter_online_discount",
        success_key="online_discount_updated",
        validator=validate_discount_percent,
        invalid_key="invalid_data",
    ),
}


async def _reply_invalid_setting_input(
    message: Message, lang: str, invalid_key: str | None, old_value: str | None
) -> None:
    text = t(invalid_key or "invalid_data", lang)
    try:
        kb = InlineKeyboardBuilder()
        retry_label = t("retry", lang)
        kb.button(
            text=(retry_label if retry_label != "retry" else "Retry"),
            callback_data=pack_cb(NavCB, act="back"),
        )
        if old_value:
            keep_label = t("keep_old", lang)
            kb.button(
                text=f"{keep_label if keep_label != 'keep_old' else 'Keep old'} {old_value}",
                callback_data=pack_cb(NavCB, act="back"),
            )
        kb.adjust(1, 1)
        await message.answer(text, reply_markup=kb.as_markup())
    except Exception:
        await message.answer(text)


async def _refresh_contacts_menu(message: Message, lang: str) -> None:
    phone = await SettingsRepo.get_setting("contact_phone", None)
    address = await SettingsRepo.get_setting("contact_address", None)
    instagram = await SettingsRepo.get_setting("contact_instagram", None)
    webapp_title = await SettingsRepo.get_setting("webapp_title", None)
    kb = contacts_settings_kb(
        lang, phone=phone, address=address, instagram=instagram, webapp_title=webapp_title
    )
    title = t("settings_category_contacts", lang)
    if not title or title == "settings_category_contacts":
        title = tr("settings_category_contacts", lang=default_language())
    await message.answer(title, reply_markup=kb)


async def _refresh_business_menu(message: Message, lang: str) -> None:
    """Send the business settings keyboard (used after editing a business setting).

    This mirrors `admin_settings_business` but sends the keyboard as a direct
    message response (used after interactive edit flows that operate on
    Message events rather than CallbackQuery).
    """
    try:
        from bot.app.services.admin_services import SettingsRepo, load_settings_from_db

        # Refresh runtime settings from DB to avoid stale in-process cache
        try:
            await load_settings_from_db()
        except Exception:
            pass

        telegram_provider_token = await get_telegram_provider_token() or ""
        try:
            payments_enabled = bool(
                await SettingsRepo.get_setting("telegram_payments_enabled", False)
            )
        except Exception:
            payments_enabled = await is_telegram_payments_enabled()
        hold_min = await SettingsRepo.get_reservation_hold_minutes()
        cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
        reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
        reminder_min = await SettingsRepo.get_reminder_lead_minutes()
        same_day_min = await SettingsRepo.get_same_day_lead_minutes()
        expire_sec = await SettingsRepo.get_expire_check_seconds()
        try:
            discount_pct = await SettingsRepo.get_online_payment_discount_percent()
        except Exception:
            discount_pct = 0
        import os

        timezone_val = os.getenv("TIMEZONE") or os.getenv("TZ") or "UTC"
        from bot.app.telegram.admin.admin_keyboards import business_settings_kb

        try:
            mini_now = bool(await SettingsRepo.get_setting("telegram_miniapp_enabled", False))
        except Exception:
            mini_now = await is_telegram_miniapp_enabled()

        kb = business_settings_kb(
            lang,
            telegram_provider_token=telegram_provider_token,
            payments_enabled=payments_enabled,
            miniapp_enabled=mini_now,
            hold_min=hold_min,
            cancel_min=cancel_min,
            reschedule_min=reschedule_min,
            discount_percent=discount_pct,
            reminder_min=reminder_min,
            reminder_same_min=same_day_min,
            expire_sec=expire_sec,
            timezone=timezone_val,
        )
        title = t("settings_category_business", lang)
        if not title or title == "settings_category_business":
            title = tr("settings_category_business", lang=default_language())
        try:
            hint = t("online_discount_hint", lang)
            if hint and hint != "online_discount_hint":
                title = f"{title} — {hint}"
        except Exception:
            pass
        await message.answer(title, reply_markup=kb)
    except Exception:
        try:
            await message.answer(t("error", lang))
        except Exception:
            pass


@admin_router.callback_query(AdminEditSettingCB.filter())
async def admin_edit_contact_setting(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    lang = locale
    setting_key = str(getattr(callback_data, "setting_key", "") or "")
    meta = EDITABLE_CONTACT_SETTINGS.get(setting_key) or EDITABLE_BUSINESS_SETTINGS.get(setting_key)
    if not meta:
        await callback.answer(t("error", lang), show_alert=True)
        return
    try:
        current_value = await SettingsRepo.get_setting(setting_key, None)
    except Exception:
        current_value = None
    await state.update_data(edit_setting_key=setting_key, edit_setting_old=current_value)
    await state.set_state(AdminStates.edit_setting_text)
    prompt = t(meta.prompt_key, lang)
    try:
        kb = InlineKeyboardBuilder()
        kb.button(text=tr("cancel", lang=lang), callback_data=pack_cb(NavCB, act="back"))
        kb.adjust(1)
        if m := _shared_msg(callback):
            await m.answer(prompt, reply_markup=kb.as_markup())
        elif callback.message:
            await callback.message.answer(prompt, reply_markup=kb.as_markup())
    except Exception:
        if m := _shared_msg(callback):
            await m.answer(prompt)
        elif callback.message:
            await callback.message.answer(prompt)
    await callback.answer()


@admin_router.message(AdminStates.edit_setting_text, F.text)
async def admin_edit_setting_input(message: Message, state: FSMContext, locale: str) -> None:
    lang = locale
    data = await state.get_data() or {}
    setting_key = str(data.get("edit_setting_key") or "")
    if not setting_key:
        return
    meta = EDITABLE_CONTACT_SETTINGS.get(setting_key) or EDITABLE_BUSINESS_SETTINGS.get(setting_key)
    if not meta:
        return
    raw = message.text or ""
    value, error_key = meta.validator(raw)
    if value is None or error_key:
        await _reply_invalid_setting_input(
            message,
            lang,
            error_key or meta.invalid_key,
            str(data.get("edit_setting_old")) if data.get("edit_setting_old") else None,
        )
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
    await state.clear()
    # After saving, return to the appropriate settings submenu depending on
    # which editable group the key belongs to.
    if setting_key in EDITABLE_BUSINESS_SETTINGS:
        await _refresh_business_menu(message, lang)
    else:
        await _refresh_contacts_menu(message, lang)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_business"))
async def admin_settings_business(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Business submenu: payments state, hold/cancel menus."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo, load_settings_from_db

        # Refresh runtime settings from DB to avoid stale in-process cache
        try:
            await load_settings_from_db()
        except Exception:
            pass
        telegram_provider_token = await get_telegram_provider_token() or ""
        # Read persisted flags directly from SettingsRepo to ensure latest value
        try:
            payments_enabled = bool(
                await SettingsRepo.get_setting("telegram_payments_enabled", False)
            )
        except Exception:
            payments_enabled = await is_telegram_payments_enabled()
        hold_min = await SettingsRepo.get_reservation_hold_minutes()
        cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
        reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
        reminder_min = await SettingsRepo.get_reminder_lead_minutes()
        same_day_min = await SettingsRepo.get_same_day_lead_minutes()
        expire_sec = await SettingsRepo.get_expire_check_seconds()
        # Discount percent for online payments
        try:
            discount_pct = await SettingsRepo.get_online_payment_discount_percent()
        except Exception:
            discount_pct = 0
        import os

        # Timezone is fixed from environment to avoid runtime drift between admins
        timezone_val = os.getenv("TIMEZONE") or os.getenv("TZ") or "UTC"
        from bot.app.telegram.admin.admin_keyboards import business_settings_kb

        try:
            mini_now = bool(await SettingsRepo.get_setting("telegram_miniapp_enabled", False))
        except Exception:
            mini_now = await is_telegram_miniapp_enabled()

        kb = business_settings_kb(
            lang,
            telegram_provider_token=telegram_provider_token,
            payments_enabled=payments_enabled,
            miniapp_enabled=mini_now,
            hold_min=hold_min,
            cancel_min=cancel_min,
            reschedule_min=reschedule_min,
            discount_percent=discount_pct,
            reminder_min=reminder_min,
            reminder_same_min=same_day_min,
            expire_sec=expire_sec,
            timezone=timezone_val,
        )
        if m := _shared_msg(callback):
            title = t("settings_category_business", lang)
            if not title or title == "settings_category_business":
                title = tr("settings_category_business", lang=default_language())
            # Append a short hint about the online-payment discount so admins
            # immediately understand this setting from the header.
            try:
                hint = t("online_discount_hint", lang)
                if hint and hint != "online_discount_hint":
                    title = f"{title} — {hint}"
            except Exception:
                pass
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_business failed: %s", e)
        await callback.answer(t("error", lang), show_alert=True)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "settings_reminder"))
async def admin_settings_reminder(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show reminder lead-time selection menu."""
    lang = locale
    try:
        from bot.app.services.admin_services import SettingsRepo

        rem = await SettingsRepo.get_reminder_lead_minutes()
        rem_same = await SettingsRepo.get_same_day_lead_minutes()
        kb = admin_reminder_menu_kb(lang, lead_min=rem, same_day_min=rem_same)
        title = t("settings_reminder_desc", lang)
        if m := _shared_msg(callback):
            await nav_push(state, title, kb, lang=lang)
            await safe_edit(m, title, reply_markup=kb)
    except Exception as e:
        logger.exception("admin_settings_reminder failed: %s", e)
        try:
            await callback.answer(t("error", lang), show_alert=True)
        except Exception as e:
            logger.exception("admin_settings_reminder: callback.answer failed: %s", e)
    await callback.answer()


@admin_router.callback_query(AdminSetReminderCB.filter())
async def admin_set_reminder(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Persist selected reminder lead-time (minutes)."""
    lang = locale
    try:
        minutes = int(getattr(callback_data, "minutes", 0) or 0)
    except Exception:
        minutes = 0
    saved = False
    try:
        from bot.app.services.admin_services import SettingsRepo

        # minutes == 0 means disable reminders (persist 0)
        saved = bool(await SettingsRepo.update_setting("reminder_lead_minutes", int(minutes)))
    except Exception:
        saved = False
    if saved:
        if minutes <= 0:
            await callback.answer(
                t("reminder_disabled", lang)
                if t("reminder_disabled", lang) != "reminder_disabled"
                else "Reminders disabled"
            )
        else:
            await callback.answer(
                t("reminder_saved", lang)
                if t("reminder_saved", lang) != "reminder_saved"
                else "Reminder saved"
            )
    else:
        await callback.answer(t("error", lang), show_alert=True)
    # Stay inside reminder menu with refreshed checkmarks
    await admin_settings_reminder(callback, state, locale)
    return


@admin_router.callback_query(AdminSetReminderSameDayCB.filter())
async def admin_set_reminder_same_day(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Persist selected same-day reminder lead-time (minutes)."""
    lang = locale
    try:
        minutes = int(getattr(callback_data, "minutes", 0) or 0)
    except Exception:
        minutes = 0
    saved = False
    try:
        from bot.app.services.admin_services import SettingsRepo

        saved = bool(await SettingsRepo.update_setting("same_day_lead_minutes", int(minutes)))
    except Exception:
        saved = False
    if saved:
        if minutes <= 0:
            await callback.answer(
                t("reminder_same_day_disabled", lang)
                if t("reminder_same_day_disabled", lang) != "reminder_same_day_disabled"
                else "Same-day reminders disabled"
            )
        else:
            await callback.answer(
                t("reminder_same_day_saved", lang)
                if t("reminder_same_day_saved", lang) != "reminder_same_day_saved"
                else "Same-day reminder saved"
            )
    else:
        await callback.answer(t("error", lang), show_alert=True)
    await admin_settings_reminder(callback, state, locale)
    return


# --- Manual currency entry (global)
@admin_router.callback_query(AdminEnterCurrencyCB.filter())
async def admin_enter_currency_callback(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Switch to FSM state for entering a global currency code manually."""
    lang = locale
    # push navigation state so 'back' works predictably
    await nav_push(state, "enter_currency")
    await state.set_state(AdminStates.enter_currency)
    # Prompt with cancel/back keyboard
    kb = InlineKeyboardBuilder()
    kb.button(text=tr("cancel", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1)
    prompt = t("enter_currency_manual_prompt", lang)
    mobj = _shared_msg(callback) or callback.message
    try:
        if mobj:
            await mobj.answer(prompt, reply_markup=kb.as_markup())
        else:
            await callback.message.answer(prompt, reply_markup=kb.as_markup())
    except Exception:
        if mobj:
            await mobj.answer(prompt)
    await callback.answer()


@admin_router.message(AdminStates.enter_currency, F.text)
async def admin_enter_currency_input(message: Message, state: FSMContext, locale: str) -> None:
    """Handle manual global currency input text."""
    lang = locale
    raw = (message.text or "").strip()
    from bot.app.services.shared_services import normalize_currency

    code = normalize_currency(raw)
    if not code:
        await message.answer(t("invalid_data", lang))
        return
    saved = False
    try:
        # Ensure policy allows changing currency (centralized check)
        try:
            can_update = await SettingsRepo.can_update_currency()
            if not can_update:
                msg = tr("currency_change_blocked_provider", lang=lang)
                if msg == "currency_change_blocked_provider":
                    msg = (
                        "Cannot change currency while a payment provider token is configured. "
                        "Remove or update the token in configuration to proceed."
                    )
                await message.answer(msg)
                await state.clear()
                return
        except Exception:
            # Conservative default: block change on error
            try:
                await message.answer(
                    tr("currency_change_blocked_provider", lang=lang)
                    or "Cannot change currency at this time."
                )
            except Exception:
                pass
            await state.clear()
            return

        saved = await SettingsRepo.update_setting("currency", code)
    except Exception:
        saved = False
    if saved:
        await message.answer(t("enter_currency_manual_success", lang).format(currency=code))
        # If provider present, show provider warning
        try:
            token = await get_telegram_provider_token()
            if token:
                warn = tr("currency_provider_warning", lang=lang)
                if warn == "currency_provider_warning":
                    warn = f"You've changed the currency. Ensure your payment provider token supports {code}."
                try:
                    warn = warn.format(currency=code, code=code)
                except Exception:
                    pass
                try:
                    await message.answer(warn)
                except Exception:
                    pass
        except Exception:
            pass
    else:
        await message.answer(t("error", lang))
    await state.clear()


# Per-service currency handlers removed: per-service currency is ignored and
# admins are instructed to change the global currency via settings.


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
            _shared_msg(callback),
            t("exit_message", lang),
            reply_markup=None,
        )
    except Exception:
        # Best-effort: if edit fails, try to send a simple message via bot
        try:
            bot = getattr(callback, "bot", None)
            if bot:
                await bot.send_message(callback.from_user.id, t("exit_message", lang))
        except Exception:
            pass
    logger.info("Выход из админ-панели для пользователя %s", callback.from_user.id)
    await callback.answer()


# --------------------------- Управление записями ---------------------------


@admin_router.callback_query(AdminMenuCB.filter(F.act == "show_bookings"))
async def admin_show_bookings(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню фильтров для просмотра записей.

    Args:
        callback: CallbackQuery для отображения меню.
    """
    # Show the bookings dashboard immediately (same UI as master), rather
    # than the old filter screen. This unifies admin/master UIs.
    if m := _shared_msg(callback):
        lang = locale
        text, kb = await _build_admin_bookings_view(state, lang, mode="upcoming", page=1)
        # persist current mode/page in state
        await state.update_data(bookings_mode="upcoming", bookings_page=1)
        # Ensure role hint is set so NavCB(role_root) returns to admin panel
        await state.update_data(preferred_role="admin")
        try:
            await nav_replace(state, text, kb)
        except Exception as e:
            try:
                await nav_replace(state, text, kb, lang=lang)
            except Exception as e2:
                logger.exception(
                    "admin_show_bookings: nav_replace failed (both attempts): %s / %s", e, e2
                )
        try:
            ok = await safe_edit(m, text=text, reply_markup=kb)
            if not ok:
                msg_obj = getattr(callback, "message", None)
                if msg_obj is not None and hasattr(msg_obj, "answer"):
                    new_msg = await msg_obj.answer(text, reply_markup=kb)
                    try:
                        bot_instance = getattr(msg_obj, "bot", None)
                        if bot_instance is not None:
                            await bot_instance.delete_message(
                                chat_id=msg_obj.chat.id, message_id=msg_obj.message_id
                            )
                    except Exception as e:
                        logger.exception(
                            "admin_show_bookings: bot_instance.delete_message failed: %s", e
                        )
        except Exception:
            logger.exception("force redraw failed in admin_show_bookings")
    logger.info("Дашборд записей показан для пользователя %s", callback.from_user.id)
    await callback.answer()
    logger.info("Дашборд записей показан для пользователя %s", callback.from_user.id)
    await callback.answer()


from bot.app.telegram.common.callbacks import AdminBookingsCB
from bot.app.telegram.common.callbacks import NavCB


@admin_router.callback_query(NavCB.filter(F.act.in_(["root", "back", "role_root"])))
async def admin_nav_clear_state(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Clear FSM on any admin navigation action to avoid input traps, then route.

    This handler mirrors the navigation helpers and should be kept lightweight.
    """
    try:
        await state.clear()
    except Exception as e:
        logger.exception("admin_nav_clear_state: state.clear failed: %s", e)
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
async def admin_bookings_navigate(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
    logger.info(
        "Admin bookings navigate: user=%s mode=%s page=%s",
        getattr(callback.from_user, "id", None),
        mode,
        page,
    )
    await callback.answer()


async def _build_admin_bookings_view(
    state: FSMContext, lang: str, mode: str, page: int, master_id: int | None = None
) -> tuple[str, InlineKeyboardMarkup | None]:
    """Fetch admin bookings data, build dynamic header and keyboard.

    If `master_id` is provided, the result will be filtered to that master.

    Returns (text, markup) where text is dynamic header string and markup is InlineKeyboardMarkup.
    """
    from bot.app.services.admin_services import ServiceRepo
    from bot.app.services.shared_services import format_booking_list_item
    from bot.app.telegram.client.client_keyboards import build_my_bookings_keyboard
    from aiogram.types import InlineKeyboardMarkup

    # If master_id not explicitly provided, check FSM state for a persisted master filter
    if master_id is None:
        try:
            data = await state.get_data()
            if data:
                mid = data.get("bookings_master_id")
                try:
                    master_id = int(mid) if mid is not None else None
                except Exception:
                    master_id = None
        except Exception:
            master_id = None

    rows, meta = await ServiceRepo.get_admin_bookings(
        mode=mode or "upcoming",
        page=int(page or 1),
        page_size=DEFAULT_PAGE_SIZE,
        master_id=master_id,
    )
    # Format bookings inline using shared formatter (admin role)
    formatted_rows: list[tuple[str, int]] = []
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
            "upcoming": (t("upcoming", lang), int(meta.get("upcoming_count", 0) or 0)),
            "done": (t("status_done", lang), int(meta.get("done_count", 0) or 0)),
            "cancelled": (t("status_cancelled", lang), int(meta.get("cancelled_count", 0) or 0)),
            "no_show": (t("no_show_bookings", lang), int(meta.get("noshow_count", 0) or 0)),
            "all": (t("all_bookings", lang), int(meta.get("total", 0) or 0)),
        }
        tab_name, tab_count = mode_map.get(mode_for_header, mode_map["upcoming"])
        page_val = int(meta.get("page", 1) or 1)
        total_pages = int(meta.get("total_pages", 1) or 1)
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
        int(meta.get("done_count", 0) or 0)
        + int(meta.get("cancelled_count", 0) or 0)
        + int(meta.get("noshow_count", 0) or 0)
    )
    kb = await build_my_bookings_keyboard(
        formatted_rows,
        int(meta.get("upcoming_count", 0)),
        completed_count,
        mode or "upcoming",
        int(meta.get("page", 1)),
        lang,
        items_per_page=DEFAULT_PAGE_SIZE,
        cancelled_count=int(meta.get("cancelled_count", 0)),
        noshow_count=int(meta.get("noshow_count", 0)),
        total_pages=total_pages,
        current_page=int(meta.get("page", 1)),
        role="admin",
        master_id=master_id,
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
        csv_path, file_name = await export_month_bookings_csv(mode=mode, reference=now_local)
        # Streamed file path returned; send as FSInputFile to avoid holding large CSV in RAM
        file = FSInputFile(csv_path, filename=file_name)
        m = _shared_msg(callback)
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
        if m := _shared_msg(callback):
            await nav_push(state, t("enter_master_name", lang), None, lang=lang)
            await safe_edit(m, t("enter_master_name", lang))
        logger.info("Начало добавления мастера для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_start: %s", e)
    await callback.answer()


@admin_router.message(AdminStates.add_master_name, F.text)
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


@admin_router.message(AdminStates.add_master_id, F.forward_from)
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
        full_name = (
            getattr(contact, "full_name", None)
            or getattr(contact, "phone_number", None)
            or "Без імені"
        )
        username = getattr(contact, "username", None)
        first_name = getattr(contact, "first_name", None)
        last_name = getattr(contact, "last_name", None)
    elif fwd_user:
        try:
            tg_id = int(getattr(fwd_user, "id", 0) or 0)
        except Exception:
            await message.answer(t("invalid_id", lang))
            return
        full_name = (
            getattr(fwd_user, "full_name", None)
            or getattr(fwd_user, "username", None)
            or "Без імені"
        )
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
            logger.info(
                "Админ %s добавил мастера (forward) %s (%s)", safe_user_id(message), tg_id, name
            )
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


@admin_router.callback_query(AdminMenuCB.filter(F.act == "delete_master"))
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
        await safe_edit(
            _shared_msg(callback), t("no_masters_admin", lang), reply_markup=admin_menu_kb(lang)
        )
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
async def delete_master_paginate(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
            page_items = all_items[start : start + page_size]
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
        logger.info(
            "Пагинация мастеров, страница %d, для пользователя %s", page, callback.from_user.id
        )
    except Exception as e:
        logger.exception("Ошибка пагинации мастеров: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ConfirmDelMasterCB.filter())
async def delete_master_confirm(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
        if m := _shared_msg(callback):
            await nav_push(
                state, t("confirm_master_delete", lang).format(id=mid), kb_markup, lang=lang
            )
            await safe_edit(
                m, t("confirm_master_delete", lang).format(id=mid), reply_markup=kb_markup
            )
        logger.info(
            "Запрос подтверждения удаления мастера %s для пользователя %s",
            mid,
            callback.from_user.id,
        )
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления мастера: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ConfirmCancelAllMasterCB.filter())
async def confirm_cancel_all_master(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Ask admin to confirm cancelling all bookings for a master."""
    try:
        mid = int(callback_data.master_id)
        # Count current active bookings via AdminRepo (no session in handler)
        bids = await admin_services.AdminRepo.get_active_future_booking_ids_for_master(mid)  # type: ignore[attr-defined]
        lang = locale
        kb_markup = confirm_cancel_all_master_kb(mid, linked_count=len(bids), lang=lang)
        prompt = tr("cancel_all_bookings_prompt", lang=lang).format(count=len(bids), master_id=mid)
        if m := _shared_msg(callback):
            await nav_push(state, prompt, kb_markup, lang=lang)
            await safe_edit(m, prompt, reply_markup=kb_markup)
        logger.info(
            "Confirm cancel all bookings for master %s requested by %s", mid, callback.from_user.id
        )
    except Exception as e:
        logger.exception("confirm_cancel_all_master failed: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ExecCancelAllMasterCB.filter())
async def exec_cancel_all_master(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
                text = tr("cancel_all_bookings_dependencies", lang=lang).format(
                    cancelled=cancelled, remaining=len(remaining)
                )
                logger.info(
                    "Mass cancel for master %s completed; remaining dependencies: %s",
                    mid,
                    remaining,
                )
            else:
                # No active/future bookings reference the master; safe to delete the master record via MasterRepo
                deleted = await master_services.MasterRepo.delete_master(mid)  # type: ignore[attr-defined]
                if deleted:
                    text = t("master_deleted", lang)
                    logger.info(
                        "Master %s deleted after mass-cancel by admin %s",
                        mid,
                        safe_user_id(callback),
                    )
                else:
                    text = t("not_found", lang)
        except Exception:
            logger.exception(
                "Failed to finalize master deletion check after mass-cancel for master %s", mid
            )
            lang = locale
            text = t("db_error", lang)

        if m := _shared_msg(callback):
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        logger.exception("exec_cancel_all_master failed: %s", e)
        try:
            lang = locale
        except Exception:
            lang = locale
        if m := _shared_msg(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(ExecDelMasterCB.filter())
async def delete_master_exec(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
                    safe_user_id(callback),
                    mid,
                    blocking,
                )
            else:
                text = t("db_error", lang)
        if m := _shared_msg(callback):
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
            if m := _shared_msg(callback):
                await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
        else:
            logger.exception("Unexpected error in delete_master_exec: %s", e)
    await callback.answer()


@admin_router.callback_query(ConfirmForceDelMasterCB.filter())
async def confirm_force_delete_master(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Show the destructive force-delete confirmation keyboard."""
    try:
        mid = int(callback_data.master_id)
        lang = locale
        kb_markup = confirm_force_delete_master_kb(mid, lang=lang)
        text = t("confirm_force_delete_title", lang)
        if m := _shared_msg(callback):
            await nav_push(state, text, kb_markup, lang=lang)
            await safe_edit(m, text, reply_markup=kb_markup)
    except Exception as e:
        logger.exception("confirm_force_delete_master failed: %s", e)
        if m := _shared_msg(callback):
            await safe_edit(m, t("db_error", locale), reply_markup=admin_menu_kb(locale))
    await callback.answer()


@admin_router.callback_query(ExecForceDelMasterCB.filter())
async def exec_force_delete_master(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Execute physical deletion of master (force delete)."""
    try:
        mid = int(callback_data.master_id)
        # Perform force delete without writing backups as requested
        success, meta = await master_services.MasterRepo.force_delete_master(mid, backup=False)
        lang = locale
        if success:
            text = t("master_force_deleted", lang)
            logger.info(
                "Admin %s force-deleted master %s (meta=%s)", safe_user_id(callback), mid, meta
            )
        else:
            text = t("db_error", lang)
        if m := _shared_msg(callback):
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        logger.exception("exec_force_delete_master failed: %s", e)
        if m := _shared_msg(callback):
            try:
                await safe_edit(m, t("db_error", locale), reply_markup=admin_menu_kb(locale))
            except Exception:
                # If safe_edit itself fails, log and let the error propagate
                logger.exception("Failed to notify admin about exec_force_delete_master failure")
        # Exception logged and best-effort UI fallback performed; do not re-raise
    await callback.answer()


# ----------------------- CRUD услуг ---------------------------


@admin_router.callback_query(AdminMenuCB.filter(F.act == "add_service"))
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
        if m := _shared_msg(callback):
            text = t("enter_service_name", lang)
            await nav_push(state, text, None, lang=lang)
            await safe_edit(m, text)
        logger.info("Начало добавления услуги для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_service_start: %s", e)
    await callback.answer()


@admin_router.message(AdminStates.add_service_name, F.text)
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
    logger.debug(
        "add_service_finish invoked: from=%s cur_state=%r text=%r",
        message.from_user.id,
        cur_state,
        message.text,
    )
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
        await safe_edit(
            _shared_msg(callback), t("no_services_admin", lang), reply_markup=admin_menu_kb(lang)
        )
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
async def delete_service_paginate(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
            page_items = all_items[start : start + page_size]
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
        logger.info(
            "Пагинация услуг, страница %d, для пользователя %s", page, callback.from_user.id
        )
    except Exception as e:
        logger.exception("Ошибка пагинации услуг: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ConfirmDelServiceCB.filter())
async def delete_service_confirm(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
        kb.button(
            text=t("confirm_delete", lang),
            callback_data=pack_cb(ExecDelServiceCB, service_id=str(sid)),
        )
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
        if m := _shared_msg(callback):
            await nav_push(state, message_text, kb_markup, lang=lang)
            await safe_edit(m, message_text, reply_markup=kb_markup)
        logger.info(
            "Запрос подтверждения удаления услуги %s (linked=%d) для пользователя %s",
            sid,
            linked,
            callback.from_user.id,
        )
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления услуги: %s", e)
        lang = locale
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


@admin_router.callback_query(ExecDelServiceCB.filter())
async def delete_service_exec(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
            logger.info(
                "Админ %s удалил услугу %s (unlinked=%d)", safe_user_id(callback), sid, unlinked
            )
            lang = locale
            text = t("service_deleted", lang) + (
                f"\n\nUnlinked from {unlinked} masters." if unlinked else ""
            )
        else:
            lang = locale
            text = t("not_found", lang)
        if m := _shared_msg(callback):
            lang = locale
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в delete_service_exec: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при удалении услуги: %s", e)
            if m := _shared_msg(callback):
                # ensure lang is available
                _lang = locals().get("lang", locale)
                await safe_edit(m, t("db_error", _lang), reply_markup=admin_menu_kb(_lang))
        else:
            logger.exception("Unexpected error in delete_service_exec: %s", e)
    await callback.answer()


# ----------------- Привязка и отвязка мастеров к услугам -----------------


async def _start_master_service_flow(
    callback: CallbackQuery, state: FSMContext, action: str, locale: str
) -> None:
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
        if m := _shared_msg(callback):
            await safe_edit(m, t("no_masters_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    kb = InlineKeyboardBuilder()
    for mid, name in masters.items():
        if action == "link":
            kb.button(text=name, callback_data=pack_cb(SelectLinkMasterCB, master_id=int(mid)))
        else:
            kb.button(text=name, callback_data=pack_cb(SelectUnlinkMasterCB, master_id=int(mid)))
    # Use a Back button (pop nav stack) and push this screen so Back returns
    # to the previous submenu instead of jumping to the root.
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1)
    if m := _shared_msg(callback):
        await nav_push(state, t("select_master", lang), kb.as_markup(), lang=lang)
        await safe_edit(m, t("select_master", lang), reply_markup=kb.as_markup())
    await state.set_state(AdminStates.link_master_service_select_master)
    await state.update_data(action=action)
    logger.info("Начало %s мастера и услуги для пользователя %s", action, callback.from_user.id)
    await callback.answer()


async def _select_master_for_service_flow(
    callback: CallbackQuery,
    state: FSMContext,
    action: str,
    callback_data: Any = None,
    locale: str | None = None,
) -> None:
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
        if m := _shared_msg(callback):
            await safe_edit(
                m,
                t("no_services_linked" if action == "unlink" else "no_services_admin", lang),
                reply_markup=admin_menu_kb(lang),
            )
        await callback.answer()
        return

    kb = InlineKeyboardBuilder()
    for sid, name in services:
        if action == "link":
            kb.button(text=name, callback_data=pack_cb(SelectLinkServiceCB, service_id=str(sid)))
        else:
            kb.button(text=name, callback_data=pack_cb(SelectUnlinkServiceCB, service_id=str(sid)))
    # Show a Back button that returns to the previous step (master selection)
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1)

    if m := _shared_msg(callback):
        await nav_push(state, t("select_service", lang), kb.as_markup(), lang=lang)
        await safe_edit(m, t("select_service", lang), reply_markup=kb.as_markup())
    await state.set_state(AdminStates.link_master_service_select_service)
    logger.info(
        "Выбор услуги для %s мастера %s пользователем %s", action, master_tid, callback.from_user.id
    )
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "link_ms"))
async def link_master_service_start(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
    """Инициирует привязку мастера к услуге.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    await _start_master_service_flow(callback, state, "link", locale)


@admin_router.callback_query(SelectLinkMasterCB.filter())
async def link_master_select(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Обрабатывает выбор мастера для привязки.

    Args:
        callback: CallbackQuery с ID мастера.
        state: Контекст FSM для сохранения состояния.
    """
    await _select_master_for_service_flow(
        callback, state, "link", callback_data=callback_data, locale=locale
    )


@admin_router.callback_query(SelectLinkServiceCB.filter())
async def link_master_finish(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
        linked = await master_services.MasterRepo.link_service(
            master_telegram_id=master_tid_int, service_id=service_id
        )  # type: ignore[attr-defined]
        if linked:
            try:
                invalidate_masters_cache()
            except Exception as e:
                logger.exception("link_master_finish: invalidate_masters_cache failed: %s", e)
            logger.info(
                "Админ %s привязал мастера %s к услуге %s",
                safe_user_id(callback),
                master_tid,
                service_id,
            )
            text = t("link_added", lang)
        else:
            text = t("already_linked", lang)
        await safe_edit(_shared_msg(callback), text, reply_markup=admin_menu_kb(lang))
    except Exception as e:
        if isinstance(e, TelegramAPIError):
            logger.error("Ошибка Telegram API в link_master_finish: %s", e)
        elif isinstance(e, SQLAlchemyError):
            logger.error("Ошибка базы данных при привязке: %s", e)
            if m := _shared_msg(callback):
                _lang = locals().get("lang", locale)
                await safe_edit(m, t("db_error", _lang), reply_markup=admin_menu_kb(_lang))
        else:
            logger.exception("Unexpected error in link_master_finish: %s", e)
    await state.clear()
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "unlink_ms"))
async def unlink_master_service_start(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
    """Инициирует отвязку мастера от услуги.

    Args:
        callback: CallbackQuery для начала процесса.
        state: Контекст FSM для сохранения состояния.
    """
    await _start_master_service_flow(callback, state, "unlink", locale)


@admin_router.callback_query(SelectUnlinkMasterCB.filter())
async def unlink_master_select(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Обрабатывает выбор мастера для отвязки.

    Args:
        callback: CallbackQuery с ID мастера.
        state: Контекст FSM для сохранения состояния.
    """
    await _select_master_for_service_flow(
        callback, state, "unlink", callback_data=callback_data, locale=locale
    )


@admin_router.callback_query(SelectUnlinkServiceCB.filter())
async def unlink_master_finish(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
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
        removed = await master_services.MasterRepo.unlink_service(
            master_telegram_id=master_tid_int, service_id=service_id
        )  # type: ignore[attr-defined]
        if removed:
            try:
                invalidate_masters_cache()
            except Exception as e:
                logger.exception("unlink_master_finish: invalidate_masters_cache failed: %s", e)
            logger.info(
                "Админ %s отвязал мастера %s от услуги %s",
                safe_user_id(callback),
                master_tid,
                service_id,
            )
            text = t("link_removed", lang)
        else:
            text = t("link_not_found", lang)
        await safe_edit(_shared_msg(callback), text, reply_markup=admin_menu_kb(lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при отвязке: %s", e)
        if m := _shared_msg(callback):
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
        mini_enabled = await is_telegram_miniapp_enabled()
        try:
            hold_min = int(await SettingsRepo.get_setting("reservation_hold_minutes", 10) or 10)
        except Exception:
            hold_min = 10
        try:
            cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
        except Exception:
            cancel_min = 180
        try:
            reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
        except Exception:
            reschedule_min = 180
        try:
            expire_sec = int(
                await SettingsRepo.get_setting("reservation_expire_check_seconds", 30) or 30
            )
        except Exception:
            expire_sec = 30
    except Exception:
        token = ""
        enabled = False
        hold_min = 10
        cancel_min = 180
        reschedule_min = 180
        expire_sec = 30

    # Fetch new settings for redesigned UI
    hours_summary = await SettingsRepo.get_setting("working_hours_summary", None)
    try:
        reminder_min = await SettingsRepo.get_reminder_lead_minutes()
    except Exception:
        reminder_min = None
    try:
        reminder_same_min = await SettingsRepo.get_same_day_lead_minutes()
    except Exception:
        reminder_same_min = None
    kb = admin_settings_kb(
        lang,
        telegram_provider_token=token,
        payments_enabled=enabled,
        miniapp_enabled=mini_enabled,
        hold_min=hold_min,
        cancel_min=cancel_min,
        reschedule_min=reschedule_min,
        hours_summary=hours_summary,
        reminder_min=reminder_min,
        reminder_same_min=reminder_same_min,
        expire_sec=expire_sec,
    )
    msg = _shared_msg(callback)
    title = t("settings_category_business", lang) or t("admin_menu_settings", lang)
    if msg:
        await nav_push(state, title, kb, lang=lang)
        await safe_edit(msg, title, reply_markup=kb)
    else:
        if callback.message:
            await callback.message.answer(title, reply_markup=kb)
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
                await callback.answer(
                    f"✅ {t('expire_check_frequency', lang) if t('expire_check_frequency', lang) != 'expire_check_frequency' else 'Frequency updated'}: каждые {label}"
                )
            except Exception as e:
                logger.exception(
                    "apply_setting_change: primary callback.answer failed for expire_check_frequency: %s",
                    e,
                )
                try:
                    await callback.answer(f"✅ Частота проверки обновлена: каждые {label}")
                except Exception as e2:
                    logger.exception(
                        "apply_setting_change: secondary callback.answer failed for expire_check_frequency: %s",
                        e2,
                    )
        elif key == "reservation_hold_minutes":
            minutes = int(value)
            try:
                await callback.answer(t("hold_label", lang).format(minutes=minutes))
            except Exception as e:
                logger.exception(
                    "apply_setting_change: callback.answer failed for reservation_hold_minutes: %s",
                    e,
                )
                try:
                    await callback.answer(f"✅ hold minutes set: {minutes}")
                except Exception as e2:
                    logger.exception(
                        "apply_setting_change: secondary callback.answer failed for reservation_hold_minutes: %s",
                        e2,
                    )
        elif key in {"client_cancel_lock_minutes", "client_reschedule_lock_minutes"}:
            minutes = int(value)
            label = format_minutes_short(minutes, lang)
            try:
                if key == "client_cancel_lock_minutes":
                    await callback.answer(t("cancel_lock_label", lang).format(minutes=label))
                else:
                    await callback.answer(t("reschedule_lock_label", lang).format(minutes=label))
            except Exception as e:
                logger.exception("apply_setting_change: callback.answer failed for %s: %s", key, e)
                try:
                    await callback.answer(f"✅ {key} set: {minutes}")
                except Exception as e2:
                    logger.exception(
                        "apply_setting_change: secondary callback.answer failed for %s: %s", key, e2
                    )
        else:
            try:
                await callback.answer(t("settings_saved", lang))
            except Exception as e:
                logger.exception(
                    "apply_setting_change: callback.answer failed for settings_saved: %s", e
                )
                try:
                    await callback.answer("✅ Saved")
                except Exception as e2:
                    logger.exception(
                        "apply_setting_change: secondary callback.answer failed for settings_saved: %s",
                        e2,
                    )
    except Exception as e:
        logger.exception(
            "apply_setting_change: unexpected error when applying setting %s=%s: %s", key, value, e
        )
        try:
            await callback.answer(t("settings_saved", locale))
        except Exception as e2:
            logger.exception("apply_setting_change: fallback callback.answer failed: %s", e2)
            try:
                await callback.answer("✅ Saved")
            except Exception as e3:
                logger.exception(
                    "apply_setting_change: final fallback callback.answer failed: %s", e3
                )

    return True


@admin_router.callback_query(AdminMenuCB.filter(F.act == "toggle_telegram_payments"))
async def admin_toggle_telegram_payments_handler(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
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
                from bot.app.services.shared_services import (
                    is_telegram_payments_enabled as _is_enabled,
                )

                enabled_now = bool(_is_enabled())
            except Exception:
                enabled_now = False
            from bot.app.telegram.admin.admin_keyboards import business_settings_kb

            hold_min = None
            cancel_min = None
            reschedule_min = None
            try:
                hold_min = await SettingsRepo.get_reservation_hold_minutes()
            except Exception as e:
                logger.exception(
                    "admin_toggle_payments: get_reservation_hold_minutes failed: %s", e
                )
            try:
                cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
            except Exception as e:
                logger.exception(
                    "admin_toggle_payments: get_client_cancel_lock_minutes failed: %s", e
                )
            try:
                reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
            except Exception as e:
                logger.exception(
                    "admin_toggle_payments: get_client_reschedule_lock_minutes failed: %s", e
                )
            try:
                expire_sec = await SettingsRepo.get_expire_check_seconds()
            except Exception:
                expire_sec = None
            kb = business_settings_kb(
                lang,
                telegram_provider_token=token,
                payments_enabled=enabled_now,
                discount_percent=(
                    await SettingsRepo.get_online_payment_discount_percent()
                    if hasattr(SettingsRepo, "get_online_payment_discount_percent")
                    else 0
                ),
                hold_min=hold_min,
                cancel_min=cancel_min,
                reschedule_min=reschedule_min,
                expire_sec=expire_sec,
            )
            msg = _shared_msg(callback)
            if msg:
                title = t("settings_category_business", lang)
                if not title or title == "settings_category_business":
                    title = tr("settings_category_business", lang=default_language())
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
        cancel_min = None
        reschedule_min = None
        reminder_min = None
        same_day_min = None
        mini_now = False
        try:
            hold_min = await SettingsRepo.get_reservation_hold_minutes()
        except Exception as e:
            logger.exception(
                "admin_toggle_payments (refresh): get_reservation_hold_minutes failed: %s", e
            )
        try:
            cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
        except Exception as e:
            logger.exception(
                "admin_toggle_payments (refresh): get_client_cancel_lock_minutes failed: %s", e
            )
        try:
            reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
        except Exception as e:
            logger.exception(
                "admin_toggle_payments (refresh): get_client_reschedule_lock_minutes failed: %s", e
            )
        try:
            reminder_min = await SettingsRepo.get_reminder_lead_minutes()
        except Exception:
            reminder_min = None
        try:
            same_day_min = await SettingsRepo.get_same_day_lead_minutes()
        except Exception:
            same_day_min = None
        try:
            mini_now = bool(await SettingsRepo.get_setting("telegram_miniapp_enabled", False))
        except Exception:
            mini_now = False
        try:
            expire_sec = await SettingsRepo.get_expire_check_seconds()
        except Exception:
            expire_sec = None
        kb = business_settings_kb(
            lang,
            telegram_provider_token=token_now,
            payments_enabled=payments_now,
            discount_percent=(
                await SettingsRepo.get_online_payment_discount_percent()
                if hasattr(SettingsRepo, "get_online_payment_discount_percent")
                else 0
            ),
            miniapp_enabled=mini_now,
            hold_min=hold_min,
            cancel_min=cancel_min,
            reschedule_min=reschedule_min,
            reminder_min=reminder_min,
            reminder_same_min=same_day_min,
            expire_sec=expire_sec,
        )
        msg = _shared_msg(callback)
        if msg:
            title = t("settings_category_business", lang)
            if not title or title == "settings_category_business":
                title = tr("settings_category_business", lang=default_language())
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


@admin_router.callback_query(AdminMenuCB.filter(F.act == "toggle_telegram_miniapp"))
async def admin_toggle_telegram_miniapp_handler(
    callback: CallbackQuery, state: FSMContext, locale: str
) -> None:
    """Toggle Telegram MiniApp booking feature flag from admin UI."""
    user_id = callback.from_user.id
    logger.info("Переключение Telegram MiniApp для пользователя %s", user_id)
    lang = locale
    try:
        new_val = await toggle_telegram_miniapp()
        status = t("enabled", lang) if new_val else t("disabled", lang)
        logger.info("Админ %s переключил Telegram MiniApp на %s", user_id, status)
        await callback.answer(t("miniapp_toggled", lang).format(status=status))

        # Rebuild admin settings view to reflect new state
        try:
            token = (await get_telegram_provider_token()) or ""
        except Exception:
            token = ""
        try:
            payments_now = await is_telegram_payments_enabled()
        except Exception:
            payments_now = False
        # Try to reload runtime settings from DB to reduce stale-cache races
        # in this process. Then read the persisted flag as the single source
        # of truth for rendering the keyboard. Fall back to `new_val` if
        # DB read fails.
        try:
            from bot.app.services.admin_services import load_settings_from_db, SettingsRepo

            try:
                await load_settings_from_db()
            except Exception:
                # best-effort reload; ignore failures
                pass
            try:
                mini_now = bool(await SettingsRepo.get_setting("telegram_miniapp_enabled", False))
            except Exception:
                mini_now = bool(new_val)
        except Exception:
            mini_now = bool(new_val)

        try:
            hold_min = int(await SettingsRepo.get_setting("reservation_hold_minutes", 10) or 10)
        except Exception:
            hold_min = 10
        try:
            cancel_min = await SettingsRepo.get_client_cancel_lock_minutes()
        except Exception:
            cancel_min = 180
        try:
            reschedule_min = await SettingsRepo.get_client_reschedule_lock_minutes()
        except Exception:
            reschedule_min = 180
        try:
            expire_sec = int(
                await SettingsRepo.get_setting("reservation_expire_check_seconds", 30) or 30
            )
        except Exception:
            expire_sec = 30

        hours_summary = await SettingsRepo.get_setting("working_hours_summary", None)
        try:
            reminder_min = await SettingsRepo.get_reminder_lead_minutes()
        except Exception:
            reminder_min = None
        try:
            reminder_same_min = await SettingsRepo.get_same_day_lead_minutes()
        except Exception:
            reminder_same_min = None

        # Rebuild the Business settings keyboard (keep same ordering as Payments toggle)
        from bot.app.telegram.admin.admin_keyboards import business_settings_kb

        kb = business_settings_kb(
            lang,
            telegram_provider_token=token,
            payments_enabled=payments_now,
            discount_percent=(
                await SettingsRepo.get_online_payment_discount_percent()
                if hasattr(SettingsRepo, "get_online_payment_discount_percent")
                else 0
            ),
            miniapp_enabled=mini_now,
            hold_min=hold_min,
            cancel_min=cancel_min,
            reschedule_min=reschedule_min,
            reminder_min=reminder_min,
            reminder_same_min=reminder_same_min,
            expire_sec=expire_sec,
        )
        msg = _shared_msg(callback)
        title = t("settings_category_business", lang) or t("admin_menu_settings", lang)
        if msg:
            # Replace current settings screen to avoid adding duplicate nav entries
            await nav_replace(state, title, kb, lang=lang)
            await safe_edit(msg, title, reply_markup=kb)
        else:
            if callback.message:
                await callback.message.answer(title, reply_markup=kb)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_toggle_telegram_miniapp_handler: %s", e)
        await callback.answer(t("telegram_error", lang))
    except Exception as e:
        logger.exception("Неожиданная ошибка в admin_toggle_telegram_miniapp_handler: %s", e)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "hold_menu"))
async def admin_hold_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора времени удержания резерва."""
    # Business logic: let exceptions bubble to global handler.
    if m := getattr(callback, "message", None):
        lang = locale
        try:
            cur_hold = await SettingsRepo.get_reservation_hold_minutes()
        except Exception:
            cur_hold = None
        kb = admin_hold_menu_kb(lang, current_min=cur_hold)
        text = t("hold_desc", lang)
        await nav_push(state, text, kb, lang=lang)
        # Only catch Telegram errors for the editing call
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_hold_menu")
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "cancel_menu"))
async def admin_cancel_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора окна запрета отмены (в минутах)."""
    # Business logic: let exceptions bubble to global handler.
    if m := getattr(callback, "message", None):
        lang = locale
        try:
            cur_cancel = await SettingsRepo.get_client_cancel_lock_minutes()
        except Exception:
            cur_cancel = None
        kb = admin_cancel_menu_kb(lang, current_min=cur_cancel)
        text = t("cancel_desc", lang)
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_cancel_menu")
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "reschedule_menu"))
async def admin_reschedule_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора окна запрета переноса (в минутах)."""
    if m := getattr(callback, "message", None):
        lang = locale
        try:
            cur_reschedule = await SettingsRepo.get_client_reschedule_lock_minutes()
        except Exception:
            cur_reschedule = None
        kb = admin_reschedule_menu_kb(lang, current_min=cur_reschedule)
        text = t("reschedule_desc", lang)
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_reschedule_menu")
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "expire_menu"))
async def admin_expire_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора частоты проверки просроченных броней (в секундах)."""
    if m := getattr(callback, "message", None):
        lang = locale
        try:
            cur_expire = await SettingsRepo.get_expire_check_seconds()
        except Exception:
            cur_expire = None
        kb = admin_expire_menu_kb(lang, current_expire=cur_expire)
        text = t("expire_check_desc", lang)
        await nav_push(state, text, kb, lang=lang)
        try:
            await safe_edit(m, text, reply_markup=kb)
        except TelegramAPIError:
            logger.exception("Telegram error while editing message in admin_expire_menu")
    await callback.answer()


@admin_router.callback_query(AdminSetExpireCB.filter())
async def admin_set_expire(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Set reservation_expire_check_seconds and refresh settings UI via admin_settings."""
    # Let failures propagate to global error handler; parse input and perform update.
    seconds = int(callback_data.seconds)
    await SettingsRepo.update_setting("reservation_expire_check_seconds", seconds)
    await admin_expire_menu(callback, state, locale)


@admin_router.callback_query(AdminSetHoldCB.filter())
async def admin_set_hold(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Set reservation_hold_minutes and refresh settings UI via admin_settings."""
    minutes = int(callback_data.minutes)
    await SettingsRepo.update_setting("reservation_hold_minutes", minutes)
    await admin_hold_menu(callback, state, locale)


@admin_router.callback_query(AdminSetCancelCB.filter())
async def admin_set_cancel_lock(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Set client_cancel_lock_minutes and refresh settings UI via admin_settings."""
    minutes = int(callback_data.minutes)
    await SettingsRepo.update_setting("client_cancel_lock_minutes", minutes)
    await admin_cancel_menu(callback, state, locale)


@admin_router.callback_query(AdminSetRescheduleCB.filter())
async def admin_set_reschedule_lock(
    callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str
) -> None:
    """Set client_reschedule_lock_minutes and refresh settings UI via admin_settings."""
    minutes = int(callback_data.minutes)
    await SettingsRepo.update_setting("client_reschedule_lock_minutes", minutes)
    await admin_reschedule_menu(callback, state, locale)


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
    if m := _shared_msg(callback):
        await safe_edit(m, body, reply_markup=reply_markup)
    logger.info("Статистика '%s' отправлена для пользователя %s", title, callback.from_user.id)


@admin_router.callback_query(AdminMenuCB.filter(F.act == "stats"))
async def show_stats_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    lang = locale
    totals = await AdminRepo.get_basic_totals()
    text = (
        f"{t('total_bookings', lang)}: {totals.get('total_bookings', 0)}\n"
        f"{t('total_users', lang)}: {totals.get('total_users', 0)}\n"
        f"{t('select_filter', lang)}"
    )
    markup = stats_menu_kb(lang)  # Добавил переменную для удобства
    if m := _shared_msg(callback):
        await safe_edit(m, text, reply_markup=markup)
        # Push analytics->stats onto the nav stack so Back returns to analytics
        await nav_push(state, text, markup, lang=lang)
    logger.info("Меню статистики показано для пользователя %s", callback.from_user.id)
    await callback.answer(cache_time=1, show_alert=False)


@admin_router.callback_query(
    AdminMenuCB.filter(F.act.in_({"stats_range_week", "stats_range_month"}))
)
async def show_stats_range(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    kind = "week" if "week" in (callback.data or "") else "month"
    lang = locale
    # Compute explicit bounds so we can compute previous-period trends
    start, end = admin_services._range_bounds(kind)
    stats = await admin_services._stats_for_bounds(start, end)
    # previous contiguous window
    delta = end - start
    prev_start = start - delta
    prev_end = start
    prev_stats = await admin_services._stats_for_bounds(prev_start, prev_end)

    title = f"📈 {t('stats_week', lang) if kind == 'week' else t('stats_month', lang)}"

    bookings_trend = admin_services._format_trend_text(
        stats.get("bookings", 0), prev_stats.get("bookings", 0), lang=lang
    )
    users_trend = admin_services._format_trend_text(
        stats.get("unique_users", 0), prev_stats.get("unique_users", 0), lang=lang
    )

    lines = [
        title,
        f"{t('bookings', lang)}: {stats.get('bookings', 0)}{bookings_trend}",
        f"{t('unique_users', lang)}: {stats.get('unique_users', 0)}{users_trend}",
        f"{t('masters', lang)}: {stats.get('masters', 0)}",
        f"{t('avg_per_day', lang)}: {stats.get('avg_per_day', 0):.1f}",
    ]

    # For month view, also include revenue + trend
    if kind == "month":
        # Show split revenue: in-cash vs expected
        rev_split = await admin_services._revenue_split_for_bounds(start, end)
        prev_rev_split = await admin_services._revenue_split_for_bounds(prev_start, prev_end)
        in_cash = rev_split.get("in_cash", 0)
        expected = rev_split.get("expected", 0)
        prev_in_cash = prev_rev_split.get("in_cash", 0)
        prev_expected = prev_rev_split.get("expected", 0)
        in_cash_trend = admin_services._format_trend_text(in_cash, prev_in_cash, lang=lang)
        expected_trend = admin_services._format_trend_text(expected, prev_expected, lang=lang)
        in_cash_txt = format_money_cents(in_cash)
        expected_txt = format_money_cents(expected)
        lines.insert(
            2, f"{t('admin_dashboard_revenue_in_cash', lang)}: {in_cash_txt}{in_cash_trend}"
        )
        lines.insert(
            3, f"{t('admin_dashboard_revenue_expected', lang)}: {expected_txt}{expected_trend}"
        )

    text = "\n".join(lines)
    markup = stats_menu_kb(lang)
    if m := _shared_msg(callback):
        await safe_edit(m, text, reply_markup=markup)
        # Push the stats view so Back pops back to the analytics menu
        await nav_push(state, text, markup, lang=lang)
    logger.info("Статистика за %s отображена для пользователя %s", kind, callback.from_user.id)
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
        if m := _shared_msg(callback):
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
        if m := _shared_msg(callback):
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
        if m := _shared_msg(callback):
            lang = locale

            # Fetch key business metrics for the last 30 days (month range)
            try:
                # Compute explicit bounds for month (last-30-days) and show trend vs previous window
                start, end = admin_services._range_bounds("month")
                revenue_month = await admin_services._revenue_for_bounds(start, end)
                prev_start = start - (end - start)
                prev_end = start
                prev_revenue_month = await admin_services._revenue_for_bounds(prev_start, prev_end)
            except Exception:
                revenue_month = 0
                prev_revenue_month = 0
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

            # Use localized summary title and localized labels without emojis.
            title = t("biz_summary_title", lang) or "Business summary (last 30 days)"
            revenue_trend = admin_services._format_trend_text(
                revenue_month, prev_revenue_month, lang=lang
            )
            revenue_line = t("admin_dashboard_revenue", lang).format(amount=revenue_txt) + (
                revenue_trend or ""
            )
            retention_line = (
                f"{t('admin_dashboard_retention', lang)} {retention_month.get('rate', 0) * 100:.1f}% "
                f"({retention_month.get('repeaters', 0)}/{retention_month.get('total', 0)})"
            )
            noshow_line = (
                f"{t('admin_dashboard_no_shows', lang)} {noshow_month.get('rate', 0) * 100:.1f}% "
                f"({noshow_month.get('no_show', 0)}/{noshow_month.get('total', 0)})"
            )

            # Lost revenue due to cancellations / no-shows
            try:
                lost = await admin_services._lost_revenue_for_bounds(start, end)
                prev_lost = await admin_services._lost_revenue_for_bounds(prev_start, prev_end)
                lost_txt = format_money_cents(lost)
                lost_trend = admin_services._format_trend_text(lost, prev_lost, lang=lang)
                lost_line = t("admin_dashboard_lost_revenue", lang).format(amount=lost_txt) + (
                    lost_trend or ""
                )
            except Exception:
                lost_line = t("admin_dashboard_lost_revenue", lang).format(amount="0")

            # Average order value (AOV) and trend
            try:
                revenue_count = await admin_services._revenue_count_for_bounds(start, end)
                prev_revenue_count = await admin_services._revenue_count_for_bounds(
                    prev_start, prev_end
                )
                aov_cents = int(revenue_month // revenue_count) if revenue_count else 0
                prev_aov_cents = (
                    int(prev_revenue_month // prev_revenue_count) if prev_revenue_count else 0
                )
                try:
                    aov_txt = format_money_cents(aov_cents)
                except Exception:
                    aov_txt = str(aov_cents)
                aov_trend = admin_services._format_trend_text(aov_cents, prev_aov_cents, lang=lang)
                aov_line = t("admin_dashboard_aov", lang).format(amount=aov_txt) + (aov_trend or "")
            except Exception:
                aov_line = t("admin_dashboard_aov", lang).format(amount="0")

            text = (
                f"<b>{title}</b>\n\n"
                f"{revenue_line}\n"
                f"{aov_line}\n"
                f"{retention_line}\n"
                f"{noshow_line}\n"
                f"{lost_line}\n\n"
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
        if m := _shared_msg(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_revenue"))
async def admin_quick_revenue(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Delegate quick revenue button to the biz revenue handler."""
    try:
        await admin_biz_revenue(callback, state, locale)
    except Exception:
        lang = locale
        if m := _shared_msg(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_retention"))
async def admin_quick_retention(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Delegate quick retention button to the biz retention handler."""
    try:
        await admin_biz_retention(callback, state, locale)
    except Exception:
        lang = locale
        if m := _shared_msg(callback):
            await safe_edit(m, t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "quick_compare"))
async def admin_quick_compare(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Quick compare delegates to the business analytics menu to choose ranges."""
    try:
        await admin_biz_menu(callback, state, locale)
    except Exception:
        lang = locale
        if m := _shared_msg(callback):
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

        if m := _shared_msg(callback):
            # только обновляем сообщение, остаёмся в бизнес‑меню
            body = "\n".join(lines)
            logger.debug(
                "admin_biz_revenue: editing message with %d chars, preview: %s",
                len(body),
                body[:200],
            )
            await safe_edit(m, body, reply_markup=biz_menu_kb(lang))
        logger.info("Статистика выручки отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_revenue: %s", e)
        if m := _shared_msg(callback):
            logger.debug("admin_biz_revenue: encountered exception, sending error text to message")
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
        if m := _shared_msg(callback):
            await safe_edit(m, "\n".join(lines), reply_markup=biz_menu_kb(lang))
        logger.info("Статистика удержания отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_retention: %s", e)
        if m := _shared_msg(callback):
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
        if m := _shared_msg(callback):
            await safe_edit(m, "\n".join(lines), reply_markup=biz_menu_kb(lang))
        logger.info("Статистика no-show отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_no_show: %s", e)
        if m := _shared_msg(callback):
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
            try:
                from bot.app.services.shared_services import get_global_currency

                default_currency = await get_global_currency()
            except Exception:
                from bot.app.services.shared_services import _default_currency

                default_currency = _default_currency()
        except Exception:
            default_currency = ""

        for row in topc:
            if not all(key in row for key in ["name", "revenue_cents", "bookings"]):
                logger.warning("Некорректная структура данных в get_top_clients_ltv: %s", row)
                continue
            money = format_money_cents(row["revenue_cents"], row.get("currency", default_currency))
            formatted_data.append(
                {
                    "name": row["name"],
                    "money": money,
                    "bookings": row["bookings"],
                    "bookings_short": t("bookings_short", lang),
                }
            )

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
        if m := _shared_msg(callback):
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
    if new_locale not in ["uk", "en", "ru"]:
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
            logger.info(
                "Админ %s установил глобальную локаль %s", safe_user_id(message), new_locale
            )
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

            await message.reply(
                t("user_locale_set_fmt", lang).format(action=action, id=tid, locale=new_locale)
            )
            logger.info(
                "Админ %s установил локаль %s для пользователя %s",
                safe_user_id(message),
                new_locale,
                tid,
            )
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
