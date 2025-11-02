from __future__ import annotations
import logging
import re
from typing import Any, Optional, cast
from bot.app.telegram.common.callbacks import (
    pack_cb,
    BookingsPageCB,
    DelMasterPageCB,
    ConfirmDelMasterCB,
    ExecDelMasterCB,
    DelServicePageCB,
    SelectLinkMasterCB,
    SelectLinkServiceCB,
    SelectUnlinkMasterCB,
    SelectUnlinkServiceCB,
    AdminSetHoldCB,
    AdminSetCancelCB,
    AdminSetExpireCB,
    AdminMenuCB,
)

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramAPIError
from sqlalchemy import select, delete
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, Master, MasterService, Service, User
from bot.app.services.admin_services import (
    get_basic_totals, get_range_stats, get_top_masters, get_top_services,
    get_revenue_total, get_revenue_by_master, get_revenue_by_service,
    get_retention, get_no_show_rates, get_top_clients_ltv,
    get_conversion, get_cancellations, get_daily_trends, get_aov,
)
from bot.app.services.shared_services import (
    toggle_telegram_payments,
    format_money_cents,
    get_telegram_provider_token,
    get_service_name,
    _msg as _shared_msg,
    safe_user_id,
    _safe_call,
    services_cache,
    masters_cache,
    invalidate_services_cache,
    invalidate_masters_cache,
)
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bot.app.services.admin_services import update_service_price_cents
from bot.app.translations import t
from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.roles import ensure_admin, AdminRoleFilter
from bot.app.telegram.admin.admin_keyboards import (
    admin_menu_kb, admin_settings_kb, admin_hold_menu_kb, pagination_kb,
    show_bookings_filter_kb, stats_menu_kb, biz_menu_kb,
    services_list_kb, edit_price_kb,
    admin_cancel_menu_kb,
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
from bot.app.telegram.client.client_keyboards import get_back_button
from bot.app.telegram.admin.states import AdminStates
import bot.config as cfg
from aiogram.types import BufferedInputFile

# Local text dictionary & helpers (static analyzer friendly)
logger = logging.getLogger(__name__)

admin_router = Router(name="admin")
# Attach locale middleware so handlers receive `locale` via data injection
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
admin_router.message.middleware(LocaleMiddleware())
admin_router.callback_query.middleware(LocaleMiddleware())
# Centralized router-level error handler will receive uncaught exceptions
# from handlers and can notify admins, log, etc.
try:
    admin_router.errors.register(handle_telegram_error)
except Exception:
    # best-effort registration; if aiogram version differs, ignore
    logger.debug("Router error handler registration skipped or unsupported in this aiogram version")
# Apply AdminRoleFilter at router level so individual handlers don't need to
# perform explicit role checks. The filter delegates to `ensure_admin` which
# sends localized denial messages when access is denied.
admin_router.message.filter(AdminRoleFilter())
# Also filter callback queries so callback handlers are protected as well.
admin_router.callback_query.filter(AdminRoleFilter())
# Access control is enforced by the router-level AdminRoleFilter.

# Local timezone for admin date/time display
LOCAL_TZ = getattr(cfg, "LOCAL_TZ", ZoneInfo("Europe/Kyiv"))

# --------------------------- Внутренние хелперы ---------------------------


def _get_msg_obj(obj: Any) -> Message | None:
    """Return the underlying message object for a callback or message.

    This delegates to the shared helper `_shared_msg` to keep behaviour stable.
    """
    return _shared_msg(obj)

# Note: prefer calling _get_msg_obj(obj) directly. The legacy alias _msg was removed
# to encourage consistent usage across the admin module.

# Note: legacy alias `_msg` removed. Use `_get_msg_obj(obj)` to obtain a Message object.


def _extract_user_id_from_ctx(obj: Any) -> int:
    """Safely extract the Telegram user id from CallbackQuery/Message-like objects."""
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or 0)
    except Exception:
        return 0


# admin_handler and admin_safe removed: routing-level AdminRoleFilter and
# LocaleMiddleware now provide access control and locale injection. Error
# handling is centralized via router error handler using
# `bot.app.telegram.common.errors.handle_telegram_error` and message edits
# should uses `safe_edit` directly inside handlers when needed.


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


# ensure_admin centralized in bot.app.telegram.common.roles


# services_cache and masters_cache moved to shared_services


async def _show_paginated(
    callback: CallbackQuery, state: FSMContext, total_pages: int, title: str, prefix: str, lang: str = "uk"
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
    page = data.get("delete_page", 1)
    items = data.get("delete_items", [])
    typ = data.get("delete_type", "item")
    paginated = items[(page - 1) * 10:page * 10]
    kb = pagination_kb(prefix, page, total_pages, lang)
    # Map common delete types to typed confirm CallbackData classes
    from bot.app.telegram.common.callbacks import ConfirmDelMasterCB, ConfirmDelServiceCB
    for key, name in paginated:
        try:
            if typ == "master":
                cb_payload = pack_cb(ConfirmDelMasterCB, master_id=int(key))
            elif typ == "service":
                cb_payload = pack_cb(ConfirmDelServiceCB, service_id=str(key))
            else:
                cb_payload = f"confirm_del_{typ}_{key}"
        except Exception:
            # Fallback to legacy string payload if casting fails
            cb_payload = f"confirm_del_{typ}_{key}"
        kb.inline_keyboard.insert(0, [InlineKeyboardButton(text=name, callback_data=cb_payload)])
    await safe_edit(_get_msg_obj(callback), f"{title} ({t('page_short', lang)} {page}/{total_pages}):", reply_markup=kb)
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
        text_root = t("admin_panel_title", lang)
        markup_root = admin_menu_kb(lang)
        # Answer root screen
        await message.answer(text_root, reply_markup=markup_root)
        # Store it as current so the next nav_push will push it onto stack
        await nav_replace(state, text_root, markup_root, lang=lang)
        # mark preferred role so role-root nav returns here
        try:
            await state.update_data(preferred_role="admin")
        except Exception:
            pass
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
                # fall through to normal behaviour if show_main_client_menu fails
                logger.debug("show_main_client_menu failed while handling admin panel back")
    except Exception:
        pass
    await nav_reset(state)
    # Try to edit the existing message in-place. If edit fails due to
    # 'message is not modified' or other transient Telegram errors, prefer
    # to silently ignore rather than sending a new message — creating a
    # duplicate admin panel instance. This keeps the UI single-window.
    try:
        m = _get_msg_obj(callback)
        if m and hasattr(m, "edit_text"):
            try:
                await m.edit_text(t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))
                # Ensure navigation state reflects the admin panel as the current screen
                try:
                    await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
                except Exception:
                    # best-effort: don't fail the handler if nav_replace has an issue
                    logger.debug("nav_replace failed when returning to admin panel")
                # mark preferred role so nav_role_root will prefer admin when appropriate
                try:
                    await state.update_data(preferred_role="admin")
                except Exception:
                    pass
            except Exception as ee:
                # ignore 'message is not modified' and similar benign errors
                if "message is not modified" in str(ee).lower():
                    logger.debug("Ignored 'message is not modified' when returning to admin panel")
                    try:
                        await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
                    except Exception:
                        logger.debug("nav_replace failed when returning to admin panel after 'not modified'")
                    try:
                        await state.update_data(preferred_role="admin")
                    except Exception:
                        pass
                else:
                    logger.debug("Failed to edit admin panel message in-place: %s", ee)
        else:
            # If no editable message available, fallback to safe_edit to best-effort show admin panel
            await safe_edit(_get_msg_obj(callback), t("admin_panel_title", lang), reply_markup=admin_menu_kb(lang))
            try:
                await nav_replace(state, t("admin_panel_title", lang), admin_menu_kb(lang), lang=lang)
            except Exception:
                logger.debug("nav_replace failed when returning to admin panel in fallback branch")
            try:
                await state.update_data(preferred_role="admin")
            except Exception:
                pass
    except Exception as e:
        logger.exception("Unexpected error while returning to admin panel: %s", e)
    logger.info("Возврат в админ-панель для пользователя %s", callback.from_user.id)
    await callback.answer()


# --------------------- Управление ценами на услуги ---------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "manage_prices"))
async def admin_manage_prices(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    
    try:
        async with get_session() as session:
            res = await session.execute(select(Service.id, Service.name).order_by(Service.name))
            services = [(sid, name) for sid, name in res.fetchall()]
        lang = locale
        text = t("manage_prices_title", lang)
        if m := _get_msg_obj(callback):
            kb = services_list_kb(services, lang)
            await nav_push(state, text, kb, lang=lang)
            await safe_edit(m, text, reply_markup=kb)
    except Exception as e:
        logger.exception("Ошибка в admin_manage_prices: %s", e)
        lang = locale
        await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
    await callback.answer()


from bot.app.telegram.common.callbacks import AdminEditPriceCB, AdminSetPriceCB, AdminPriceAdjCB, AdminSetCurrencyCB, ExecDelServiceCB, ConfirmDelServiceCB


@admin_router.callback_query(AdminEditPriceCB.filter())
@admin_handler
@admin_safe()
async def admin_edit_price(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    
    lang = locale
    try:
        sid = str(callback_data.service_id)
        async with get_session() as session:
            svc = await session.get(Service, sid)
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
    except Exception as e:
        logger.exception("Ошибка в admin_edit_price: %s", e)
        await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()


@admin_router.callback_query(AdminSetPriceCB.filter())
@admin_handler
@admin_safe()
async def admin_set_price(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    
    lang = locale
    try:
        sid = str(callback_data.service_id)
        await state.update_data(price_service_id=sid)
        if msg := _get_msg_obj(callback):
            await msg.answer(t("enter_price", lang))
    except Exception as e:
        logger.exception("Ошибка в admin_set_price: %s", e)
    await callback.answer()

@admin_router.callback_query(AdminPriceAdjCB.filter())
@admin_handler
@admin_safe()
async def admin_price_adjust(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Adjust service price by delta (in UAH) via inline stepper.

    Callback data format: admin_price_adj_{service_id}:{delta}
    where delta is integer UAH, can be prefixed with + or -.
    """
    
    lang = locale
    try:
        sid = str(callback_data.service_id)
        delta_ua = int(callback_data.delta)
        delta_cents = delta_ua * 100

        # Use centralized service price updater
        new_cents = None
        async with get_session() as _s_check:
            _svc = await _s_check.get(Service, sid)
            if not _svc:
                await callback.answer(t("not_found", lang), show_alert=True)
                return
            current_cents = getattr(_svc, 'final_price_cents', None) or getattr(_svc, 'price_cents', None) or 0
            new_cents = max(0, current_cents + delta_cents)

        svc = await update_service_price_cents(sid, new_cents)
        if not svc:
            await callback.answer(t("error", lang), show_alert=True)
            return
        currency = getattr(svc, 'currency', None) or 'UAH'
        price_txt = format_money_cents(new_cents, currency)
        text = (f"<b>{svc.name}</b>\n"
                f"ID: <code>{svc.id}</code>\n"
                f"{t('current_price', lang)}: {price_txt}")
        try:
            if mmsg := _get_msg_obj(callback):
                kb = edit_price_kb(sid, lang)
                await safe_edit(mmsg, text, reply_markup=kb)
            await callback.answer(t("price_updated", lang))
        except Exception as e:
            logger.exception("Ошибка в admin_price_adjust (inner): %s", e)
            try:
                await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
            except Exception:
                pass
            try:
                await callback.answer()
            except Exception:
                pass
    except Exception as e:
        # Outer catch: log and attempt best-effort UI fallback
        logger.exception("Ошибка в admin_price_adjust: %s", e)
        try:
            await safe_edit(_get_msg_obj(callback), t("error", lang), reply_markup=admin_menu_kb(lang))
        except Exception:
            pass
        try:
            await callback.answer(t("error", lang))
        except Exception:
            pass

@admin_router.message(F.text.regexp(r"^\d{2,6}$"))
@admin_handler
@admin_safe()
async def admin_price_input(message: Message, state: FSMContext, locale: str) -> None:
    data = await state.get_data()
    sid = data.get("price_service_id")
    if not sid:
        return
    lang = locale
    try:
        grn = int(message.text or "0")
        cents = grn * 100
        svc = await update_service_price_cents(sid, cents)
        if not svc:
            await message.answer(t("error", lang))
            await state.update_data(price_service_id=None)
            return
        await message.answer(t("price_updated", lang))
        await state.update_data(price_service_id=None)
    except Exception as e:
        logger.exception("Ошибка admin_price_input: %s", e)
        await message.answer(t("error", lang))


@admin_router.callback_query(AdminSetCurrencyCB.filter())
@admin_handler
@admin_safe()
async def admin_set_currency(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    
    lang = locale
    try:
        sid = str(callback_data.service_id)
        await state.update_data(currency_service_id=sid)
        if msg := _get_msg_obj(callback):
            await msg.answer(t("enter_currency", lang))
    except Exception as e:
        logger.exception("Ошибка в admin_set_currency: %s", e)
    await callback.answer()


@admin_router.message(F.text.regexp(r"^(UAH|EUR|USD)$"))
@admin_handler
@admin_safe()
async def admin_currency_input(message: Message, state: FSMContext, locale: str) -> None:
    data = await state.get_data()
    sid = data.get("currency_service_id")
    if not sid:
        return
    lang = locale
    try:
        cur = (message.text or "UAH").upper()
        async with get_session() as session:
            svc = await session.get(Service, sid)
            if not svc:
                await message.answer(t("not_found", lang))
                await state.update_data(currency_service_id=None)
                return
            svc.currency = cur
            await session.commit()
        await message.answer(t("currency_updated", lang))
        await state.update_data(currency_service_id=None)
    except Exception as e:
        logger.exception("Ошибка admin_currency_input: %s", e)
        await message.answer(t("error", lang))
    # No callback context here


@admin_router.callback_query(AdminMenuCB.filter(F.act == "exit"))
async def admin_exit(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Закрывает админ-панель и очищает состояние.

    Args:
        callback: CallbackQuery для выхода.
        state: Контекст FSM для очистки состояния.
    """
    
    await state.clear()
    lang = (await nav_get_lang(state)) or locale
    await safe_edit(
        _get_msg_obj(callback),
        t("exit_message", lang),
        reply_markup=None
    )
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
    
    if m := _get_msg_obj(callback):
        lang = (await nav_get_lang(state)) or locale
        await nav_push(state, t("bookings_filter", lang), show_bookings_filter_kb(lang), lang=lang)
        await safe_edit(
            m,
            t("bookings_filter", lang),
            reply_markup=show_bookings_filter_kb(lang)
        )
    logger.info("Меню фильтров записей показано для пользователя %s", callback.from_user.id)
    await callback.answer()


from bot.app.telegram.common.callbacks import AdminBookingsCB


@admin_router.callback_query(AdminBookingsCB.filter())
async def admin_bookings_filter(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Отображает записи с применением выбранного фильтра.

    Args:
        callback: CallbackQuery с выбранным фильтром (all, paid, awaiting).
    """
    # Access is enforced by AdminRoleFilter applied on the router
    mode = getattr(callback_data, "mode", "all")
    lang = (await nav_get_lang(state)) or locale
    try:
        # Persist current mode and reset to first page
        await state.update_data(bookings_mode=mode, bookings_page=1)
        text, kb = await _render_bookings_page(mode, 1, lang)
        if m := _get_msg_obj(callback):
            await nav_push(state, text, kb, lang=lang)
            await safe_edit(m, text, reply_markup=kb)
        logger.info("Записи отображены для пользователя %s, фильтр=%s", callback.from_user.id, mode)
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных в admin_bookings_filter: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_bookings_filter: %s", e)
    await callback.answer()


@admin_router.callback_query(BookingsPageCB.filter())
async def admin_bookings_paginate(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Handles pagination for admin bookings list."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = (await nav_get_lang(state)) or locale
    try:
        page = int(callback_data.page)
        data = await state.get_data()
        mode = data.get("bookings_mode", "all")
        await state.update_data(bookings_page=page)
        text, kb = await _render_bookings_page(mode, page, lang)
        if msg := _get_msg_obj(callback):
            await safe_edit(msg, text, reply_markup=kb)
    except Exception as e:
        logger.exception("Ошибка пагинации списка записей: %s", e)
        await callback.answer(t("error", lang))
    else:
        await callback.answer()


async def _render_bookings_page(mode: str, page: int, lang: str) -> tuple[str, InlineKeyboardMarkup]:
    """Renders a paginated admin bookings list with localized details.

    Returns: (text, inline_keyboard)
    """
    page_size = 10
    async with get_session() as session:
        stmt = select(Booking).order_by(Booking.starts_at.desc())
        if mode == "paid":
            stmt = stmt.where(Booking.status == BookingStatus.PAID)
        elif mode == "awaiting":
            stmt = stmt.where(Booking.status.in_([
                getattr(BookingStatus, "AWAITING_CASH", BookingStatus.CONFIRMED),
                BookingStatus.PENDING_PAYMENT,
                BookingStatus.RESERVED,
            ]))
        elif mode == "upcoming":
            now = datetime.now().astimezone(ZoneInfo("UTC"))
            stmt = stmt.where(Booking.starts_at >= now)
        elif mode == "cancelled":
            stmt = stmt.where(Booking.status == BookingStatus.CANCELLED)
        elif mode == "done":
            stmt = stmt.where(Booking.status == BookingStatus.DONE)
        elif mode == "no_show":
            stmt = stmt.where(Booking.status == BookingStatus.NO_SHOW)
        elif mode == "today":
            now_local = datetime.now(LOCAL_TZ)
            start_utc = now_local.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
            end_utc = now_local.replace(hour=23, minute=59, second=59, microsecond=999999).astimezone(ZoneInfo("UTC"))
            stmt = stmt.where(Booking.starts_at >= start_utc, Booking.starts_at <= end_utc)
        elif mode == "week":
            now_local = datetime.now(LOCAL_TZ)
            start_of_week = now_local.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now_local.weekday())
            end_of_week = start_of_week + timedelta(days=7)
            stmt = stmt.where(Booking.starts_at >= start_of_week.astimezone(ZoneInfo("UTC")), Booking.starts_at < end_of_week.astimezone(ZoneInfo("UTC")))
        elif mode == "this_month":
            now_local = datetime.now(LOCAL_TZ)
            month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_month = month_start.replace(year=month_start.year + 1, month=1) if month_start.month == 12 else month_start.replace(month=month_start.month + 1)
            stmt = stmt.where(
                Booking.starts_at >= month_start.astimezone(ZoneInfo("UTC")),
                Booking.starts_at < next_month.astimezone(ZoneInfo("UTC")),
            )
        elif mode == "last_month":
            now_local = datetime.now(LOCAL_TZ)
            this_month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_month_end = this_month_start
            if this_month_start.month == 1:
                last_month_start = this_month_start.replace(year=this_month_start.year - 1, month=12)
            else:
                last_month_start = this_month_start.replace(month=this_month_start.month - 1)
            stmt = stmt.where(
                Booking.starts_at >= last_month_start.astimezone(ZoneInfo("UTC")),
                Booking.starts_at < last_month_end.astimezone(ZoneInfo("UTC")),
            )

        all_rows = (await session.execute(stmt)).scalars().all()
        total = len(all_rows)
        total_pages = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        rows = all_rows[start:start + page_size]

        if not rows:
            text = t("no_bookings_admin", lang)
        else:
            # Prefetch clients by id and masters by telegram_id to avoid N+1
            client_ids = {int(getattr(b, "user_id", 0) or 0) for b in rows if getattr(b, "user_id", None)}
            master_tids = {int(getattr(b, "master_id", 0) or 0) for b in rows if getattr(b, "master_id", None)}
            clients_map: dict[int, User] = {}
            masters_map: dict[int, User] = {}
            if client_ids:
                c_res = await session.execute(select(User).where(User.id.in_(client_ids)))
                clients_map = {u.id: u for u in c_res.scalars().all()}
            if master_tids:
                m_res = await session.execute(select(User).where(User.telegram_id.in_(master_tids)))
                masters_map = {u.telegram_id: u for u in m_res.scalars().all()}

            # Prefetch services (name + category) to avoid per-row queries
            # service_id can be numeric or string (slug). Keep raw values to avoid ValueError.
            service_ids = {getattr(b, "service_id") for b in rows if getattr(b, "service_id", None) is not None}
            services_map: dict[object, tuple[str, str | None]] = {}
            if service_ids:
                s_res = await session.execute(select(Service.id, Service.name, Service.category).where(Service.id.in_(service_ids)))
                services_map = {sid: (sname, scategory) for sid, sname, scategory in s_res.all()}

            cards: list[str] = []
            for b in rows:
                # Client
                client = clients_map.get(int(getattr(b, "user_id", 0) or 0))
                client_name = getattr(client, "name", None) or t("unknown", lang)
                client_username = getattr(client, "username", None)
                client_line = f"{client_name} (@{client_username})" if client_username else client_name
                # Master (by telegram_id)
                master = masters_map.get(int(getattr(b, "master_id", 0) or 0))
                master_name = getattr(master, "name", None) or str(getattr(b, "master_id", "-"))
                master_username = getattr(master, "username", None)
                master_line = f"{master_name} (@{master_username})" if master_username else master_name
                # Service (name + optional category) from prefetch map
                svc_name: str
                svc_cat: str | None
                sid = getattr(b, "service_id", None)
                if sid in services_map:
                    svc_name, svc_cat = services_map.get(sid, (str(sid), None))
                else:
                    # Fallbacks
                    try:
                        svc_name = await get_service_name(str(sid))
                    except Exception:
                        svc_name = str(sid)
                    svc_cat = None
                # Date/time localized
                try:
                    dt_local = b.starts_at.astimezone(LOCAL_TZ)
                    dt_txt = f"{dt_local:%d.%m %H:%M}"
                except Exception:
                    dt_txt = str(b.starts_at)
                # Amount
                cents = getattr(b, "final_price_cents", 0) or getattr(b, "original_price_cents", 0) or 0
                price_txt = format_money_cents(cents)
                # Localized status
                status_value = getattr(getattr(b, "status", None), "value", str(getattr(b, "status", "")))
                status_key = {
                    "RESERVED": "status_reserved",
                    "PENDING_PAYMENT": "status_pending_payment",
                    "CONFIRMED": "status_confirmed",
                    "AWAITING_CASH": "status_awaiting_cash",
                    "PAID": "status_paid",
                    "ACTIVE": "status_active",
                    "CANCELLED": "status_cancelled",
                    "DONE": "status_done",
                    "NO_SHOW": "status_no_show",
                    "EXPIRED": "status_expired",
                }.get(status_value, "status_active")
                status_txt = t(status_key, lang)

                # Card in mini receipt style
                header = f"🆔 {b.id} | 📅 {dt_txt}"
                service_line = f"💇 {t('service_label', lang)}: {svc_name}"
                if svc_cat:
                    service_line = f"💇 {svc_cat} → {svc_name}"
                card = (
                    f"{header}\n"
                    f"� {t('client_label', lang)}: {client_line}\n"
                    f"👨‍🎨 {t('master_label', lang)}: {master_line}\n"
                    f"{service_line}\n"
                    f"💰 {price_txt} | {t('status_label', lang)}: {status_txt}"
                )
                cards.append(card)
            text = "\n\n".join(cards)

    # Pagination keyboard
    kb = pagination_kb("bookings", page, total_pages, lang)
    return text, kb


@admin_router.callback_query(AdminMenuCB.filter(F.act == "export_csv"))
async def admin_export_csv(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Export current month and current filter to CSV and send as a document."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = (await nav_get_lang(state)) or locale
    try:
        data = await state.get_data()
        mode = data.get("bookings_mode", "all")
        # Compute month range (local TZ)
        now_local = datetime.now(LOCAL_TZ)
        month_start = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1)
        month_end = next_month

        # Fetch rows according to mode
        async with get_session() as session:
            stmt = select(Booking).order_by(Booking.starts_at.desc()).where(
                Booking.starts_at >= month_start.astimezone(ZoneInfo("UTC")),
                Booking.starts_at < month_end.astimezone(ZoneInfo("UTC")),
            )
            if mode == "paid":
                stmt = stmt.where(Booking.status == BookingStatus.PAID)
            elif mode == "awaiting":
                stmt = stmt.where(Booking.status.in_([
                    getattr(BookingStatus, "AWAITING_CASH", BookingStatus.CONFIRMED),
                    BookingStatus.PENDING_PAYMENT,
                    BookingStatus.RESERVED,
                ]))
            elif mode == "upcoming":
                now_utc = datetime.now().astimezone(ZoneInfo("UTC"))
                stmt = stmt.where(Booking.starts_at >= now_utc)
            elif mode == "cancelled":
                stmt = stmt.where(Booking.status == BookingStatus.CANCELLED)
            elif mode == "done":
                stmt = stmt.where(Booking.status == BookingStatus.DONE)
            elif mode == "no_show":
                stmt = stmt.where(Booking.status == BookingStatus.NO_SHOW)

            rows = (await session.execute(stmt)).scalars().all()

            # Prefetch users and services to avoid N+1 in CSV generation
            client_ids = {int(getattr(b, "user_id", 0) or 0) for b in rows if getattr(b, "user_id", None)}
            master_tids = {int(getattr(b, "master_id", 0) or 0) for b in rows if getattr(b, "master_id", None)}
            service_ids = {int(getattr(b, "service_id", 0) or 0) for b in rows if getattr(b, "service_id", None)}
            clients_map: dict[int, User] = {}
            masters_map: dict[int, User] = {}
            services_map: dict[int, str] = {}
            if client_ids:
                c_res = await session.execute(select(User).where(User.id.in_(client_ids)))
                clients_map = {u.id: u for u in c_res.scalars().all()}
            if master_tids:
                m_res = await session.execute(select(User).where(User.telegram_id.in_(master_tids)))
                masters_map = {u.telegram_id: u for u in m_res.scalars().all()}
            if service_ids:
                s_res = await session.execute(select(Service.id, Service.name).where(Service.id.in_(service_ids)))
                services_map = {sid: sname for sid, sname in s_res.all()}

            # Build CSV in-memory
            import io, csv
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["ID", "Date", "Client", "Master", "Service", "Amount", "Status"])
            for b in rows:
                # Client and master (from prefetch maps)
                client = clients_map.get(int(getattr(b, "user_id", 0) or 0))
                c_name = getattr(client, "name", "")
                c_usr = getattr(client, "username", None)
                c_cell = f"{c_name} (@{c_usr})" if c_usr else c_name
                master = masters_map.get(int(getattr(b, "master_id", 0) or 0))
                m_name = getattr(master, "name", "")
                m_usr = getattr(master, "username", None)
                m_cell = f"{m_name} (@{m_usr})" if m_usr else m_name
                # Service (from prefetch, fallback to ID)
                sid = int(getattr(b, "service_id", 0) or 0)
                s_name = services_map.get(sid) or str(sid)
                # Date local
                dt_local = b.starts_at.astimezone(LOCAL_TZ)
                dt_txt = f"{dt_local:%Y-%m-%d %H:%M}"
                # Amount
                cents = getattr(b, "final_price_cents", 0) or getattr(b, "original_price_cents", 0) or 0
                price = format_money_cents(cents)
                # Status
                status_value = getattr(getattr(b, "status", None), "value", str(getattr(b, "status", "")))
                writer.writerow([b.id, dt_txt, c_cell, m_cell, s_name, price, status_value])

            buf.seek(0)
            file_name = f"bookings_{mode}_{now_local:%Y_%m}.csv"
            data_bytes = buf.getvalue().encode("utf-8")
            file = BufferedInputFile(file=data_bytes, filename=file_name)
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


@admin_router.message(AdminStates.add_master_name)
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
        await message.answer(t("enter_master_id", lang))
        logger.info("Имя мастера '%s' сохранено для пользователя %s", name, safe_user_id(message))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_get_name: %s", e)


@admin_router.message(AdminStates.add_master_id)
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
        async with get_session() as session:
            if await session.scalar(select(Master).where(Master.telegram_id == tg_id)):
                lang = locale
                await message.answer(t("master_exists", lang))
            else:
                session.add(Master(telegram_id=tg_id, name=name))
                await session.commit()
                invalidate_masters_cache()
                logger.info("Админ %s добавил мастера %s (%s)", safe_user_id(message), tg_id, name)
                lang = locale
                await message.answer(t("master_added", lang).format(name=name))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при добавлении мастера: %s", e)
        lang = locale
        await message.answer(t("db_error", lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_master_finish: %s", e)
    await state.clear()
    lang = locale
    await message.answer(
        t("admin_panel_title", lang),
        reply_markup=admin_menu_kb(lang)
    )


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
    masters = await masters_cache()
    if not masters:
        lang = locale
        await safe_edit(_get_msg_obj(callback), t("no_masters_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    items = list(masters.items())
    total_pages = (len(items) + 9) // 10
    await state.update_data(delete_items=items, delete_page=1, delete_type="master")
    await _show_paginated(
        callback,
        state,
        total_pages,
        f"{t('select_master_to_delete', locale)}",
        "del_master",
        locale
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
        page = int(callback_data.page)
        lang = (await nav_get_lang(state)) or locale
        await state.update_data(delete_page=page)
        masters = await masters_cache()
        items = list(masters.items())
        total_pages = (len(items) + 9) // 10
        await _show_paginated(
            callback,
            state,
            total_pages,
            f"{t('select_master_to_delete', lang)}",
            "del_master",
            lang
        )
        logger.info("Пагинация мастеров, страница %d, для пользователя %s", page, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка пагинации мастеров: %s", e)
        lang = (await nav_get_lang(state)) or locale
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
        kb = InlineKeyboardBuilder()
        lang = (await nav_get_lang(state)) or locale
        kb.button(text=t("confirm_delete", lang), callback_data=pack_cb(ExecDelMasterCB, master_id=int(mid)))
        kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
        if m := _get_msg_obj(callback):
            await nav_push(state, t("confirm_master_delete", lang).format(id=mid), kb.as_markup(), lang=lang)
            await safe_edit(m, t("confirm_master_delete", lang).format(id=mid), reply_markup=kb.as_markup())
        logger.info("Запрос подтверждения удаления мастера %s для пользователя %s", mid, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления мастера: %s", e)
        lang = (await nav_get_lang(state)) or locale
        await callback.answer(t("error", lang))
    else:
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
        async with get_session() as session:
            master = await session.get(Master, mid)
            if master:
                await session.delete(master)
                await session.commit()
                invalidate_masters_cache()
                logger.info("Админ %s удалил мастера %s", safe_user_id(callback), mid)
                lang = (await nav_get_lang(state)) or locale
                text = t("master_deleted", lang)
            else:
                lang = (await nav_get_lang(state)) or locale
                text = t("not_found", lang)
        if m := _get_msg_obj(callback):
            lang = (await nav_get_lang(state)) or locale
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при удалении мастера: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в delete_master_exec: %s", e)
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
        lang = (await nav_get_lang(state)) or locale
        await state.set_state(AdminStates.add_service_id)
        if m := _get_msg_obj(callback):
            text = t("enter_service_id", lang)
            await nav_push(state, text, None, lang=lang)
            await safe_edit(m, text)
        logger.info("Начало добавления услуги для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_service_start: %s", e)
    await callback.answer()


@admin_router.message(AdminStates.add_service_id)
@admin_handler
@admin_safe()
async def add_service_get_id(message: Message, state: FSMContext, locale: str) -> None:
    """Получает ID новой услуги и запрашивает название.

    Args:
        message: Сообщение с ID услуги.
        state: Контекст FSM для сохранения ID.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    # Log entry for diagnostic: confirm handler reached and current FSM state.
    try:
        cur_state = await state.get_state()
    except Exception:
        cur_state = None
    try:
        logger.info("add_service_get_id invoked for user %s text=%r state=%r", safe_user_id(message), message.text, cur_state)
    except Exception:
        # best-effort logging; don't fail the handler on logging errors
        logger.debug("add_service_get_id invoked (logging failed)")

    # Temporary debug reply removed (we rely on logs).

    sid = (message.text or "").strip().lower()
    # Allow letters/digits/underscore/hyphen from any Unicode script so admins
    # can type IDs in their preferred script (Cyrillic etc.). If you prefer
    # to restrict IDs to ASCII slugs, we can revert to the stricter pattern.
    if not sid or not re.fullmatch(r"[\w-]+", sid):
        lang = locale
        await message.answer(t("invalid_service_id", lang))
        try:
            logger.debug("Invalid service id input from %s: %r", safe_user_id(message), message.text)
        except Exception:
            pass
        return
    try:
        await state.update_data(id=sid)
        await state.set_state(AdminStates.add_service_name)
        lang = locale
        await message.answer(t("enter_service_name", lang))
        logger.info("ID услуги '%s' сохранен для пользователя %s", sid, safe_user_id(message))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_service_get_id: %s", e)


@admin_router.message(AdminStates.add_service_name)
@admin_handler
@admin_safe()
async def add_service_finish(message: Message, state: FSMContext, locale: str) -> None:
    """Завершает добавление услуги, сохраняя ее в базу.

    Args:
        message: Сообщение с названием услуги.
        state: Контекст FSM с сохраненным ID.
    """
    # Access is enforced by AdminRoleFilter applied on the router
    data = await state.get_data()
    sid = data.get("id")
    name = message.text or "(без назви)"
    try:
        async with get_session() as session:
            if await session.get(Service, sid):
                lang = locale
                await message.answer(t("service_exists", lang))
            else:
                session.add(Service(id=sid, name=name))
                await session.commit()
                invalidate_services_cache()
                logger.info("Админ %s добавил услугу %s (%s)", safe_user_id(message), sid, name)
                lang = locale
                await message.answer(t("service_added", lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при добавлении услуги: %s", e)
        _lang = locale
        await message.answer(t("db_error", _lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в add_service_finish: %s", e)
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
    services = await services_cache()
    if not services:
        lang = locale
        await safe_edit(_get_msg_obj(callback), t("no_services_admin", lang), reply_markup=admin_menu_kb(lang))
        await callback.answer()
        return
    items = list(services.items())
    total_pages = (len(items) + 9) // 10
    await state.update_data(delete_items=items, delete_page=1, delete_type="service")
    await _show_paginated(
        callback,
        state,
        total_pages,
        f"{t('select_service_to_delete', locale)}",
        "del_service",
        locale
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
        page = int(callback_data.page)
        lang = (await nav_get_lang(state)) or locale
        await state.update_data(delete_page=page)
        services = await services_cache()
        items = list(services.items())
        total_pages = (len(items) + 9) // 10
        await _show_paginated(
            callback,
            state,
            total_pages,
            f"{t('select_service_to_delete', lang)}",
            "del_service",
            lang
        )
        logger.info("Пагинация услуг, страница %d, для пользователя %s", page, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка пагинации услуг: %s", e)
        lang = (await nav_get_lang(state)) or locale
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
        kb = InlineKeyboardBuilder()
        lang = (await nav_get_lang(state)) or locale
        kb.button(text=t("confirm_delete", lang), callback_data=pack_cb(ExecDelServiceCB, service_id=str(sid)))
        kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_service"))
        if m := _get_msg_obj(callback):
            await nav_push(state, t("confirm_service_delete", lang).format(id=sid), kb.as_markup(), lang=lang)
            await safe_edit(m, t("confirm_service_delete", lang).format(id=sid), reply_markup=kb.as_markup())
        logger.info("Запрос подтверждения удаления услуги %s для пользователя %s", sid, callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка подтверждения удаления услуги: %s", e)
        lang = (await nav_get_lang(state)) or locale
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
        async with get_session() as session:
            svc = await session.get(Service, sid)
            if svc:
                await session.delete(svc)
                await session.commit()
                invalidate_services_cache()
                logger.info("Админ %s удалил услугу %s", safe_user_id(callback), sid)
                lang = (await nav_get_lang(state)) or locale
                text = t("service_deleted", lang)
            else:
                lang = (await nav_get_lang(state)) or locale
                text = t("not_found", lang)
        if m := _get_msg_obj(callback):
            lang = (await nav_get_lang(state)) or locale
            await nav_push(state, text, admin_menu_kb(lang), lang=lang)
            await safe_edit(m, text, reply_markup=admin_menu_kb(lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при удалении услуги: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в delete_service_exec: %s", e)
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
    lang = (await nav_get_lang(state)) or locale
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
        lang = (await nav_get_lang(state)) or (locale or "uk")
        await callback.answer(t("invalid_id", lang), show_alert=True)
        return

    lang = (await nav_get_lang(state)) or (locale or "uk")
    
    # Получаем список услуг
    async with get_session() as session:
        if action == "unlink":
            # Для отвязки: запрашиваем только услуги, привязанные к мастеру
            stmt = select(Service.id, Service.name).join(
                MasterService, MasterService.service_id == Service.id
            ).where(MasterService.master_telegram_id == master_tid).order_by(Service.name)
            result = await session.execute(stmt)
            services_raw = result.fetchall()
            logger.debug("Services raw data for unlink: %s", services_raw)  # Отладка
            services = [(str(sid), name) for sid, name in services_raw]
        else:
            # Для привязки: все доступные услуги
            services_dict = await services_cache()
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
    lang = (await nav_get_lang(state)) or locale
    data = await state.get_data()
    master_tid = data.get("master_tid")
    try:
        async with get_session() as session:
            master = await session.scalar(select(Master).where(Master.telegram_id == master_tid))
            if not master:
                await callback.answer(t("master_not_found", lang), show_alert=True)
                return
            exists = await session.scalar(select(MasterService).where(
                MasterService.master_telegram_id == master.telegram_id,
                MasterService.service_id == service_id
            ))
            if exists:
                text = t("already_linked", lang)
            else:
                session.add(MasterService(master_telegram_id=master.telegram_id, service_id=service_id))
                await session.commit()
                invalidate_masters_cache()
                logger.info("Админ %s привязал мастера %s к услуге %s", safe_user_id(callback), master_tid, service_id)
                text = t("link_added", lang)
            await safe_edit(_get_msg_obj(callback), text, reply_markup=admin_menu_kb(lang))
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при привязке: %s", e)
        if m := _get_msg_obj(callback):
            await safe_edit(m, t("db_error", lang), reply_markup=admin_menu_kb(lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в link_master_finish: %s", e)
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
    lang = (await nav_get_lang(state)) or locale
    data = await state.get_data()
    master_tid = data.get("master_tid")
    try:
        async with get_session() as session:
            master = await session.scalar(select(Master).where(Master.telegram_id == master_tid))
            if not master:
                await callback.answer(t("master_not_found", lang), show_alert=True)
                return
            link = await session.scalar(select(MasterService).where(
                MasterService.master_telegram_id == master.telegram_id,
                MasterService.service_id == service_id
            ))
            if link:
                await session.delete(link)
                await session.commit()
                invalidate_masters_cache()  # Обновляем кэш
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
    lang = (await nav_get_lang(state)) or locale
    kb = admin_settings_kb(lang)
    msg = _get_msg_obj(callback)
    if msg:
        await nav_push(state, t("settings_title", lang), kb, lang=lang)
        await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
    else:
        if callback.message:
            await callback.message.answer(t("settings_title", lang), reply_markup=kb)
    logger.info("Меню настроек отображено для пользователя %s", user_id)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "toggle_telegram_payments"))
async def admin_toggle_telegram_payments_handler(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Переключает состояние Telegram Payments.

    Args:
        callback: CallbackQuery для переключения.
    """
    user_id = callback.from_user.id
    logger.info("Переключение Telegram Payments для пользователя %s", user_id)
    # Access is enforced by AdminRoleFilter applied on the router
    lang = (await nav_get_lang(state)) or locale
    try:
        # Prevent enabling when provider token missing
        token = get_telegram_provider_token() or ""
        if not token:
            await callback.answer(
                t("payments_token_missing", lang),
                show_alert=True,
            )
            kb = admin_settings_kb(lang)
            msg = _get_msg_obj(callback)
            if msg:
                await nav_push(state, t("settings_title", lang), kb, lang=lang)
                await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
            return
        new_val = await toggle_telegram_payments()
        status = t("enabled", lang) if new_val else t("disabled", lang)
        logger.info("Админ %s переключил Telegram Payments на %s", user_id, status)
        await callback.answer(t("payments_toggled", lang).format(status=status))
        kb = admin_settings_kb(lang)
        msg = _get_msg_obj(callback)
        if msg:
            await nav_push(state, t("settings_title", lang), kb, lang=lang)
            await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
        else:
            if callback.message:
                await callback.message.answer(t("settings_title", lang), reply_markup=kb)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_toggle_telegram_payments_handler: %s", e)
        await callback.answer(t("telegram_error", lang))
    except Exception as e:
        logger.exception("Неожиданная ошибка в admin_toggle_telegram_payments_handler: %s", e)

@admin_router.callback_query(AdminMenuCB.filter(F.act == "bookings_filters"))
async def admin_bookings_filters(cb: CallbackQuery, state: FSMContext, locale: str):
    """Возврат в меню фильтров записей."""
    try:
        lang = (await nav_get_lang(state)) or locale
    except Exception:
        lang = locale or "uk"
    kb = show_bookings_filter_kb(lang)
    if cb.message:
        await safe_edit(cb.message, t("bookings_filters_title", lang), reply_markup=kb)
    await cb.answer()

@admin_router.callback_query(AdminMenuCB.filter(F.act == "hold_menu"))
async def admin_hold_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора времени удержания резерва."""
    try:
        if m := getattr(callback, "message", None):
            lang = (await nav_get_lang(state)) or locale
            kb = admin_hold_menu_kb(lang)
            await nav_push(state, t("settings_title", lang), kb, lang=lang)
            await safe_edit(m, t("settings_title", lang), reply_markup=kb)
    except Exception as e:
        logger.error("Ошибка admin_hold_menu: %s", e)
    finally:
        await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "cancel_menu"))
async def admin_cancel_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Показывает меню выбора окна запрета отмены (в часах)."""
    try:
        if m := getattr(callback, "message", None):
            lang = (await nav_get_lang(state)) or locale
            kb = admin_cancel_menu_kb(lang)
            await nav_push(state, t("settings_title", lang), kb)
            await safe_edit(m, t("settings_title", lang), reply_markup=kb)
    except Exception as e:
        logger.error("Ошибка admin_cancel_menu: %s", e)
    finally:
        await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "expire_menu"))
async def admin_expire_menu(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Show expiration-check frequency menu to admin."""
    try:
        if m := getattr(callback, "message", None):
            lang = (await nav_get_lang(state)) or locale
            from bot.app.telegram.admin.admin_keyboards import admin_expire_menu_kb
            kb = admin_expire_menu_kb(lang)
            await nav_push(state, t("settings_title", lang), kb)
            await safe_edit(m, t("settings_title", lang), reply_markup=kb)
    except Exception as e:
        logger.error("Ошибка admin_expire_menu: %s", e)
    finally:
        await callback.answer()


@admin_router.callback_query(AdminSetExpireCB.filter())
async def admin_set_expire(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Set reservation_expire_check_seconds and persist to DB/.env and runtime cfg."""
    try:
        import os
        lang = (await nav_get_lang(state)) or locale
        seconds = int(callback_data.seconds)
        # runtime: update centralized settings API
        try:
            from bot.app.services import shared_services

            await shared_services.update_setting("reservation_expire_check_seconds", seconds)
        except Exception:
            # Best-effort: do not break on settings update failure
            pass
        # DB
        try:
            from bot.app.domain.models import Setting
            async with get_session() as session:
                from sqlalchemy import select
                s = await session.scalar(select(Setting).where(Setting.key == "reservation_expire_check_seconds"))
                if s:
                    s.value = str(seconds)
                else:
                    session.add(Setting(key="reservation_expire_check_seconds", value=str(seconds)))
                await session.commit()
        except Exception as db_e:
            logger.warning("Ошибка сохранения в БД, fallback на cfg.SETTINGS: %s", db_e)
        # .env
        try:
            env_path = ".env"
            lines = []
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    lines = f.readlines()
            updated = False
            for i, line in enumerate(lines):
                if line.startswith("RESERVATION_EXPIRE_CHECK_SECONDS="):
                    lines[i] = f"RESERVATION_EXPIRE_CHECK_SECONDS={seconds}\n"
                    updated = True
                    break
            if not updated:
                lines.append(f"RESERVATION_EXPIRE_CHECK_SECONDS={seconds}\n")
            with open(env_path, "w") as f:
                f.writelines(lines)
            logger.info("Обновлен .env: RESERVATION_EXPIRE_CHECK_SECONDS=%d", seconds)
        except Exception as env_e:
            logger.warning("Ошибка обновления .env, значение сохранено только в runtime/БД: %s", env_e)

        # Refresh settings screen
        if msg := getattr(callback, "message", None):
            kb = admin_settings_kb(lang)
            await nav_replace(state, t("settings_title", lang), kb)
            await safe_edit(msg, t("settings_title", lang), reply_markup=kb)

        # Confirmation to admin (localized simple text)
        # Build human-friendly label
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
        except Exception:
            await callback.answer(f"✅ Частота проверки обновлена: каждые {label}")
    except Exception as e:
        logger.error("Ошибка admin_set_expire: %s", e)
        try:
            await callback.answer(t("error", lang))
        except Exception:
            pass


@admin_router.callback_query(AdminSetHoldCB.filter())
async def admin_set_hold(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Устанавливает новое значение reservation_hold_minutes и сохраняет в БД и .env (fallback на cfg.SETTINGS)."""
    try:
        import os
        lang = (await nav_get_lang(state)) or locale
        minutes = int(callback_data.minutes)
        # Update centralized settings API (runtime + persist)
        try:
            from bot.app.services import shared_services

            await shared_services.update_setting("reservation_hold_minutes", minutes)
        except Exception:
            pass
        # Сохраняем в БД (fallback если БД недоступна)
        try:
            from bot.app.core.db import get_session
            from bot.app.domain.models import Setting
            async with get_session() as session:
                from sqlalchemy import select
                s = await session.scalar(select(Setting).where(Setting.key == "reservation_hold_minutes"))
                if s:
                    s.value = str(minutes)
                else:
                    session.add(Setting(key="reservation_hold_minutes", value=str(minutes)))
                await session.commit()
        except Exception as db_e:
            logger.warning("Ошибка сохранения в БД, fallback на cfg.SETTINGS: %s", db_e)
        # Обновляем .env
        try:
            env_path = ".env"
            lines = []
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    lines = f.readlines()
            updated = False
            for i, line in enumerate(lines):
                if line.startswith("RESERVATION_HOLD_MINUTES="):
                    lines[i] = f"RESERVATION_HOLD_MINUTES={minutes}\n"
                    updated = True
                    break
            if not updated:
                lines.append(f"RESERVATION_HOLD_MINUTES={minutes}\n")
            with open(env_path, "w") as f:
                f.writelines(lines)
            logger.info("Обновлен .env: RESERVATION_HOLD_MINUTES=%d", minutes)
        except Exception as env_e:
            logger.warning("Ошибка обновления .env, значение сохранено только в runtime/БД: %s", env_e)
        # Обновляем экран настроек
        if msg := getattr(callback, "message", None):
            kb = admin_settings_kb(lang)
            await nav_replace(state, t("settings_title", lang), kb)
            await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
        await callback.answer(t("hold_label", lang).format(minutes=minutes))
    except Exception as e:
        logger.error("Ошибка admin_set_hold: %s", e)
        try:
            await callback.answer(t("error", lang))
        except Exception:
            pass


@admin_router.callback_query(AdminSetCancelCB.filter())
async def admin_set_cancel_lock(callback: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Устанавливает новое значение client_cancel_lock_hours и сохраняет в БД и .env."""
    try:
        import os
        lang = (await nav_get_lang(state)) or locale
        hours = int(callback_data.hours)
        # Update centralized settings API (runtime + persist)
        try:
            from bot.app.services import shared_services

            await shared_services.update_setting("client_cancel_lock_hours", hours)
        except Exception:
            pass
        # DB
        try:
            from bot.app.domain.models import Setting
            async with get_session() as session:
                from sqlalchemy import select
                s = await session.scalar(select(Setting).where(Setting.key == "client_cancel_lock_hours"))
                if s:
                    s.value = str(hours)
                else:
                    session.add(Setting(key="client_cancel_lock_hours", value=str(hours)))
                await session.commit()
        except Exception as db_e:
            logger.warning("Ошибка сохранения client_cancel_lock_hours в БД: %s", db_e)
        # .env
        try:
            env_path = ".env"
            lines = []
            if os.path.exists(env_path):
                with open(env_path, "r") as f:
                    lines = f.readlines()
            updated = False
            for i, line in enumerate(lines):
                if line.startswith("CLIENT_CANCEL_LOCK_HOURS="):
                    lines[i] = f"CLIENT_CANCEL_LOCK_HOURS={hours}\n"
                    updated = True
                    break
            if not updated:
                lines.append(f"CLIENT_CANCEL_LOCK_HOURS={hours}\n")
            with open(env_path, "w") as f:
                f.writelines(lines)
            logger.info("Обновлен .env: CLIENT_CANCEL_LOCK_HOURS=%d", hours)
        except Exception as env_e:
            logger.warning("Ошибка обновления .env (CLIENT_CANCEL_LOCK_HOURS), сохранено только runtime/БД: %s", env_e)
        # refresh settings screen
        if msg := getattr(callback, "message", None):
            kb = admin_settings_kb(lang)
            await nav_replace(state, t("settings_title", lang), kb)
            await safe_edit(msg, t("settings_title", lang), reply_markup=kb)
        await callback.answer(t("cancel_lock_label", lang).format(hours=hours))
    except Exception as e:
        logger.error("Ошибка admin_set_cancel_lock: %s", e)
        try:
            await callback.answer(t("error", lang))
        except Exception:
            pass


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
    lang = (await nav_get_lang(state)) or locale
    try:
        totals = await get_basic_totals()
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
    lang = (await nav_get_lang(state)) or locale
    try:
        stats = await get_range_stats(kind)
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
    lang = (await nav_get_lang(state)) or locale
    try:
        await _format_and_send_stats(
            callback,
            t("top_masters", lang),
            await get_top_masters(limit=10),
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
    lang = (await nav_get_lang(state)) or locale
    try:
        services = await get_top_services(limit=10)
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
            lang = (await nav_get_lang(state)) or locale
            await nav_push(state, t("biz_analytics_title", lang), biz_menu_kb(lang), lang=lang)
            await safe_edit(m, t("biz_analytics_title", lang), reply_markup=biz_menu_kb(lang))
        logger.info("Меню бизнес-аналитики показано для пользователя %s", callback.from_user.id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в admin_biz_menu: %s", e)
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_rev"))
async def admin_biz_revenue(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику выручки."""
    # Access is enforced by AdminRoleFilter applied on the router
    lang = (await nav_get_lang(state)) or locale
    try:
        lines = [t("revenue_title", lang), ""]
        lines.append(f"{t('month', lang)}: {format_money_cents(await get_revenue_total('month'))}")
        lines.append(f"{t('week', lang)}: {format_money_cents(await get_revenue_total('week'))}")

        masters = await get_revenue_by_master("month", limit=5)
        if masters:
            lines.append(f"\n{t('top_masters', lang)}:")
            lines.extend(
                f"- {m['name']}: {format_money_cents(m['revenue_cents'])} "
                f"({m['bookings']} {t('bookings_short', lang)})"
                for m in masters
            )

        services = await get_revenue_by_service("month", limit=5)
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
            logger.debug("admin_biz_revenue: editing message with %d chars, preview: %s", len(body), body[:200])
            await safe_edit(m, body, reply_markup=biz_menu_kb(lang))
        logger.info("Статистика выручки отображена для пользователя %s", callback.from_user.id)
    except Exception as e:
        logger.exception("Ошибка в admin_biz_revenue: %s", e)
        if m := _get_msg_obj(callback):
            logger.debug("admin_biz_revenue: encountered exception, sending error text to message")
            await safe_edit(m, t("error", lang), reply_markup=biz_menu_kb(lang))
    await callback.answer()


@admin_router.callback_query(AdminMenuCB.filter(F.act == "biz_ret"))
async def admin_biz_retention(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Отображает статистику удержания клиентов."""
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        lang = (await nav_get_lang(state)) or locale
        ret_m = await get_retention("month")
        ret_w = await get_retention("week")
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
        lang = (await nav_get_lang(state)) or locale
        ns = await get_no_show_rates("month")
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
        lang = (await nav_get_lang(state)) or locale
        topc = await get_top_clients_ltv("month", limit=5)
        format_str = "- {name}: {money} ({bookings} {bookings_short})"
        formatted_data = []
        try:
            from bot.app.services import shared_services

            default_currency = shared_services.get_setting("currency", "UAH") or "UAH"
        except Exception:
            default_currency = getattr(cfg, "SETTINGS", {}).get("currency", "UAH")
        
        for row in topc:
            if not all(key in row for key in ["name", "revenue_cents", "bookings"]):
                logger.warning("Некорректная структура данных в get_top_clients_ltv: %s", row)
                continue
            money = format_money_cents(row["revenue_cents"], row.get("currency", default_currency))
            formatted_data.append({
                "name": row["name"],
                "money": money,
                "bookings": row["bookings"],
                "bookings_short": t("bookings_short", lang)
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
        return getattr(getattr(message, "from_user", None), "id", None)
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
    lang = locale
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
            try:
                from bot.app.services import shared_services

                await shared_services.update_setting("language", new_locale)
            except Exception:
                # Fallback: try to update runtime cfg directly (best-effort)
                try:
                    settings = getattr(cfg, "SETTINGS", {})
                    settings["language"] = new_locale  # type: ignore[index]
                except Exception:
                    pass
            await message.reply(t("global_locale_set", lang).format(locale=new_locale))
            logger.info("Админ %s установил глобальную локаль %s", safe_user_id(message), new_locale)
            return
        async with get_session() as session:
            user = await session.scalar(select(User).where(User.telegram_id == target_id))
            if not user:
                user = User(telegram_id=target_id, name=str(target_id), locale=new_locale)
                session.add(user)
                action = t("user_created", lang)
            else:
                user.locale = new_locale
                action = t("user_updated", lang)
            await session.commit()
            await message.reply(t("user_locale_set_fmt", lang).format(action=action, id=target_id, locale=new_locale))
            logger.info("Админ %s установил локаль %s для пользователя %s", safe_user_id(message), new_locale, target_id)
    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных при установке локали: %s", e)
        _lang = locale
        await message.reply(t("db_error", _lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в cmd_set_locale: %s", e)


# -------------------- Глобальная навигация назад --------------------

@admin_router.callback_query(AdminMenuCB.filter(F.act == "panel"))
async def admin_global_back(callback: CallbackQuery, state: FSMContext, locale: str) -> None:
    """Возвращает пользователя из админ-панели в корневое клиентское меню."""
    # Access is enforced by AdminRoleFilter applied on the router
    try:
        if callback.message is None:
            logger.warning("Отсутствует сообщение для редактирования в admin_global_back для пользователя %s", callback.from_user.id)
            await callback.answer(t("error", lang="uk"), show_alert=True)
            return

        # Сбрасываем стек навигации
        try:
            await nav_reset(state)
            logger.debug("Стек навигации сброшен для пользователя %s", callback.from_user.id)
        except Exception as e:
            logger.warning("Не удалось сбросить стек навигации: %s", e)

        # Импортируем show_main_menu
        try:
            from bot.app.telegram.client.client_handlers import show_main_menu
        except Exception as e:
            logger.error("Не удалось импортировать show_main_menu: %s", e)
            show_main_menu = None

        user_id = callback.from_user.id if callback.from_user else 0
        lang = (await nav_get_lang(state)) or locale

        if show_main_menu:
            logger.debug("Вызов show_main_menu для пользователя %s с prefer_edit=True", user_id)
            await show_main_menu(callback, state, prefer_edit=True)
            logger.info("Клиентское меню отображено для пользователя %s", user_id)
        else:
            from bot.app.telegram.client.client_keyboards import get_main_menu
            # get_main_menu is async and expects (telegram_id,), language is resolved inside
            reply_kb = await get_main_menu(user_id)
            await callback.message.answer(t("main_menu", lang), reply_markup=reply_kb)
            logger.info("Клиентское меню отправлено как новое сообщение для пользователя %s", user_id)

        await callback.answer()
    except Exception as e:
        logger.exception("Ошибка в admin_global_back: %s", e)
        await callback.answer(t("error", lang), show_alert=True)


__all__ = ["admin_router"]