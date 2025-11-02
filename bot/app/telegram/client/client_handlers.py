from __future__ import annotations
import logging
import re
import os
from typing import Optional, Any

from sqlalchemy.sql import func
from aiogram import F, Router, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, PreCheckoutQuery, LabeledPrice
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramAPIError
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, UTC
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.navigation import nav_push, nav_back, nav_reset, nav_replace
from bot.app.telegram.common.callbacks import (
    ServiceSelectCB,
    ServiceToggleCB,
    MasterMultiCB,
    MasterProfileCB,
    MasterSelectCB,
    CalendarCB,
    DateCB,
    TimeCB,
    RescheduleCB,
    pack_cb,
)
from bot.app.telegram.common.callbacks import PayCB, BookingActionCB, CreateBookingCB
from bot.app.telegram.common.callbacks import MyBookingsCB
from bot.app.telegram.common.callbacks import MasterMenuCB
from bot.app.telegram.common.callbacks import NavCB
from bot.app.telegram.common.callbacks import ClientMenuCB
from bot.app.telegram.common.callbacks import RatingCB
from bot.app.telegram.client.client_keyboards import (
    home_kb,
    get_service_menu,
    get_master_keyboard,
    get_calendar_keyboard,
    get_main_menu,
    get_payment_keyboard,
    get_back_button,
    build_rating_keyboard,
    get_simple_kb,
)
from bot.app.translations import t
import bot.app.translations as i18n
from bot.app.services.client_services import (
    get_or_create_user,
    create_booking,
    calculate_price,
    record_booking_rating,
    get_services_duration_and_price,
    create_composite_booking,
)
from bot.app.services.shared_services import (
    get_service_name,
    format_money_cents,
    status_to_emoji,
    is_online_payments_available,
    get_telegram_provider_token,
    send_booking_notification,
    tr,
    format_master_profile,
)
import bot.config as cfg
from bot.app.core.db import get_session
from bot.app.domain.models import Booking, Master, BookingStatus, User, Service
from bot.app.telegram.common.errors import handle_telegram_error, handle_db_error

logger = logging.getLogger(__name__)

# Определяем маршрутизатор один раз
client_router = Router(name="client")
# Attach locale middleware used elsewhere
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
client_router.message.middleware(LocaleMiddleware())
client_router.callback_query.middleware(LocaleMiddleware())
# Register centralized error handlers (best-effort, aiogram may not expose errors.register)
try:
    client_router.errors.register(handle_telegram_error)
    client_router.errors.register(handle_db_error)
except AttributeError:
    # older aiogram versions may not expose `errors.register`; skip silently
    logger.debug("Client router: error handler registration skipped or unsupported in this aiogram version")


# Локальная таймзона из SETTINGS['timezone'] с запасным вариантом
try:
    LOCAL_TZ = ZoneInfo(str(getattr(cfg, "SETTINGS", {}).get("timezone", "Europe/Kyiv")))
except ZoneInfoNotFoundError:
    LOCAL_TZ = ZoneInfo("Europe/Kyiv")


class BookingStates(StatesGroup):
    """Состояния FSM для процесса бронирования."""
    waiting_for_service = State()
    waiting_for_master = State()
    waiting_for_date = State()
    reschedule_select_date = State()
    reschedule_select_time = State()


from bot.app.telegram.common.navigation import show_main_client_menu as show_main_menu


# Locale lookups are provided by LocaleMiddleware; handlers receive `locale: str`.


async def resolve_locale(state: FSMContext | None, locale: str, user_id: Optional[int]) -> str:
    """Resolve locale with the following precedence:
    1. nav_get_lang(state) if available
    2. injected locale parameter (middleware guarantees it's present)
    3. global configured language

    This helper centralizes the common pattern used during migration. It no
    longer performs DB lookups and assumes handlers receive `locale: str`.
    """
    try:
        from bot.app.telegram.common.navigation import nav_get_lang
        if state is not None:
            try:
                nav = await nav_get_lang(state)
            except Exception:
                nav = None
            if nav:
                return nav
    except Exception:
        # nav module not available or import failed; ignore
        pass

    if locale:
        return locale

    return getattr(cfg, "SETTINGS", {}).get("language", "uk")


# Note: forwarding of master menu callbacks to master handlers has been
# removed. The master feature's own router (`master_router`) must handle
# MasterMenuCB(act="menu") callbacks directly. This prevents duplicate
# handlers and order-dependent behavior when client and master routers are
# registered in different orders.





@client_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, locale: str) -> None:
    """Обрабатывает команду /start: очищает состояние и показывает главное меню."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info("Команда /start вызвана для пользователя %s", user_id)
    try:
        await state.clear()
        logger.info("show_main_menu вызвана для user %s", user_id)
        await show_main_menu(message, state, prefer_edit=False)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в cmd_start для пользователя %s: %s", user_id, e)
        await message.answer(tr("error_retry", lang=locale))
    # Let router-level error handlers process unexpected exceptions


@client_router.message(F.text.regexp(r"^/start(?:@[A-Za-z0-9_]+)?(?:\s|$)"))
async def cmd_start_fallback(message: Message, state: FSMContext, locale: str) -> None:
    """Обработчик для /start, отправленного как текст (без сущности команды)."""
    await cmd_start(message, state, locale)


@client_router.message(F.text.regexp(r"(?i)^(start|старт)(\s|$)"))
async def cmd_start_plaintext(message: Message, state: FSMContext, locale: str) -> None:
    """Обработчик для 'start' или 'старт', набранных как обычный текст."""
    await cmd_start(message, state, locale)


@client_router.message(Command("whoami"))
async def cmd_whoami(message: Message, locale: str) -> None:
    """Показывает Telegram ID пользователя для отладки (например, для ADMIN_IDS)."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info("Команда /whoami вызвана для пользователя %s", user_id)
    try:
        lang = locale
        await message.answer(f"{i18n.t('your_telegram_id', lang)} {user_id}")
        logger.info("Telegram ID отправлен для пользователя %s", user_id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в cmd_whoami для пользователя %s: %s", user_id, e)
    # Unexpected exceptions will be handled by router-level error handlers


@client_router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    """Проверка работоспособности: отвечает 'pong'.

    Args:
        message: Входящее сообщение от пользователя.
    """
    logger.info("Команда /ping вызвана для пользователя %s", message.from_user.id if message.from_user else 0)
    try:
        await message.answer("pong")
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в cmd_ping: %s", e)
    # Let centralized error handlers handle unexpected exceptions


@client_router.callback_query(ClientMenuCB.filter(F.act == "booking_service"))
async def start_booking(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Инициирует процесс бронирования, показывая меню услуг.

    Args:
        cb: CallbackQuery от inline-кнопки.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = cb.from_user.id
    logger.info("Начало бронирования для пользователя %s", user_id)
    kb = await get_service_menu()
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        prompt = t("choose_service", lang)
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_service)
    await cb.answer()
    logger.info("Меню услуг отправлено для пользователя %s", user_id)


@client_router.callback_query(ServiceSelectCB.filter())
async def select_service(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор услуги и показывает список мастеров.

    Args:
        cb: CallbackQuery с данными выбранной услуги.
        callback_data: Данные callback'а с ID услуги.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = cb.from_user.id
    service_id = callback_data.service_id
    logger.info("Выбор услуги для пользователя %s, service_id=%s", user_id, service_id)
    # Кэшируем имя услуги для последующего использования
    service_name = await get_service_name(service_id)
    await state.update_data(service_id=service_id, service_name=service_name)
    kb = await get_master_keyboard(service_id)
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        prompt = t("choose_master", lang)
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_master)
    await cb.answer()
    logger.info("Меню мастеров отправлено для пользователя %s", user_id)


@client_router.callback_query(MasterSelectCB.filter())
async def select_master(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор мастера и показывает календарь.

    Args:
        cb: CallbackQuery с данными выбранного мастера.
        callback_data: Данные callback'а с ID мастера и услуги.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = cb.from_user.id
    master_id = callback_data.master_id
    service_id = callback_data.service_id
    logger.info("Выбор мастера для пользователя %s, master_id=%s, service_id=%s", user_id, master_id, service_id)
    # Длительность слота из SETTINGS
    try:
        duration = int(getattr(cfg, "SETTINGS", {}).get("slot_duration", 60))
    except (TypeError, ValueError):
        duration = 60

    await state.update_data(master_id=master_id, service_id=service_id)
    kb = await get_calendar_keyboard(service_id=service_id, master_id=master_id, service_duration_min=duration, user_id=user_id)
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        prompt = t("choose_date", lang)
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_date)
    await cb.answer()
    logger.info("Календарь отправлен для пользователя %s", user_id)


@client_router.callback_query(CalendarCB.filter())
async def navigate_calendar(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает навигацию по месяцам в календаре.

    Args:
        cb: CallbackQuery с данными навигации.
        callback_data: Данные callback'а с годом и месяцем.
    """
    user_id = cb.from_user.id
    logger.info("Навигация по календарю для пользователя %s, year=%s, month=%s", user_id, callback_data.year, callback_data.month)
    try:
        # Preserve custom multi-service duration if present in state
        service_duration_min = None
        sid = str(getattr(callback_data, "service_id", ""))
        if "+" in sid:
            try:
                data = await state.get_data()
                service_duration_min = int(data.get("multi_duration_min") or 0) or None
            except (TypeError, ValueError):
                service_duration_min = None
        kb = await get_calendar_keyboard(
            service_id=callback_data.service_id,
            master_id=callback_data.master_id,
            year=callback_data.year,
            month=callback_data.month,
            service_duration_min=service_duration_min or 60,
            user_id=user_id
        )
        if cb.message:
            lang = locale
            await safe_edit(cb.message, t("choose_date", lang), reply_markup=kb)
        await cb.answer()
        logger.info("Календарь обновлен для пользователя %s", user_id)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в navigate_calendar для пользователя %s: %s", user_id, e)
    # Unexpected exceptions are handled by centralized router-level error handlers


@client_router.callback_query(DateCB.filter())
async def select_date(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор даты из календаря и показывает доступные временные слоты."""
    cur_state = await state.get_state()
    user_id = cb.from_user.id
    selected_date = callback_data.date
    logger.info("Выбор даты для пользователя %s, date=%s", user_id, selected_date)
    try:
        from bot.app.services.client_services import get_available_time_slots
        # Определяем длительность слота: для мульти-услуг берем суммарную, иначе из конфигурации
        try:
            sid = str(getattr(callback_data, "service_id", ""))
            if "+" in sid:
                data = await state.get_data()
                duration = int(data.get("multi_duration_min") or 0) or int(getattr(cfg, "SETTINGS", {}).get("slot_duration", 60))
            else:
                duration = int(getattr(cfg, "SETTINGS", {}).get("slot_duration", 60))
        except (TypeError, ValueError):
            duration = 60

        try:
            base_dt = datetime.fromisoformat(selected_date)
        except ValueError as e:
            logger.error("Некорректный формат даты %s: %s", selected_date, e)
            lang = locale
            await cb.answer(t("invalid_date", lang))
            return

        slots = await get_available_time_slots(base_dt, callback_data.master_id, duration)
        time_labels = [s.strftime("%H:%M") for s in slots]
        time_values = [s.strftime("%H%M") for s in slots]

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        b = InlineKeyboardBuilder()
        if cur_state and "reschedule_select_date" in str(cur_state):
            # Client reschedule flow: use cres:time callbacks and keep booking_id from state
            data = await state.get_data()
            booking_id = data.get("cres_booking_id")
            # Ensure booking_id is an int for the typed callback; fall back to 0 if missing
            try:
                booking_id = int(booking_id) if booking_id is not None else 0
            except (TypeError, ValueError):
                booking_id = 0
            for label, value in zip(time_labels, time_values):
                # Use typed callback packing instead of manual f-strings
                cb_payload = pack_cb(RescheduleCB, action="time", booking_id=booking_id, date=selected_date, time=value)
                b.button(text=label, callback_data=cb_payload)
        else:
            # New booking flow
            sid = str(getattr(callback_data, "service_id", ""))
            for label, value in zip(time_labels, time_values):
                from bot.app.telegram.common.callbacks import TimeCB
                cb_payload = pack_cb(TimeCB, service_id=sid, master_id=callback_data.master_id, date=selected_date, time=value)
                b.button(text=label, callback_data=cb_payload)
        lang = locale
        from typing import cast, Any
        from bot.app.telegram.common.callbacks import NavCB
        b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
        b.adjust(3, 3, 3, 1, 1)
        kb = b.as_markup()

        if not time_values:
            if cb.message:
                await safe_edit(cb.message, t("no_time_for_date", lang), reply_markup=get_back_button())
            await cb.answer()
            return

        if cb.message:
            prefix = t("choose_time_on_date_prefix", lang)
            # Форматируем дату в DD.MM.YYYY
            formatted_date = base_dt.strftime("%d.%m.%Y")
            header = f"{prefix} {formatted_date}"
            await nav_push(state, header, kb)
            await safe_edit(cb.message, header, reply_markup=kb)
        await cb.answer()
        # For reschedule, transition to time selection state; else keep booking creation flow metadata
        if cur_state and "reschedule_select_date" in str(cur_state):
            await state.set_state(BookingStates.reschedule_select_time)
        else:
            await state.update_data(selected_date=selected_date)
        logger.info("Временные слоты показаны для пользователя %s на %s", user_id, selected_date)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в select_date для пользователя %s: %s", user_id, e)
        await cb.answer(i18n.t("error_retry", locale))


@client_router.callback_query(ClientMenuCB.filter(F.act == "services_multi"))
async def services_multi_entry(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Entry point for multi-service selection."""
    data = await state.get_data()
    selected = set(data.get("multi_selected") or [])
    import importlib
    _ck = importlib.import_module("bot.app.telegram.client.client_keyboards")
    kb = await getattr(_ck, "get_service_menu_multi")(selected)
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    prompt = t("choose_service", lang)
    if cb.message:
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.update_data(multi_selected=list(selected), current_screen="multi_select")
    await cb.answer()


@client_router.callback_query(ServiceToggleCB.filter())
async def svc_toggle(cb: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Toggle a service in the multi-selection and re-render keyboard."""
    sid = callback_data.service_id
    data = await state.get_data()
    selected = set(data.get("multi_selected") or [])
    if sid in selected:
        selected.remove(sid)
    else:
        selected.add(sid)
    await state.update_data(multi_selected=list(selected))
    import importlib
    _ck = importlib.import_module("bot.app.telegram.client.client_keyboards")
    kb = await getattr(_ck, "get_service_menu_multi")(selected)
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        await safe_edit(cb.message, t("choose_service", lang), reply_markup=kb)
    await cb.answer()


@client_router.callback_query(ClientMenuCB.filter(F.act == "svc_done"))
async def svc_done(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Finalize service selection: show masters that support all selected services."""
    data = await state.get_data()
    selected = list(set(data.get("multi_selected") or []))
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    if not selected:
        await cb.answer(t("choose_service", lang), show_alert=True)
        return
    # Find masters who offer all selected services
    from sqlalchemy import func
    from bot.app.domain.models import Master, MasterService
    async with get_session() as session:
        stmt = (
            select(Master.telegram_id, Master.name)
            .join(MasterService, MasterService.master_telegram_id == Master.telegram_id)
            .where(MasterService.service_id.in_(selected))
            .group_by(Master.telegram_id, Master.name)
            .having(func.count(func.distinct(MasterService.service_id)) == len(selected))
            .order_by(Master.name)
        )
        rows = list((await session.execute(stmt)).all())
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        b = InlineKeyboardBuilder()
        if rows:
            from bot.app.telegram.common.callbacks import MasterMultiCB
            for mid, name in rows:
                b.button(text=str(name or mid), callback_data=pack_cb(MasterMultiCB, master_id=int(mid)))
            b.adjust(2)
        else:
            b.button(text="—", callback_data="dummy")
        b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
        # Precompute total duration for later calendar
        try:
            agg = await get_services_duration_and_price(selected, online_payment=False)
            total_min = int(agg.get("total_minutes") or 60)
        except (SQLAlchemyError, TypeError, ValueError):
            total_min = 60
        await state.update_data(multi_selected=selected, multi_duration_min=total_min)
        if cb.message:
            await nav_push(state, t("choose_master", lang), b.as_markup())
            await safe_edit(cb.message, t("choose_master", lang), reply_markup=b.as_markup())
        await state.set_state(BookingStates.waiting_for_master)
        await cb.answer()


@client_router.callback_query(MasterMultiCB.filter())
async def master_multi(cb: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Proceed to calendar for multi-service booking with combined duration."""
    master_id = int(callback_data.master_id)
    data = await state.get_data()
    selected = list(data.get("multi_selected") or [])
    if not selected:
        await cb.answer(i18n.t("invalid_data", locale), show_alert=True)
        return
    total_min = int(data.get("multi_duration_min") or 60)
    service_id = "+".join(selected)
    kb = await get_calendar_keyboard(service_id=service_id, master_id=master_id, service_duration_min=total_min, user_id=cb.from_user.id)
    lang = locale
    if cb.message:
        await nav_push(state, t("choose_date", lang), kb)
        await safe_edit(cb.message, t("choose_date", lang), reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_date)
    await state.update_data(master_id=master_id)
    await cb.answer()


@client_router.callback_query(TimeCB.filter())
async def select_time_multi(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Confirm screen for multi-service booking before creation (TimeCB covers multi by allowing '+' in service_id)."""
    user_id = cb.from_user.id
    sid_joined = callback_data.service_id
    master_id_str = str(callback_data.master_id)
    date_str = callback_data.date
    time_compact = callback_data.time
    time_str = f"{time_compact[:2]}:{time_compact[2:]}"
    local_dt = datetime.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=LOCAL_TZ)
    _ = local_dt.astimezone(UTC)  # just validate
    ids = [s for s in sid_joined.split("+") if s]
    # Names and total price
    names = []
    for sid in ids:
        try:
            names.append(await get_service_name(sid))
        except SQLAlchemyError:
            names.append(sid)
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import CreateBookingCB
    try:
        payload = pack_cb(CreateBookingCB, service_id=sid_joined, master_id=int(master_id_str), date=date_str, time=time_compact)
    except (TypeError, ValueError):
        # Defensive: if master id is not int for some reason, fall back to raw strings
        payload = pack_cb(CreateBookingCB, service_id=sid_joined, master_id=master_id_str, date=date_str, time=time_compact)
    b.button(text=t("confirm", lang), callback_data=payload)
    b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    b.adjust(2)
    formatted_date = datetime.fromisoformat(date_str).strftime("%d.%m.%Y")
    header = (
        f"{t('confirm_create_header', lang)}\n\n"
        f"{t('service_label', lang)}: <b>{' + '.join(names)}</b>\n"
        f"{t('master_label', lang)}: <b>{master_id_str}</b>\n"
        f"{t('date_label', lang)}: <b>{formatted_date}</b>  {t('time_label', lang)}: <b>{time_str}</b>"
    )
    if cb.message:
        await nav_push(state, header, b.as_markup())
        await safe_edit(cb.message, header, reply_markup=b.as_markup())
    await cb.answer()


@client_router.callback_query(CreateBookingCB.filter())
async def confirm_create_booking_multi(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
    """Create a composite booking and show payment methods."""
    # CreateBookingCB.service_id may contain '+' joined ids for multi-service
    sid_joined = callback_data.service_id
    master_id_str = str(callback_data.master_id)
    date_str = callback_data.date
    time_compact = callback_data.time
    time_str = f"{time_compact[:2]}:{time_compact[2:]}"
    local_dt = datetime.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=LOCAL_TZ)
    slot_dt = local_dt.astimezone(UTC)
    ids = [s for s in sid_joined.split("+") if s]
    user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name if cb.from_user else str(cb.from_user.id))
    booking = await create_composite_booking(user.id, int(master_id_str), ids, slot_dt)

    # Prepare names string and header
    names = []
    for sid in ids:
        try:
            names.append(await get_service_name(sid))
        except SQLAlchemyError:
            names.append(sid)
    service_name = " + ".join(names)
    master_name: Optional[str] = None
    try:
        async with get_session() as session:
            res = await session.execute(select(Master.name).where(Master.telegram_id == int(master_id_str)))
            master_name = res.scalar_one_or_none()
    except SQLAlchemyError:
        pass
    formatted_date = datetime.fromisoformat(date_str).strftime("%d.%m.%Y")
    header, kb = await get_payment_keyboard(booking, service_name, master_name, cb.from_user.id, date=formatted_date)
    if cb.message:
        await nav_replace(state, header, kb)
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


## Legacy positional "date:" fallback removed: DateCB.filter() is used instead.
## Удален специализированный back_to_calendar: используется глобальная навигация

@client_router.callback_query(TimeCB.filter())
async def select_time(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Предварительное подтверждение перед созданием брони, затем оплата.

    Args:
        cb: CallbackQuery с данными выбранного времени.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = cb.from_user.id
    logger.info("Выбор времени (custom encoded) для пользователя %s", user_id)
    try:
        # Use typed TimeCB fields
        service_id = callback_data.service_id
        master_id_str = str(callback_data.master_id)
        date_str = callback_data.date
        time_compact = callback_data.time
        time_str = f"{time_compact[:2]}:{time_compact[2:]}"
        local_dt = datetime.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=LOCAL_TZ)
        slot_dt = local_dt.astimezone(UTC)

        # Preview confirmation before creating booking
        # Use cached service name if present
        state_data = await state.get_data()
        service_name = state_data.get("service_name") or await get_service_name(service_id)
        master_name: Optional[str] = None
        try:
            async with get_session() as session:
                res = await session.execute(select(Master.name).where(Master.telegram_id == int(master_id_str)))
                master_name = res.scalar_one_or_none()
        except SQLAlchemyError:
            logger.warning("Не удалось получить имя мастера %s", master_id_str)

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        b = InlineKeyboardBuilder()

        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale

        from bot.app.telegram.common.callbacks import CreateBookingCB
        try:
            payload = pack_cb(CreateBookingCB, service_id=service_id, master_id=int(master_id_str), date=date_str, time=time_compact)
        except (TypeError, ValueError):
            # Defensive: if master id is not int for some reason, fall back to raw strings
            payload = pack_cb(CreateBookingCB, service_id=service_id, master_id=master_id_str, date=date_str, time=time_compact)

        b.button(text=t("confirm", lang), callback_data=payload)
        b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
        b.adjust(2)

        # Форматируем дату в DD.MM.YYYY
        formatted_date = datetime.fromisoformat(date_str).strftime("%d.%m.%Y")

        header = (
            f"{t('confirm_create_header', lang)}\n\n"
            f"{t('service_label', lang)}: <b>{service_name}</b>\n"
            f"{t('master_label', lang)}: <b>{master_name or master_id_str}</b>\n"
            f"{t('date_label', lang)}: <b>{formatted_date}</b>  {t('time_label', lang)}: <b>{time_str}</b>"
        )
        if cb.message:
            await nav_push(state, header, b.as_markup())
            await safe_edit(cb.message, header, reply_markup=b.as_markup())
        await cb.answer()
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в select_time для пользователя %s: %s", user_id, e)
        # Use injected locale as fallback for the error message
        await cb.answer(i18n.t("create_failed_retry_later", locale), show_alert=True)

        
@client_router.callback_query(CreateBookingCB.filter())
async def confirm_create_booking(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
    """Creates the booking after user confirms, then shows payment options."""
    service_id = callback_data.service_id
    master_id_str = str(callback_data.master_id)
    date_str = callback_data.date
    time_compact = callback_data.time
    time_str = f"{time_compact[:2]}:{time_compact[2:]}"
    local_dt = datetime.fromisoformat(f"{date_str}T{time_str}").replace(tzinfo=LOCAL_TZ)
    slot_dt = local_dt.astimezone(UTC)
    user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name if cb.from_user else str(cb.from_user.id))
    booking = await create_booking(user.id, int(master_id_str), service_id, slot_dt)
    # Fallback: if for some reason snapshots are missing, set from service
    try:
        if not getattr(booking, "original_price_cents", None) or not getattr(booking, "final_price_cents", None):
            async with get_session() as session:
                svc = await session.get(Service, service_id)
                if svc and svc.price_cents:
                    booking.original_price_cents = svc.price_cents
                    booking.final_price_cents = svc.price_cents
                    await session.commit()
    except SQLAlchemyError:
        pass

    # Compute price for header
    price_info = await calculate_price(service_id, online_payment=False)
    try:
        setattr(booking, "final_price_cents", price_info.get("final_price_cents", 0))
    except (AttributeError, TypeError):
        pass

    # Get names
    state_data = await state.get_data()
    service_name = state_data.get("service_name") or await get_service_name(service_id)
    master_name: Optional[str] = None
    try:
        async with get_session() as session:
            res = await session.execute(select(Master.name).where(Master.telegram_id == int(master_id_str)))
            master_name = res.scalar_one_or_none()
    except SQLAlchemyError:
        pass

    # Форматируем дату в DD.MM.YYYY
    formatted_date = datetime.fromisoformat(date_str).strftime("%d.%m.%Y")

    # Передаём отформатированную дату в get_payment_keyboard
    header, kb = await get_payment_keyboard(booking, service_name, master_name, cb.from_user.id, date=formatted_date)
    if cb.message:
        await nav_replace(state, header, kb)
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()



@client_router.callback_query(PayCB.filter(F.action == "prep_cash"))
async def pay_cash_prepare(cb: CallbackQuery, callback_data: Any, locale: str) -> None:
    """Shows a confirmation screen before confirming cash payment (booking confirmation).

    Uses the canonical BookingDetails builder + pure formatter for consistent output.
    """
    from bot.app.services.shared_services import build_booking_details, format_booking_details_text
    booking_id = int(callback_data.booking_id)
    lang = locale
    async with get_session() as session:
        b = await session.get(Booking, booking_id)
        if not b:
            await cb.answer(t("booking_not_found", lang), show_alert=True)
            return
    # Try to compute a localized date string for display
    try:
        dt_local = b.starts_at.astimezone(LOCAL_TZ)
        date_txt = f"{dt_local:%d.%m.%Y}"
    except (AttributeError, ValueError):
        date_txt = None

    # Build canonical details and format for display
    details_obj = await build_booking_details(b, service_name=None, master_name=None, user_id=cb.from_user.id, date=date_txt, lang=lang)
    details = format_booking_details_text(details_obj, lang)
    header = f"{details}\n\n{t('confirm', lang)}?"

    kb = get_simple_kb([
        (t("confirm", lang), pack_cb(PayCB, action="conf_cash", booking_id=int(booking_id))),
        (t("back_to_payment_methods", lang), pack_cb(PayCB, action="back_methods", booking_id=int(booking_id))),
    ], cols=1)
    if cb.message:
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


@client_router.callback_query(RatingCB.filter())
async def handle_rating(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Handle booking rating selection (typed RatingCB)."""
    try:
        booking_id = int(callback_data.booking_id)
        rating = int(callback_data.rating)
    except (TypeError, ValueError):
        try:
            await cb.answer()
        except TelegramAPIError:
            pass
        return
    try:
        # Attempt to record rating; service will validate booking status
        res = await record_booking_rating(booking_id, rating)
        lang = locale
        # Acknowledge with a friendly toast
        await cb.answer(t("thanks_for_feedback", lang))
    except SQLAlchemyError as e:
        logger.exception("Ошибка записи рейтинга: %s", e)
        lang = locale
        try:
            await cb.answer(t("rating_save_failed", lang), show_alert=True)
        except TelegramAPIError:
            pass


@client_router.callback_query(PayCB.filter(F.action == "conf_cash"))
async def pay_cash(cb: CallbackQuery, callback_data: Any, locale: str) -> None:
    """
    Обрабатывает выбор оплаты наличными:
    - обновляет статус брони на CONFIRMED,
    - уведомляет клиента, мастера и админов.
    """
    booking_id = int(callback_data.booking_id)

    # Analytics: confirm cash clicked
    logger.info("analytics.cash_confirm_click user_id=%s booking_id=%s", cb.from_user.id, booking_id)

    # Обновляем бронь в базе
    async with get_session() as session:
        b = await session.get(Booking, booking_id)
        if not b:
            lang = locale
            await cb.answer(t("booking_not_found", lang), show_alert=True)
            return
        await session.execute(
            update(Booking)
            .where(Booking.id == booking_id)
            .values(status=BookingStatus.CONFIRMED, cash_hold_expires_at=None)
        )
        await session.commit()

    # Сообщение клиенту
    if cb.message:
        lang = locale
        await safe_edit(
            cb.message,
            t("cash_confirmed_message", lang),
            reply_markup=home_kb()
        )

    await cb.answer()
    logger.info("Бронь %s подтверждена как оплата наличными", booking_id)

    # Unified notifications via shared helper
    bot = getattr(cb, "bot", None)
    if bot and b:
        recipients = [int(getattr(b, "master_id", 0))] + list(getattr(cfg, "ADMIN_IDS", set()))
        await send_booking_notification(bot, booking_id, "cash_confirmed", recipients)



## Deprecated: old global_back handler removed; use NavCB(act='back'|'root'|'role_root')



## Удален специализированный back_to_masters: используется глобальная навигация


@client_router.callback_query(MyBookingsCB.filter())
async def my_bookings(cb: CallbackQuery, callback_data: Any, state: FSMContext, locale: str, replace_screen: bool = False) -> None:
    """ Отображает активные и предстоящие брони пользователя. """
    user_id = cb.from_user.id
    logger.info("Запрос списка бронирований для пользователя %s", user_id)
    await cb.answer("Завантаження...", show_alert=False)
    try:
        user = await get_or_create_user(
            user_id, cb.from_user.full_name if cb.from_user else str(user_id)
        )
        now = datetime.now(UTC)

        # Определяем новый фильтр и страницу из callback_data
        mode_val = getattr(callback_data, "mode", None)
        new_filter = mode_val if mode_val in ("upcoming", "completed") else "upcoming"
        # Extract page number; CallbackData may carry page=None so coerce to 0
        page_raw = getattr(callback_data, "page", None)
        try:
            page = int(page_raw) if page_raw is not None else 0
        except (TypeError, ValueError):
            page = 0
        if page < 0:
            page = 0  # Защита от отрицательных страниц

        # Determine whether this navigation should replace the current screen
        prev_state = await state.get_data() or {}
        prev_page = prev_state.get("my_bookings_page", 0)
        prev_filter = prev_state.get("my_bookings_filter", "upcoming")
        # If caller explicitly requested replace_screen, honor it; otherwise only replace when page changed.
        # We intentionally do NOT replace when the user switches tabs (upcoming <-> completed)
        # so that a Back action from 'completed' reliably returns to the previous 'upcoming' view.
        effective_replace = bool(replace_screen) or (page != prev_page)

        # Сохраняем новый фильтр, страницу и текущий экран in state
        await state.update_data(my_bookings_filter=new_filter, my_bookings_page=page, current_screen="my_bookings")

        filter_mode = new_filter

        async with get_session() as session:
            # Подсчёт записей для обеих вкладок
            upcoming_count_stmt = (
                select(func.count())
                .select_from(Booking)
                .where(
                    Booking.user_id == user.id,
                    Booking.starts_at >= now,
                    Booking.status.notin_([
                        BookingStatus.CANCELLED,
                        BookingStatus.DONE,
                        BookingStatus.NO_SHOW,
                        BookingStatus.EXPIRED,
                    ])
                )
            )
            upcoming_count_result = await session.execute(upcoming_count_stmt)
            upcoming_count = upcoming_count_result.scalar() or 0

            completed_count_stmt = (
                select(func.count())
                .select_from(Booking)
                .where(
                    Booking.user_id == user.id,
                    Booking.status.in_([BookingStatus.DONE, BookingStatus.NO_SHOW, BookingStatus.CANCELLED]),
                )
            )
            completed_count_result = await session.execute(completed_count_stmt)
            completed_count = completed_count_result.scalar() or 0

            if filter_mode == "completed":
                items_per_page = 5  # Количество записей на страницу
                offset = page * items_per_page  # Смещение для текущей страницы
                stmt = (
                    select(
                        Booking.id,
                        Booking.master_id,
                        Booking.service_id,
                        Booking.status,
                        Booking.starts_at,
                        Booking.original_price_cents,
                        Booking.final_price_cents,
                        Master.name.label("master_name"),
                    )
                    .join(Master, Master.telegram_id == Booking.master_id, isouter=True)
                    .where(
                        Booking.user_id == user.id,
                        Booking.status.in_([BookingStatus.DONE, BookingStatus.NO_SHOW, BookingStatus.CANCELLED]),
                    )
                    .order_by(Booking.starts_at.desc())
                    .limit(items_per_page)
                    .offset(offset)
                )
                total_count = completed_count
            else:
                stmt = (
                    select(
                        Booking.id,
                        Booking.master_id,
                        Booking.service_id,
                        Booking.status,
                        Booking.starts_at,
                        Booking.original_price_cents,
                        Booking.final_price_cents,
                        Master.name.label("master_name"),
                    )
                    .join(Master, Master.telegram_id == Booking.master_id, isouter=True)
                    .where(
                        Booking.user_id == user.id,
                        Booking.starts_at >= now,
                        Booking.status.notin_([
                            BookingStatus.CANCELLED,
                            BookingStatus.DONE,
                            BookingStatus.NO_SHOW,
                            BookingStatus.EXPIRED,
                        ])
                    )
                    .order_by(Booking.starts_at)
                )
                total_count = upcoming_count

            result = await session.execute(stmt)
            rows = list(result.all())

        texts_map = getattr(cfg, "TEXTS", {})
        try:
            from bot.app.translations import t
        except ImportError:
            t = None  # type: ignore
        lang = await resolve_locale(state, locale, user.id)
        kb = None

        if not rows:
            # No rows for the current filter. If we're on the 'upcoming' tab but
            # there are completed bookings, offer a direct button to switch to
            # the completed view so users without future bookings can still see
            # their past bookings.
            if filter_mode == "upcoming" and completed_count > 0:
                from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
                from bot.app.telegram.common.callbacks import MyBookingsCB, NavCB
                comp_txt = (t("completed", lang) if t else "Завершені") + f" ({completed_count})"
                back_txt = (t("back", lang) if t else "⬅️ Back")
                rows_buttons = [
                    [InlineKeyboardButton(text=comp_txt, callback_data=pack_cb(MyBookingsCB, mode="completed", page=0))],
                    [InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back"))],
                ]
                kb = InlineKeyboardMarkup(inline_keyboard=rows_buttons)
                text = texts_map.get("no_bookings", i18n.t("no_bookings", lang))
            else:
                text = texts_map.get("no_bookings", i18n.t("no_bookings", lang))
                kb = get_back_button()
        else:
            if t:
                if filter_mode == "upcoming":
                    title = f"{t('upcoming_bookings_title', lang)} ({upcoming_count})"
                else:
                    title = t("completed_bookings_title", lang)
            else:
                if filter_mode == "upcoming":
                    title = f"{i18n.t('upcoming_bookings_title', lang)} ({upcoming_count})"
                else:
                    title = i18n.t("completed_bookings_title", lang)
            if filter_mode == "completed" and total_count > 0:
                items_per_page = 5  # Должно совпадать с лимитом в SQL
                total_pages = (total_count + items_per_page - 1) // items_per_page  # Округление вверх
                title = f"{title} ({total_count} записів, Сторінка {page + 1} з {total_pages})"
            elif filter_mode == "completed":
                title = f"{title} (0 записів)"
            text = f"{title}\n\n"

            from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
            from bot.app.telegram.common.callbacks import MyBookingsCB, BookingActionCB, NavCB

            rows_buttons: list[list[InlineKeyboardButton]] = []

            # Top control: on upcoming screen show button to go to completed
            comp_txt = (t("completed", lang) if t else "Завершені") + f" ({completed_count})"
            if filter_mode == "upcoming":
                rows_buttons.append([
                    InlineKeyboardButton(text=comp_txt, callback_data=pack_cb(MyBookingsCB, mode="completed", page=0))
                ])

            # Pagination row for completed mode (if any)
            if filter_mode == "completed":
                pag_row: list[InlineKeyboardButton] = []
                items_per_page = 5
                if page > 0:
                    pag_row.append(InlineKeyboardButton(text=f"⬅️ Назад (Стор. {page})", callback_data=pack_cb(MyBookingsCB, mode="completed", page=page - 1)))
                if total_count > (page + 1) * items_per_page:
                    pag_row.append(InlineKeyboardButton(text=f"Вперёд (Стор. {page + 2}) ➡️", callback_data=pack_cb(MyBookingsCB, mode="completed", page=page + 1)))
                if pag_row:
                    rows_buttons.append(pag_row)

            # Per-booking rows (each booking gets its own row)
            det_txt = (t("details", lang) if t else "Details")
            for row in rows:
                m = row._mapping
                bid = m["id"]
                master_id = m.get("master_id")
                service_id = m["service_id"]
                status = m["status"]
                starts_at = m["starts_at"]
                orig_cents = m.get("original_price_cents")
                final_cents = m.get("final_price_cents")

                master_name = m.get("master_name") or str(master_id)
                dt = starts_at.astimezone(LOCAL_TZ).strftime("%d.%m %H:%M")
                service_name = await get_service_name(service_id)

                price = final_cents if final_cents is not None else orig_cents
                if price in (None, 0):
                    try:
                        async with get_session() as session:
                            svc = await session.get(Service, service_id)
                            if svc and getattr(svc, "price_cents", None):
                                price = svc.price_cents
                    except SQLAlchemyError:
                        pass
                price_txt = format_money_cents(price, "UAH") if price is not None else ""

                st_val = status.value if hasattr(status, "value") else str(status)
                st = status_to_emoji(st_val)

                button_text = f"{st} {dt}  {service_name[:24]}  {master_name[:16]}  {price_txt}"
                rows_buttons.append([
                    InlineKeyboardButton(text=button_text, callback_data=pack_cb(BookingActionCB, act="details", booking_id=int(bid)))
                ])

            # Back button on its own bottom row
            back_txt = (t("back", lang) if t else "⬅️ Back")
            rows_buttons.append([
                InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
            ])

            kb = InlineKeyboardMarkup(inline_keyboard=rows_buttons)

        if cb.message:
            if effective_replace:
                await nav_replace(state, text, kb or get_back_button())
            else:
                await nav_push(state, text, kb or get_back_button())
            await safe_edit(cb.message, text, reply_markup=(kb or get_back_button()))
            # Ограничиваем длину текста для Telegram
            text = text[:4096]

        await cb.answer()
        logger.info(
            "Список бронирований отображен для пользователя %s (количество=%d)",
            user_id, len(rows)
        )

    except SQLAlchemyError as e:
        logger.error("Ошибка базы данных в my_bookings для пользователя %s: %s", user_id, e)
        if cb.message:
            try:
                lang = await resolve_locale(state, locale, cb.from_user.id)
            except (SQLAlchemyError, TypeError, ValueError):
                lang = getattr(cfg, "SETTINGS", {}).get("language", "uk")
            await safe_edit(
                cb.message,
                i18n.t("error_retry", lang),
                reply_markup=get_back_button()
            )
        await cb.answer()
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в my_bookings для пользователя %s: %s", user_id, e)
    # Unexpected exceptions are handled by centralized router-level error handlers

@client_router.callback_query(BookingActionCB.filter(F.act == "details"))
async def client_booking_details(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Показывает детали записи клиента с расширенной информацией и действиями."""
    booking_id = int(callback_data.booking_id)

    async with get_session() as session:
        b = await session.get(Booking, booking_id)
        master_name = None
        if b and getattr(b, "master_id", None):
            master = await session.get(Master, b.master_id)
            master_name = master.name if master else None

    lang = await resolve_locale(state, locale, cb.from_user.id)
    if not b:
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return

    # Проверка владельца
    user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name or str(cb.from_user.id))
    if int(user.id) != int(b.user_id or 0):
        await cb.answer(t("no_permission", lang), show_alert=True)
        return

    # Обновляем state
    await state.update_data(current_screen="booking_details", current_booking_id=booking_id)

    # Build canonical booking details via shared service to ensure consistency
    from bot.app.services import shared_services

    bd = await shared_services.build_booking_details(b, master_name=master_name, user_id=cb.from_user.id, lang=lang)

    # Base formatted summary (pure formatter)
    text = shared_services.format_booking_details_text(bd, lang)

    # Append time/duration and client comment if available (client-facing extras)
    try:
        if bd.starts_at:
            try:
                dt_local = bd.starts_at.astimezone(LOCAL_TZ)
                dur_val = None
                if isinstance(bd.raw, dict):
                    dur_val = bd.raw.get("duration_minutes")
                if dur_val is None:
                    dur_val = getattr(b, "duration_minutes", None)
                duration_txt = f"{dur_val} {t('minutes_short', lang)}" if isinstance(dur_val, int) and dur_val > 0 else ""
                time_line = f"📅 {dt_local:%d.%m.%Y} ⏰ {dt_local:%H:%M} {duration_txt}".strip()
                text = text + "\n" + time_line
            except (AttributeError, ValueError):
                pass
    except (AttributeError, ValueError):
        pass

    try:
        comment = None
        if isinstance(bd.raw, dict):
            comment = bd.raw.get("comment")
        if comment is None:
            comment = getattr(b, "comment", None)
        if comment:
            text = text + "\n" + f"💬 {comment}"
    except (AttributeError, TypeError):
        pass

    # Build action buttons using bd flags
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    bld = InlineKeyboardBuilder()

    can_reschedule = bool(getattr(bd, "can_reschedule", False))
    can_cancel = bool(getattr(bd, "can_cancel", False))

    # Informational note when no actions available
    if not (can_reschedule or can_cancel):
        terminal_set = {
            BookingStatus.CANCELLED,
            BookingStatus.DONE,
            BookingStatus.NO_SHOW,
            BookingStatus.EXPIRED,
        }
        is_terminal = getattr(b, "status", None) in terminal_set
        if is_terminal:
            info_txt = t("booking_not_active", lang)
        else:
            try:
                lock_h = int(getattr(cfg, "get_client_reschedule_lock_hours", lambda: 3)())
            except (TypeError, ValueError):
                lock_h = 3
            info_txt = t("cancel_too_close", lang).format(hours=lock_h)
        text = text + "\n\n" + f"ℹ️ {info_txt}"

    # Reschedule button (only when allowed)
    if can_reschedule:
        from bot.app.telegram.common.callbacks import RescheduleCB
        cb_payload = pack_cb(RescheduleCB, action="start", booking_id=int(bd.booking_id))
        bld.button(text=t("reschedule", lang), callback_data=cb_payload)

    # Cancel button (only when allowed)
    if can_cancel:
        from bot.app.telegram.common.callbacks import BookingActionCB
        cb_payload = pack_cb(BookingActionCB, act="cancel_confirm", booking_id=int(bd.booking_id))
        bld.button(text=t("cancel", lang), callback_data=cb_payload)

    # Always include Back
    bld.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))

    # Layout
    if can_reschedule and can_cancel:
        bld.adjust(2, 1)
    elif can_reschedule or can_cancel:
        bld.adjust(2)
    else:
        bld.adjust(1)

    if cb.message:
        await nav_push(state, f"{t('booking_details_title', lang)} #{bd.booking_id}", bld.as_markup())
        await safe_edit(cb.message, text, reply_markup=bld.as_markup())

    await cb.answer()

@client_router.callback_query(BookingActionCB.filter(F.act == "cancel_confirm"))
async def cancel_booking_confirm(cb: CallbackQuery, callback_data, locale: str) -> None:
    """Ask for confirmation before cancelling a booking."""
    booking_id = int(callback_data.booking_id)
    # Enforce client cancellation lock window before even showing confirm
    lang = await resolve_locale(None, locale, cb.from_user.id)
    try:
        try:
            lock_h = int(getattr(cfg, "SETTINGS", {}).get("client_cancel_lock_hours", 3))
        except (TypeError, ValueError):
            lock_h = 3
        lock_h = int(lock_h)
    except (TypeError, ValueError):
        lock_h = 3
    try:
        async with get_session() as session:
            b = await session.get(Booking, booking_id)
        # Disallow showing confirmation for terminal bookings
        terminal_statuses = {
            BookingStatus.CANCELLED,
            BookingStatus.DONE,
            BookingStatus.NO_SHOW,
            BookingStatus.EXPIRED,
        }
        if b and getattr(b, "status", None) in terminal_statuses:
            await cb.answer(t("booking_not_active", lang), show_alert=True)
            return
        if b and (b.starts_at - datetime.now(UTC)).total_seconds() < lock_h * 3600:
            logger.info("analytics.cancel_too_close preconfirm user_id=%s booking_id=%s hours=%s", cb.from_user.id, booking_id, lock_h)
            await cb.answer(t("cancel_too_close", lang).format(hours=lock_h), show_alert=True)
            return
    except SQLAlchemyError:
        pass
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    cancel_payload = pack_cb(BookingActionCB, act="cancel", booking_id=int(booking_id))
    b.button(text=t("confirm", lang), callback_data=cancel_payload)
    b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    b.adjust(2)
    if cb.message:
        await safe_edit(cb.message, t("cancel_confirm_question", lang), reply_markup=b.as_markup())
    await cb.answer()

@client_router.callback_query(MasterProfileCB.filter())
async def master_profile_handler(cb: CallbackQuery, callback_data: Any, state: FSMContext, locale: str) -> None:
    """Обрабатывает переход в профиль мастера из меню выбора."""
    master_id = int(callback_data.master_id)
    service_id = callback_data.service_id
    await show_master_profile(cb, master_id, service_id, state, locale)


async def show_master_profile(cb: CallbackQuery, master_id: int, service_id: str, state: FSMContext, locale: str) -> None:
    """Отображает профиль мастера с кнопкой для продолжения бронирования."""
    lang = locale
    # Fetch data via master service and use pure formatter
    from bot.app.services import master_services, shared_services

    data = await master_services.get_master_profile_data(master_id)
    text = shared_services.format_master_profile_text(data, lang, with_title=False)

    # Создаем клавиатуру с кнопкой "Записаться" и "Назад"
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from typing import cast, Any
    builder = InlineKeyboardBuilder()
    builder.button(
        text=t("book_button", lang) if t("book_button", lang) != "book_button" else "Записатися",
        callback_data=pack_cb(MasterSelectCB, service_id=service_id, master_id=master_id),
    )
    builder.button(
        text=t("back", lang) if t("back", lang) != "back" else "⬅️ Назад",
        callback_data=pack_cb(NavCB, act="back"),
    )
    builder.adjust(1, 1)

    if cb.message:
        await nav_push(state, text, builder.as_markup())
        await safe_edit(cb.message, text, reply_markup=builder.as_markup())
    await cb.answer()
    logger.info("Профиль мастера %s показан пользователю %s", master_id, cb.from_user.id)
    # Any unexpected exceptions will be handled by centralized router error handlers

@client_router.callback_query(ClientMenuCB.filter(F.act == "contacts"))
async def contacts(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Отображает контактную информацию салона из конфигурации.
       Телефон и адрес показываются текстом, Instagram — кликабельной ссылкой «📷 Instagram».
    """
    user_id = cb.from_user.id
    logger.info("Запрос контактов для пользователя %s", user_id)
    try:
        contacts_map = getattr(cfg, "CONTACTS", {})
        try:
            from bot.app.translations import t
        except ImportError:
            t = None  # type: ignore

        lang = locale

        phone = contacts_map.get("phone")
        insta = contacts_map.get("instagram")
        address = contacts_map.get("address")

        title_txt = (t("contacts_title", lang) if t else i18n.t("contacts_title", lang))
        addr_lbl = (t("address_label", lang) if t else i18n.t("address_label", lang))
        phone_lbl = (t("phone_label", lang) if t else i18n.t("phone_label", lang))

        # Формируем текст
        lines = [f"<b>{title_txt}</b>"]
        kb = None
        if address:
            maps_url = f"https://www.google.com/maps/search/?api=1&query={address.replace(' ', '+')}"
            # Show plain address in the text (no external link text that may render
            # a provider label). Provide a separate inline URL button "📍 На карте".
            lines.append(f"{addr_lbl}: {address}")
            try:
                # build a small keyboard with a map link + back button
                from aiogram.utils.keyboard import InlineKeyboardBuilder
                from aiogram.types import InlineKeyboardButton
                kb_builder = InlineKeyboardBuilder()
                kb_builder.button(text="📍 На карте", url=maps_url)
                # back button (text localized inside get_back_button); mirror its callback
                kb_builder.button(text="⬅️ Назад", callback_data=pack_cb(NavCB, act="back"))
                kb = kb_builder.as_markup()
            except (ImportError, AttributeError, TypeError):
                kb = None
        if phone:
            phone_clean = phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            lines.append(f"{phone_lbl}: <a href='tel:{phone_clean}'>{phone}</a>")
        if insta:
            # Только одна строка: 📷 Instagram (кликабельно)
            lines.append(f"<a href='{insta}'>📷 Instagram</a>")

        text = "\n".join(lines)

        if cb.message:
            await nav_push(state, title_txt, get_back_button())
            # prefer the custom keyboard with map link if available
            await safe_edit(cb.message, text, reply_markup=(kb or get_back_button()), parse_mode="HTML")
        await cb.answer()
        logger.info("Контакты отображены для пользователя %s", user_id)

    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в contacts для пользователя %s: %s", user_id, e)
        if cb.message:
            lang = locale
            await safe_edit(cb.message, i18n.t("error_retry", lang), reply_markup=get_back_button())
        await cb.answer()
    # Unexpected exceptions are handled by centralized router-level error handlers

@client_router.callback_query(RescheduleCB.filter(F.action == "start"))
async def client_reschedule_start(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Start client reschedule: show calendar for the booking's master/service."""
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    booking_id = int(callback_data.booking_id)
    async with get_session() as session:
        b = await session.get(Booking, booking_id)
    if not b or b.user_id is None:
        await cb.answer(t("booking_not_found", lang), show_alert=True); return
    # Only allow the booking owner
    user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name or str(cb.from_user.id))
    if int(user.id) != int(b.user_id):
        await cb.answer(t("no_permission", lang), show_alert=True); return
    # Disallow rescheduling of already finished/cancelled/expired bookings
    terminal_statuses = {
        BookingStatus.CANCELLED,
        BookingStatus.DONE,
        BookingStatus.NO_SHOW,
        BookingStatus.EXPIRED,
    }
    if getattr(b, "status", None) in terminal_statuses:
        await cb.answer(t("booking_not_active", lang), show_alert=True)
        return
    # Prepare calendar
    try:
        duration = int(getattr(cfg, "SETTINGS", {}).get("slot_duration", 60))
    except (TypeError, ValueError):
        duration = 60
    kb = await get_calendar_keyboard(service_id=b.service_id, master_id=b.master_id, service_duration_min=duration, user_id=cb.from_user.id)
    if cb.message:
        await nav_push(state, f"{t('reschedule_pick_date', lang)}", kb, lang=lang)
        await safe_edit(cb.message, f"{t('reschedule_pick_date', lang)}", reply_markup=kb)
    await state.update_data(cres_booking_id=booking_id, service_id=b.service_id, master_id=b.master_id)
    await state.set_state(BookingStates.reschedule_select_date)
    await cb.answer()


## reschedule-specific DateCB handler removed; select_date covers both flows now


@client_router.callback_query(RescheduleCB.filter(F.action == "time"))
async def client_reschedule_time(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    lang = await resolve_locale(state, locale, cb.from_user.id)
    cur = await state.get_state()
    if not cur or "reschedule_select_time" not in str(cur):
        return
    # lang already computed from nav/state/locale/helper above
    # Use typed callback_data fields: booking_id, date, time
    try:
        booking_id_str = str(callback_data.booking_id)
        date_str = callback_data.date
        time_compact = callback_data.time
    except (AttributeError, TypeError):
        await cb.answer(t("invalid_data", lang), show_alert=True)
        return
    hh, mm = time_compact[:2], time_compact[2:]
    from bot.app.services.client_services import LOCAL_TZ, UTC
    local_dt = datetime.fromisoformat(f"{date_str}T{hh}:{mm}").replace(tzinfo=LOCAL_TZ)
    new_dt_utc = local_dt.astimezone(UTC)
    # Confirm screen + apply lock check right before confirm
    from bot.app.services.client_services import LOCAL_TZ as _LTZ, UTC as _UTC
    if (local_dt.astimezone(_UTC) - datetime.now(_UTC)).total_seconds() < getattr(cfg, "get_client_reschedule_lock_hours", lambda: 3)() * 3600:
        await cb.answer(t("reschedule_too_close", lang), show_alert=True)
        return
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from bot.app.telegram.common.callbacks import RescheduleCB
    b = InlineKeyboardBuilder()
    payload = pack_cb(RescheduleCB, action="confirm", booking_id=int(booking_id_str), date=date_str, time=f"{hh}{mm}")
    b.button(text=t("confirm", lang), callback_data=payload)
    b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    b.adjust(2)
    if cb.message:
        await safe_edit(cb.message, f"{t('reschedule_confirm_time_prefix', lang)} {date_str} {hh}:{mm}?", reply_markup=b.as_markup())
    await cb.answer()
    # Let router-level handler manage unexpected exceptions; no local catch-all


@client_router.callback_query(RescheduleCB.filter(F.action == "confirm"))
async def client_reschedule_confirm(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Подтверждает перенос бронирования на новое время."""
    lang = await resolve_locale(state, locale, cb.from_user.id)
    booking_id = int(callback_data.booking_id)
    date_str = callback_data.date
    time_compact = callback_data.time
    hh, mm = time_compact[:2], time_compact[2:]
    local_dt = datetime.fromisoformat(f"{date_str}T{hh}:{mm}").replace(tzinfo=LOCAL_TZ)
    new_dt_utc = local_dt.astimezone(UTC)

    # Ownership check and update
    async with get_session() as session:
        b = await session.get(Booking, booking_id)
        if not b:
            await cb.answer(t("booking_not_found", lang), show_alert=True)
            return

        user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name or str(cb.from_user.id))
        if int(user.id) != int(b.user_id):
            await cb.answer(t("no_permission", lang), show_alert=True)
            return

            b.starts_at = new_dt_utc
            b.cash_hold_expires_at = None
            await session.commit()

        # Notify master and admins (unified helper)
        bot = getattr(cb, "bot", None)
        if bot and b:
            recipients = [int(getattr(b, "master_id", 0))] + list(getattr(cfg, "ADMIN_IDS", set()))
            await send_booking_notification(bot, booking_id, "rescheduled_by_client", recipients)

        # После действия показываем кнопки для возврата к деталям или списку
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        from bot.app.telegram.common.callbacks import BookingActionCB, MyBookingsCB
        kb.button(text=t("back_to_details", lang), callback_data=pack_cb(BookingActionCB, act="details", booking_id=booking_id))
        kb.button(text=t("list_button", lang), callback_data=pack_cb(MyBookingsCB, mode="all"))
        kb.adjust(1, 1)

        if cb.message:
            await safe_edit(cb.message, t("reschedule_done", lang), reply_markup=kb.as_markup())
        await cb.answer(t("reschedule_done_toast", lang))





@client_router.callback_query(PayCB.filter(F.action == "online"))
async def pay_online(cb: CallbackQuery, callback_data, locale: str) -> None:
    """Генерирует счет Telegram для онлайн-оплаты."""
    try:
        booking_id = int(callback_data.booking_id)
    except (TypeError, ValueError):
        await cb.answer()
        return
    user_id = cb.from_user.id
    logger.info("Онлайн-оплата для брони %s пользователем %s", booking_id, user_id)

    # Guard: ensure online payments are currently available
    if not is_online_payments_available():
        await cb.answer(t("online_payments_unavailable", locale))
        return

    from bot.app.domain.models import Service, Master
    async with get_session() as session:
        booking = await session.get(Booking, booking_id)
        if not booking:
            await cb.answer(t("booking_not_found", locale))
            return

        # Try to get service snapshot/name
        service = await session.get(Service, booking.service_id)
        service_name = service.name if service else t("service_label", locale)
        try:
            from bot.app.domain.models import BookingItem as BI, Service as Svc
            rows = list((await session.execute(
                select(BI.service_id, Svc.name)
                .join(Svc, Svc.id == BI.service_id)
                .where(BI.booking_id == booking_id)
            )).all())
            if rows:
                names = [r[1] or r[0] for r in rows]
                service_name = " + ".join(names)
        except SQLAlchemyError:
            pass

        master = await session.get(Master, booking.master_id)
        master_name = master.name if master else t("master_label", locale)

        # Проверка цены (fallback to Service.price_cents if snapshots are missing)
        price_cents = booking.final_price_cents or booking.original_price_cents
        if not price_cents and service and service.price_cents:
            price_cents = service.price_cents
        if not price_cents or price_cents <= 0:
            await cb.answer(t("invoice_missing_price", locale), show_alert=True)
            return

        currency = service.currency if service and service.currency else "UAH"

    prices = [LabeledPrice(label=f"{service_name} у {master_name}", amount=price_cents)]

    provider_token = get_telegram_provider_token() or ""
    if not provider_token:
        await cb.answer(t("online_payments_unavailable", locale))
        return

    if cb.message:
        await cb.message.answer_invoice(
            title=t("invoice_title", locale),
            description=f"{service_name} у {master_name} {booking.starts_at.strftime('%d.%m %H:%M')}",
            payload=f"booking_{booking_id}",
            provider_token=provider_token,
            currency=currency,
            prices=prices,
            start_parameter="pay_online",
        )

    # Обновляем статус брони
    async with get_session() as session:
        booking = await session.get(Booking, booking_id)
        if booking:
            booking.status = BookingStatus.PENDING_PAYMENT
            await session.commit()

    await cb.answer()
    logger.info("Счет для онлайн-оплаты отправлен для брони %s", booking_id)


@client_router.callback_query(PayCB.filter(F.action == "prep_online"))
async def pay_online_prepare(cb: CallbackQuery, callback_data, locale: str) -> None:
    """Shows a confirmation screen before issuing the Telegram invoice."""
    try:
        booking_id = int(callback_data.booking_id)
    except (TypeError, ValueError):
        await cb.answer(); return
    user_id = cb.from_user.id

    lang = await resolve_locale(None, locale, user_id)

    # Guard: ensure online payments are currently available
    if not is_online_payments_available():
        await cb.answer(t("online_payments_unavailable", lang))
        return

    from bot.app.services.shared_services import build_booking_details, format_booking_details_text

    bd = await build_booking_details(int(booking_id), user_id=user_id, lang=lang)
    if not bd or not getattr(bd, 'booking_id', None):
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return

    service_name = bd.service_name or t("service_label", lang)
    master_name = bd.master_name or t("master_label", lang)
    date_txt = bd.date_str or None

    kb = get_simple_kb([
        (t("pay_now", lang), pack_cb(PayCB, action="online", booking_id=int(booking_id))),
        (t("back_to_payment_methods", lang), pack_cb(PayCB, action="back_methods", booking_id=int(booking_id))),
    ], cols=1)

    # Build header from canonical formatter (client view)
    header = format_booking_details_text(bd, lang)
    header = f"<b>{t('pay_online_confirm_title', lang)}</b>\n" + header + "\n\n" + f"{t('confirm', lang)}?"
    if cb.message:
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


@client_router.callback_query(PayCB.filter(F.action == "back_methods"))
async def pay_back_methods(cb: CallbackQuery, callback_data, locale: str) -> None:
    """Return to the payment method selection for a booking."""
    booking_id = int(callback_data.booking_id)
    lang = locale
    from bot.app.services.shared_services import build_booking_details

    bd = await build_booking_details(int(booking_id), user_id=cb.from_user.id, lang=lang)
    if not bd or not getattr(bd, 'booking_id', None):
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return

    # Delegate to the shared keyboard builder; pass raw data if available to avoid extra DB lookups
    booking_payload = bd.raw if bd.raw else bd.booking_id
    header, kb = await get_payment_keyboard(booking_payload, bd.service_name or t("service_label", lang), bd.master_name, cb.from_user.id, date=bd.date_str)
    if cb.message:
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


@client_router.pre_checkout_query()
async def pre_checkout_query(pre_checkout_query: PreCheckoutQuery) -> None:
    """Подтверждает предварительный запрос на оплату.

    Args:
        pre_checkout_query: Запрос на предварительную проверку оплаты.
    """
    await pre_checkout_query.answer(ok=True)
    logger.info("Предварительный запрос на оплату подтвержден")


@client_router.message(F.successful_payment)
async def successful_payment(message: Message, locale: str) -> None:
    """Обрабатывает успешную оплату: обновляет статус брони на PAID, уведомляет клиента, мастера и админов."""
    sp = getattr(message, "successful_payment", None)
    if not sp or not getattr(sp, "invoice_payload", None):
        return

    payload = sp.invoice_payload
    booking_id = int(payload.split("_")[1])
    logger.info("Успешная оплата для брони %s", booking_id)

    async with get_session() as session:
        booking = await session.get(Booking, booking_id)
        if not booking:
            await message.answer(t("booking_not_found", locale), reply_markup=get_back_button())
            return
        booking.status = BookingStatus.PAID
        booking.paid_at = datetime.now(UTC)
        booking.cash_hold_expires_at = None
        await session.commit()

    # Сообщение клиенту
    await message.answer(t("payment_success", locale), reply_markup=get_back_button())
    logger.info("Статус брони %s обновлен на PAID", booking_id)

    # Уведомления мастеру и админам (через общий helper)
    bot = getattr(message, "bot", None)
    if bot and booking:
        recipients = [int(getattr(booking, "master_id", 0))]
        recipients += list(getattr(cfg, "ADMIN_IDS", set()))
        await send_booking_notification(bot, booking_id, "paid", recipients)


@client_router.callback_query(BookingActionCB.filter(F.act == "cancel"))
async def cancel_booking(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Позволяет пользователю отменить свою будущую бронь и обновляет список."""
    # Precompute user id so error handlers can reference it if needed
    user_tg_id = cb.from_user.id
    from bot.app.telegram.common.navigation import nav_get_lang
    try:
        # Извлекаем ID брони
        booking_id = int(callback_data.booking_id)
        master_id = None
        b = None

        async with get_session() as session:
            # Проверяем пользователя
            user_row = await session.execute(
                select(User.id).where(User.telegram_id == user_tg_id)
            )
            uid = user_row.scalar_one_or_none()
            if not uid:
                lang = await resolve_locale(state, locale, user_tg_id)
                await cb.answer(t("user_not_found", lang), show_alert=True)
                return

            b = await session.get(Booking, booking_id)
            if not b or b.user_id != uid:
                lang = await resolve_locale(state, locale, user_tg_id)
                await cb.answer(t("booking_not_found", lang), show_alert=True)
                return

            if getattr(b, "status", None) and str(getattr(b.status, "value", b.status)) in {
                "cancelled", "done", "no_show", "expired"
            }:
                lang = await resolve_locale(state, locale, user_tg_id)
                await cb.answer(t("booking_not_active", lang), show_alert=True)
                return

            if b.starts_at <= datetime.now(UTC):
                lang = await resolve_locale(state, locale, user_tg_id)
                await cb.answer(t("cannot_cancel_past", lang), show_alert=True)
                return

            # Enforce cancel lock window
            try:
                try:
                    lock_h = int(getattr(cfg, "SETTINGS", {}).get("client_cancel_lock_hours", 3))
                except (TypeError, ValueError):
                    lock_h = 3
                lock_h = int(lock_h)
            except (TypeError, ValueError):
                lock_h = 3
            if (b.starts_at - datetime.now(UTC)).total_seconds() < lock_h * 3600:
                lang = await resolve_locale(state, locale, user_tg_id)
                logger.info("analytics.cancel_too_close confirm user_id=%s booking_id=%s hours=%s", user_tg_id, booking_id, lock_h)
                await cb.answer(t("cancel_too_close", lang).format(hours=lock_h), show_alert=True)
                return

            master_id = b.master_id
            # Обновляем статус
            await session.execute(
                update(Booking).where(Booking.id == booking_id).values(status=BookingStatus.CANCELLED)
            )
            await session.commit()

            # Acquire bot instance robustly before we update the UI so we still
            # have a reference when sending notifications.
            bot = getattr(cb, "bot", None)
            if not bot:
                _msg = getattr(cb, "message", None)
                bot = getattr(_msg, "bot", None) if _msg is not None else None

            # Обновляем список бронирований
            # Call my_bookings with callback_data=None (handler tolerates None via getattr)
            await my_bookings(cb, None, state, replace_screen=True)

            # Уведомления мастеру и админам (через общий helper)
            logger.info("cancel_booking: post-commit bot=%s master_id=%s booking_id=%s b_present=%s", bool(bot), master_id, booking_id, bool(b))
        # Defensive logging to help diagnose missing notifications
        logger.debug("cancel_booking: bot=%s master_id=%s booking_id=%s", bool(bot), master_id, getattr(b, 'id', None))
        if bot and master_id and b:
            try:
                master_tid = int(master_id)
            except Exception:
                logger.warning("cancel_booking: invalid master_id, skipping notify: %r", master_id)
                master_tid = None
            if master_tid:
                recipients = [master_tid] + [int(x) for x in list(getattr(cfg, "ADMIN_IDS", set()) or []) if str(x).isdigit()]
                # Log at INFO so it appears in typical container logs and we can
                # verify the notification path is executed in production runs.
                logger.info("cancel_booking: notifying recipients=%s for booking=%s", recipients, booking_id)
                try:
                    await send_booking_notification(bot, booking_id, "cancelled", recipients)
                except Exception as notify_err:
                    # Defensive: log any unexpected errors from the notification flow
                    logger.exception("cancel_booking: send_booking_notification raised: %s", notify_err)
            # Success response to the client after commit + notifications
            from bot.app.telegram.common.navigation import nav_get_lang
            lang = await resolve_locale(state, locale, user_tg_id)
            await cb.answer(t("booking_cancelled_success", lang))
            logger.info("Бронь %s отменена пользователем %s", booking_id, user_tg_id)

    except SQLAlchemyError as e:
        logger.exception("Ошибка в cancel_booking (DB): %s", e)
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = await resolve_locale(state, locale, user_tg_id) if user_tg_id else getattr(cfg, "SETTINGS", {}).get("language", "uk")
        await cb.answer(t("cancel_failed", lang), show_alert=True)
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в cancel_booking: %s", e)
    # Unexpected exceptions will be handled by centralized router-level error handlers


from bot.app.telegram.common.callbacks import MasterInfoCB


@client_router.callback_query(MasterInfoCB.filter())
async def master_info_handler(callback: CallbackQuery, callback_data: Any, locale: str) -> None:
    """Отображает информацию о мастере в модальном окне.

    Формат включает:
    - Заголовок профиля
    - Имя/username, ID и контакты
    - Список услуг с длительностью и ценой (если доступны)
    - Рейтинг и количество заказов (если доступны)
    - Ближайшие свободные слоты (до 3)
    - Блок «Про себе» из MasterProfile.bio (ключ about)
    - Последние отзывы из BookingRating
    """
    if not callback.data:
        await callback.answer()
        return

    master_id = int(callback_data.master_id)
    lang = await resolve_locale(None, locale, callback.from_user.id)

    # Prefer the centralized service + formatter
    try:
        from bot.app.services import master_services, shared_services

        data = await master_services.get_master_profile_data(master_id)
        if not data:
            await callback.answer(t("master_not_found", lang), show_alert=True)
            return

        text = shared_services.format_master_profile_text(data, lang)
        await callback.answer(text, show_alert=True)
        logger.info("Информация о мастере %s показана пользователю %s", master_id, callback.from_user.id)
        return
    except (ImportError, SQLAlchemyError, AttributeError, ValueError):
        # Fall back to older inline logic only if service/formatter fail
        logger.exception("Ошибка в master_services.format_master_profile_data или formatter, falling back to inline", exc_info=True)

    # If we get here, try a last-resort inline fetch (keeps previous behavior)
    async with get_session() as session:
        master = await session.scalar(select(Master).where(Master.telegram_id == master_id))
        if not master:
            await callback.answer(t("master_not_found", lang), show_alert=True)
            return

    # Delegate to shared formatter using minimal data
    try:
        minimal = {
            "master": master,
            "master_id": master_id,
        }
        from bot.app.services import shared_services
        text = shared_services.format_master_profile_text(minimal, lang)
        await callback.answer(text, show_alert=True)
        logger.info("Информация о мастере %s показана пользователю %s (fallback)", master_id, callback.from_user.id)
    except (AttributeError, ValueError, SQLAlchemyError):
        logger.exception("Ошибка при получении информации о мастере (fallback)", exc_info=True)
        lang = await resolve_locale(None, locale, callback.from_user.id)
        await callback.answer("⚠️ " + t("error_retry", lang), show_alert=True)

@client_router.message()
async def debug_any_message(message: Message, state: FSMContext, locale: str) -> None:
    """Обрабатывает неподходящие сообщения с возвратом к /start при необходимости.

    Args:
        message: Входящее сообщение от пользователя.
        state: Контекст FSM для управления состоянием.
    """
    user_id = message.from_user.id if message.from_user else 0
    text = message.text or ""
    logger.info("DEBUG_ANY_MESSAGE: text=%r от пользователя %s", text, user_id)
    try:
        # If the user currently has an active FSM state, don't swallow the
        # message here — let stateful handlers (admin/master/etc.) receive it.
        try:
            cur_state = await state.get_state()
        except (RuntimeError, AttributeError, TypeError):
            cur_state = None
        if cur_state:
            logger.debug("debug_any_message skipping handling because user %s has FSM state %r", user_id, cur_state)
            return
        if text.lower().startswith(("/start", "start", "старт")):
            await cmd_start(message, state)
            return
        lang = await resolve_locale(None, locale, user_id)
        await message.answer(t("bot_started_notice", lang))
    except TelegramAPIError as e:
        logger.error("Ошибка Telegram API в debug_any_message для пользователя %s: %s", user_id, e)
    # Unexpected exceptions will be handled by centralized router-level error handlers


@client_router.message()
async def debug_raw(message: Message) -> None:
    """Логирует необработанные сообщения для отладки.

    Args:
        message: Входящее сообщение от пользователя.
    """
    logger.info("RAW MESSAGE: text=%r entities=%s", message.text, message.entities)


__all__ = [
    "client_router",
    "BookingStates",
    "start_booking",
    "select_service",
    "select_master",
    "master_profile_handler", 
]