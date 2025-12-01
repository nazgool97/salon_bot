from __future__ import annotations
import logging
import re
import os
from typing import Optional, Any, Callable, Awaitable

from bot.app.domain.models import Booking, BookingStatus, Master, MasterService, Service, User


from aiogram import F, Router, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, PreCheckoutQuery, LabeledPrice
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select
from aiogram.exceptions import TelegramAPIError
from datetime import datetime, UTC
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.navigation import nav_push, nav_back, nav_reset, nav_replace
from bot.app.telegram.common.callbacks import (
    ServiceSelectCB,
    ServiceToggleCB,
    MasterMultiCB,
    MasterProfileCB,
    MasterServicesCB,
    MastersListCB,
    MasterSelectCB,
    CalendarCB,
    DateCB,
    TimeCB,
    RescheduleCB,
    pack_cb,
)
from bot.app.telegram.common.callbacks import PayCB, BookingActionCB
from bot.app.telegram.common.callbacks import MyBookingsCB
from bot.app.telegram.common.callbacks import MasterMenuCB
from bot.app.telegram.common.callbacks import NavCB
from bot.app.telegram.common.callbacks import ClientMenuCB
from bot.app.core.constants import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_SERVICE_FALLBACK_DURATION,
)
from typing import Protocol

# Structural protocols for dynamic CallbackData subclasses (improves static checking)
class HasMasterId(Protocol):
    master_id: int

class HasServiceId(Protocol):
    service_id: str

class HasBookingId(Protocol):
    booking_id: int

class HasModePage(Protocol):
    mode: str | None
    page: int | None
from bot.app.telegram.common.callbacks import RatingCB
from bot.app.telegram.client.client_keyboards import (
    home_kb,
    get_service_menu,
    get_master_keyboard,
    get_calendar_keyboard,
    get_main_menu,
    get_payment_keyboard,
    build_rating_keyboard,
    build_my_bookings_keyboard,
    get_time_slots_kb,
)
from bot.app.services.client_services import format_bookings_for_ui, format_booking_details_text
from bot.app.services.shared_services import default_language, get_admin_ids, get_contact_info
from bot.app.telegram.client.client_keyboards import get_back_button, get_simple_kb
from bot.app.translations import t
import bot.app.translations as i18n
from bot.app.services.client_services import (
    get_or_create_user,
    calculate_price,
    record_booking_rating,
    get_services_duration_and_price,
    BookingRepo,
    book_slot,
    send_booking_notification,
)
from bot.app.services.admin_services import ServiceRepo
from bot.app.services.admin_services import SettingsRepo
from bot.app.services.shared_services import (
    format_money_cents,
    status_to_emoji,
    is_online_payments_available,
    get_telegram_provider_token,
    tr,
)
import bot.app.services.master_services as master_services
from bot.app.services.master_services import MasterRepo
from bot.app.telegram.common.errors import handle_telegram_error, handle_db_error
# Master FSM state handling has been moved to master router; imports removed

logger = logging.getLogger(__name__)


async def _int_setting(getter: Callable[[], Awaitable[Any]], default: int) -> int:
    try:
        value = await getter()
        return int(value) if value is not None else default
    except Exception:
        return default


async def _slot_duration_default() -> int:
    return await _int_setting(SettingsRepo.get_slot_duration, DEFAULT_SERVICE_FALLBACK_DURATION)


async def _calendar_max_days_default() -> int:
    return await _int_setting(SettingsRepo.get_calendar_max_days_ahead, 365)


def _get_cb_user_id(cb: CallbackQuery) -> Optional[int]:
    """Safely extract telegram user id from a CallbackQuery or return None.

    Some update types (inline queries, channel posts, etc.) may not include
    a `from_user`. Accessing `cb.from_user.id` without a guard raises an
    AttributeError in those scenarios. Use this helper to avoid that.
    """
    try:
        user = getattr(cb, "from_user", None)
        if user is None:
            return None
        return user.id
    except (AttributeError, TypeError):
        return None


def _get_message_user_id(message: Message) -> Optional[int]:
    """Safely extract telegram user id from a Message or return None."""
    try:
        user = getattr(message, "from_user", None)
        if user is None:
            return None
        return user.id
    except (AttributeError, TypeError):
        return None

# Определяем маршрутизатор один раз
client_router = Router(name="client")
# Attach locale middleware used elsewhere
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
client_router.message.middleware(LocaleMiddleware())
client_router.callback_query.middleware(LocaleMiddleware())
# Error handlers now registered only globally in run_bot.py to simplify debugging.


from bot.app.services.shared_services import LOCAL_TZ


class BookingStates(StatesGroup):
    """Состояния FSM для процесса бронирования."""
    waiting_for_service = State()
    waiting_for_master = State()
    waiting_for_date = State()
    reschedule_select_date = State()
    reschedule_select_time = State()


from bot.app.telegram.common.navigation import show_main_client_menu as show_main_menu


# Locale lookups are provided by LocaleMiddleware; handlers receive `locale: str`.


# NOTE: temporary debug handler removed — it intercepted all messages and
# prevented more specific handlers (like /start) from running. Use
# logging in specific handlers or a router-level debug fallback instead.



async def resolve_locale(state: FSMContext | None, locale: str, user_id: Optional[int]) -> str:
    """Resolve locale for handlers: prefer middleware-injected `locale`.

    Previous behavior consulted nav state; middleware now provides the
    canonical locale so this function simply returns the provided `locale`.
    """
    return locale


def with_booking_details(func):
    """Decorator for callback handlers that operate on a single booking.

    The decorator extracts `booking_id` from `callback_data`, resolves the
    locale, builds canonical `BookingDetails` via `build_booking_details` and
    injects it into the wrapped handler as keyword argument `booking_details`.

    If the booking is not found, the decorator responds to the user with
    an appropriate toast/alert and does not call the handler.
    """
    from functools import wraps

    @wraps(func)
    async def _wrapped(cb: CallbackQuery, callback_data: Any, *args, **kwargs):
        booking_id_raw = getattr(callback_data, "booking_id", None)
        if booking_id_raw is None:
            await cb.answer()
            return
        try:
            booking_id = int(booking_id_raw)
        except (TypeError, ValueError):
            await cb.answer()
            return

        user_id = cb.from_user.id if cb.from_user else None

        # Resolve locale similarly to other handlers. We accept injected
        # `locale` either as kwarg or as the third positional argument.
        locale = kwargs.get("locale")
        if locale is None and len(args) >= 1:
            # args[0] corresponds to the positional `locale` when present
            locale = args[0]

        locale_value = locale
        if locale_value is None:
            try:
                locale_value = await SettingsRepo.get_setting("language", default_language())
            except Exception:
                locale_value = default_language()
        try:
            from bot.app.services.client_services import build_booking_details
            lang = await resolve_locale(None, locale_value, user_id)
            bd = await build_booking_details(int(booking_id), user_id=user_id, lang=lang)
            if not bd or not getattr(bd, "booking_id", None):
                await cb.answer(t("booking_not_found", lang), show_alert=True)
                return
        except (ImportError, AttributeError, ValueError, SQLAlchemyError):
            # On unexpected errors from building booking details, surface a generic message and do not call handler
            try:
                lang = await resolve_locale(None, locale_value, user_id)
            except (RuntimeError, AttributeError):
                lang = locale_value
            await cb.answer(t("booking_not_found", lang), show_alert=True)
            return

        # Call the original handler with booking_details and resolved lang injected
        kwargs.setdefault("booking_details", bd)
        kwargs.setdefault("lang", lang)
        await func(cb, callback_data, *args, **kwargs)

    return _wrapped


# Note: forwarding of master menu callbacks to master handlers has been
# removed. The master feature's own router (`master_router`) must handle
# MasterMenuCB(act="menu") callbacks directly. This prevents duplicate
# handlers and order-dependent behavior when client and master routers are
# registered in different orders.


# Note: master edit-note FSM handling moved to master router (master_handlers)

"""Per DRY, handler-level error guards were removed; rely on router.errors handlers."""

@client_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, locale: str) -> None:
    """Обрабатывает команду /start: очищает состояние и показывает главное меню."""
    user_id = message.from_user.id if message.from_user else 0
    logger.debug("Команда /start вызвана для пользователя %s", user_id)
    await state.clear()
    logger.debug("show_main_menu вызвана для user %s", user_id)
    await show_main_menu(message, state, prefer_edit=False)
    # Let router-level error handlers process unexpected exceptions



@client_router.message(F.text.regexp(r"(?i)^(start|старт)(\s|$)"))
async def cmd_start_plaintext(message: Message, state: FSMContext, locale: str) -> None:
    """Обработчик для 'start' или 'старт', набранных как обычный текст."""
    await cmd_start(message, state, locale)


@client_router.message(Command("whoami"))
async def cmd_whoami(message: Message, locale: str) -> None:
    """Показывает Telegram ID пользователя для отладки (например, для ADMIN_IDS)."""
    user_id = message.from_user.id if message.from_user else 0
    logger.info("Команда /whoami вызвана для пользователя %s", user_id)
    lang = locale
    await message.answer(f"{i18n.t('your_telegram_id', lang)} {user_id}")
    logger.info("Telegram ID отправлен для пользователя %s", user_id)
    # Unexpected exceptions will be handled by router-level error handlers


@client_router.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    """Проверка работоспособности: отвечает 'pong'.

    Args:
        message: Входящее сообщение от пользователя.
    """
    logger.info("Команда /ping вызвана для пользователя %s", message.from_user.id if message.from_user else 0)
    await message.answer("pong")
    # Let centralized error handlers handle unexpected exceptions


@client_router.callback_query(ClientMenuCB.filter(F.act == "booking_service"))
async def start_booking(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Инициирует процесс бронирования, показывая меню услуг.

    Args:
        cb: CallbackQuery от inline-кнопки.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            logger.exception("select_service: failed to notify anonymous user about error")
        return
    logger.info("Начало бронирования для пользователя %s", user_id)
    from bot.app.services.client_services import get_filtered_services
    services = await get_filtered_services()
    # Use DTOs returned by get_filtered_services to avoid extra profile queries.
    try:
        from bot.app.domain.models import MasterService
        from bot.app.core.db import get_session
        service_ids = [s.id for s in services]
        # Build profile duration map from DTOs (may be None)
        durations_map: dict[str, int | None] = {s.id: s.duration_minutes for s in services}

        async with get_session() as session:
            # Master-specific durations: collect all non-null, >0 values per service
            ms_rows = (
                await session.execute(
                    select(MasterService.service_id, MasterService.duration_minutes).where(MasterService.service_id.in_(service_ids))
                )
            ).all()
            master_durations: dict[str, list[int]] = {}
            for sid, d in ms_rows:
                try:
                    dv = int(d) if d is not None else 0
                except Exception:
                    continue
                if dv and dv > 0:
                    master_durations.setdefault(str(sid), []).append(dv)
    except Exception:
        durations_map = {}
        master_durations = {}

    # Fallback to global slot duration for services missing explicit profile duration
    default_dur = await _slot_duration_default()
    lang = locale
    unit = t("minutes_short", lang)
    decorated: dict[str, str] = {}
    for s in services:
        sid_str = str(s.id)
        name = s.name
        ms_list = master_durations.get(sid_str) or []
        if ms_list:
            mn = min(ms_list)
            mx = max(ms_list)
            if mn == mx:
                decorated[sid_str] = f"{name} · {mn} {unit}" if unit != "minutes_short" else f"{name} · {mn}m"
            else:
                decorated[sid_str] = f"{name} · от {mn} до {mx} {unit}"
        else:
            # No per-master durations: fall back to profile duration if present; otherwise omit duration
            prof_minutes = durations_map.get(sid_str)
            if isinstance(prof_minutes, int) and prof_minutes > 0:
                decorated[sid_str] = f"{name} · {prof_minutes} {unit}" if unit != "minutes_short" else f"{name} · {prof_minutes}m"
            else:
                # Omit duration suffix when no reliable duration data is available
                decorated[sid_str] = f"{name}"
    kb = await get_service_menu(decorated)
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
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            pass
        return
    service_id = callback_data.service_id
    logger.info("Выбор услуги для пользователя %s, service_id=%s", user_id, service_id)
    # Кэшируем имя услуги для последующего использования
    service_name = await ServiceRepo.get_service_name(service_id)
    await state.update_data(service_id=service_id, service_name=service_name)
    # If a master was chosen first, skip masters list and go directly to calendar for that master
    try:
        data = await state.get_data()
        raw_val = data.get("forced_master_id") if data else None
        forced_mid = None
        if isinstance(raw_val, int):
            forced_mid = raw_val
        elif isinstance(raw_val, str) and raw_val.isdigit():
            forced_mid = int(raw_val)
    except Exception:
        forced_mid = None
    if forced_mid:
        # Clear the forced master to avoid leaking across flows
        try:
            await state.update_data(forced_master_id=None)
        except Exception:
            logger.exception("select_service: failed to clear forced_master_id in state")
        from types import SimpleNamespace
        cb_shim = SimpleNamespace(master_id=forced_mid, service_id=service_id)
        return await select_master(cb, cb_shim, state, locale)
    # Prefetch masters list in handler (keyboards should be UI-only)
    # Use the stable module-level facade to avoid class-binding/import timing issues
    masters_list = await master_services.get_masters_for_service(service_id)
    # Debug: log what MasterRepo returned to help diagnose missing-master issues
    try:
        mids = [getattr(m, "telegram_id", None) for m in masters_list]
    except (AttributeError, TypeError):
        mids = []
    logger.debug("select_service: service_id=%s -> masters_count=%d, masters=%s", service_id, len(masters_list), mids)
    kb = await get_master_keyboard(service_id, masters_list)
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        prompt = t("choose_master", lang)
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_master)
    await cb.answer()
    logger.info("Меню мастеров отправлено для пользователя %s", user_id)


@client_router.callback_query(ClientMenuCB.filter(F.act == "masters_list"))
@client_router.callback_query(MastersListCB.filter())
async def show_masters_catalog(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Показывает список всех мастеров без привязки к услуге."""
    # Resolve target page
    try:
        page = int(getattr(callback_data, "page", 1) or 1)
    except Exception:
        page = 1
    PAGE_SIZE = 20
    try:
        from bot.app.services.master_services import MasterRepo
        total = await MasterRepo.count_masters()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        if page < 1:
            page = 1
        if page > total_pages:
            page = total_pages
        masters = await MasterRepo.get_masters_page(page=page, page_size=PAGE_SIZE)
    except Exception:
        masters = []
        total_pages = 1
        page = 1
    from bot.app.telegram.client.client_keyboards import get_masters_catalog_keyboard
    kb = await get_masters_catalog_keyboard(masters, page=page, total_pages=total_pages)
    lang = locale
    base_title = t("masters_button", lang) if t("masters_button", lang) != "masters_button" else "Наші майстри"
    title = f"{base_title} ({page}/{total_pages})" if total_pages > 1 else base_title
    if cb.message:
        await nav_push(state, title, kb)
        await safe_edit(cb.message, title, reply_markup=kb)
    await cb.answer()


@client_router.callback_query(MasterServicesCB.filter())
async def show_services_for_master(cb: CallbackQuery, callback_data: HasMasterId, state: FSMContext, locale: str) -> None:
    """Показывает услуги, доступные у выбранного мастера, и включает форс-мастера для последующего шага."""
    master_id = int(callback_data.master_id)
    try:
        from bot.app.services.master_services import MasterRepo
        # Use the repo helper that returns effective durations per-service
        rows = await MasterRepo.get_services_with_durations_for_master(master_id)
    except Exception:
        rows = []
    services: dict[str, str] = {str(sid): str(name) for sid, name, _ in rows} if rows else {}
    if not services:
        if cb.message:
            await cb.answer(t("no_services_for_master", locale), show_alert=True)
        return
    # Remember forced master and show service selection limited to this master
    try:
        await state.update_data(forced_master_id=int(master_id))
    except Exception:
        pass
    from bot.app.telegram.client.client_keyboards import get_service_menu
    # Decorate with duration label similar to initial booking flow
    # If we have rows with durations from the repo, use those effective durations
    default_dur = await _slot_duration_default()
    lang = locale
    unit = t("minutes_short", lang)
    decorated = {}
    if rows:
        for sid, nm, eff in rows:
            minutes = int(eff or default_dur)
            decorated[str(sid)] = f"{nm} · {minutes} {unit if unit != 'minutes_short' else 'm'}"
    else:
        # Fallback: try to use ServiceProfile like before
        try:
            from bot.app.domain.models import ServiceProfile
            from bot.app.core.db import get_session
            async with get_session() as session:
                prof_rows = (
                    await session.execute(
                        select(ServiceProfile.service_id, ServiceProfile.duration_minutes).where(ServiceProfile.service_id.in_(list(services.keys())))
                    )
                ).all()  # type: ignore[name-defined]
                durations_map = {str(r[0]): int(r[1] or 0) for r in prof_rows}
        except Exception:
            durations_map = {}
        for sid, nm in services.items():
            minutes = durations_map.get(str(sid)) or default_dur
            decorated[sid] = f"{nm} · {minutes} {unit if unit != 'minutes_short' else 'm'}"
    kb = await get_service_menu(decorated)
    if cb.message:
        prompt = t("choose_service", locale)
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.set_state(BookingStates.waiting_for_service)
    await cb.answer()


@client_router.callback_query(MasterSelectCB.filter())
async def select_master(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор мастера и показывает календарь.

    Args:
        cb: CallbackQuery с данными выбранного мастера.
        callback_data: Данные callback'а с ID мастера и услуги.
        state: Контекст FSM для сохранения состояния.
    """
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            pass
        return
    master_id = callback_data.master_id
    service_id = callback_data.service_id
    logger.info("Выбор мастера для пользователя %s, master_id=%s, service_id=%s", user_id, master_id, service_id)
    # Compute effective duration for the chosen service + master (respect master overrides)
    slot_default = await _slot_duration_default()
    duration = slot_default
    try:
        from bot.app.services.client_services import get_services_duration_and_price
        totals = await get_services_duration_and_price([service_id], online_payment=False, master_id=int(master_id))
        duration = int(totals.get("total_minutes") or slot_default)
    except Exception:
        duration = slot_default

    await state.update_data(master_id=master_id, service_id=service_id)
    # Pre-fetch availability data from services and pass into keyboard (keyboards should NOT query DB)
    try:
        from bot.app.services.client_services import get_available_days_for_month
        from datetime import datetime as _dt
        now = _dt.now()
        year, month = now.year, now.month
        available_days = await get_available_days_for_month(master_id, year, month, service_duration_min=duration)
        sched = await MasterRepo.get_schedule(master_id)
        allowed_weekdays = sorted([int(k) for k, v in (sched or {}).items() if isinstance(v, list) and v]) if sched else []
    except (ImportError, AttributeError, TypeError, ValueError, SQLAlchemyError):
        available_days = set()
        allowed_weekdays = []

    # Resolve max days ahead for calendar via repo (ENV fallback supported)
    max_days = await _calendar_max_days_default()

    # Compute day states in handler to keep keyboards dumb
    try:
        from bot.app.services.client_services import compute_calendar_day_states
        day_states = compute_calendar_day_states(
            year,
            month,
            today=datetime.now(),
            allowed_weekdays=allowed_weekdays or [],
            available_days=available_days or set(),
        )
    except Exception:
        day_states = []

    kb = await get_calendar_keyboard(
        service_id=service_id,
        master_id=master_id,
        year=year,
        month=month,
        service_duration_min=duration,
        user_id=user_id,
        available_days=available_days,
        allowed_weekdays=allowed_weekdays,
        max_days=max_days,
        day_states=day_states,
    )
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        # Build stacked bold legend lines using list-based translation
        from bot.app.translations import tr as _tr
        legend = _tr("calendar_legend_lines", lang)
        if isinstance(legend, list) and legend:
            legend_html = "\n".join([f"<b>{line}</b>" for line in legend])
        else:
            # Fallback to old single-line key
            legend_html = f"<b>{t('calendar_legend', lang)}</b>"
        prompt = f"{t('choose_date', lang)}\n\n{legend_html}"
        await nav_push(state, prompt, kb, lang=lang, parse_mode="HTML")
        await safe_edit(cb.message, prompt, reply_markup=kb, parse_mode="HTML")
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
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            pass
        return
    logger.info("Навигация по календарю для пользователя %s, year=%s, month=%s", user_id, callback_data.year, callback_data.month)
    # Compute effective duration for this calendar view using selected service(s) and master override.
    try:
        sid = str(getattr(callback_data, "service_id", ""))
        if "+" in sid:
            service_ids = [s for s in sid.split("+") if s]
        else:
            service_ids = [sid] if sid else []
        if service_ids:
            try:
                from bot.app.services.client_services import get_services_duration_and_price, get_available_days_for_month
                totals = await get_services_duration_and_price(service_ids, online_payment=False, master_id=int(callback_data.master_id))
                sd = int(totals.get("total_minutes") or DEFAULT_SERVICE_FALLBACK_DURATION)
                # Persist multi_duration_min when multiple services are selected so subsequent flows reuse it
                if "+" in sid:
                    try:
                        await state.update_data(multi_duration_min=sd)
                    except Exception:
                        logger.exception("navigate_calendar: failed to persist multi_duration_min into state")
            except Exception:
                sd = DEFAULT_SERVICE_FALLBACK_DURATION
        else:
            sd = DEFAULT_SERVICE_FALLBACK_DURATION

        available_days = await get_available_days_for_month(callback_data.master_id, callback_data.year, callback_data.month, sd)
        sched = await MasterRepo.get_schedule(callback_data.master_id)
        allowed_weekdays = sorted([int(k) for k, v in (sched or {}).items() if isinstance(v, list) and v]) if sched else []
    except (ImportError, AttributeError, TypeError, ValueError, SQLAlchemyError):
        available_days = set()
        allowed_weekdays = []

    # Compute day states for requested month
    try:
        from bot.app.services.client_services import compute_calendar_day_states
        day_states = compute_calendar_day_states(
            callback_data.year,
            callback_data.month,
            today=datetime.now(),
            allowed_weekdays=allowed_weekdays or [],
            available_days=available_days or set(),
        )
    except Exception:
        logger.exception("navigate_calendar: compute_calendar_day_states failed")
        day_states = []

    kb = await get_calendar_keyboard(
        service_id=callback_data.service_id,
        master_id=callback_data.master_id,
        year=callback_data.year,
        month=callback_data.month,
        service_duration_min=int(sd or DEFAULT_SERVICE_FALLBACK_DURATION),
        user_id=user_id,
        available_days=available_days,
        allowed_weekdays=allowed_weekdays,
        day_states=day_states,
    )
    if cb.message:
        lang = locale
        from bot.app.translations import tr as _tr
        legend = _tr("calendar_legend_lines", lang)
        if isinstance(legend, list) and legend:
            legend_html = "\n".join([f"<b>{line}</b>" for line in legend])
        else:
            legend_html = f"<b>{t('calendar_legend', lang)}</b>"
        prompt = f"{t('choose_date', lang)}\n\n{legend_html}"
        await safe_edit(cb.message, prompt, reply_markup=kb, parse_mode="HTML")
    await cb.answer()
    logger.info("Календарь обновлен для пользователя %s", user_id)
    # Unexpected exceptions are handled by centralized router-level error handlers


@client_router.callback_query(DateCB.filter())
async def select_date(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Обрабатывает выбор даты из календаря и показывает доступные временные слоты."""
    cur_state = await state.get_state()
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            pass
        return
    selected_date = callback_data.date
    logger.info("Выбор даты для пользователя %s, date=%s", user_id, selected_date)
    from bot.app.services.client_services import get_available_time_slots
    slot_default = await _slot_duration_default()
    # Determine slot duration: always compute via get_services_duration_and_price
    # so per-master overrides are respected (single and multi-service flows).
    try:
        sid = str(getattr(callback_data, "service_id", ""))
        # Build list of service ids for the aggregator
        if "+" in sid:
            service_ids = [s for s in sid.split("+") if s]
        else:
            service_ids = [sid] if sid else []
        if service_ids:
            try:
                totals = await get_services_duration_and_price(service_ids, online_payment=False, master_id=int(getattr(callback_data, "master_id", 0)))
                duration = int(totals.get("total_minutes") or slot_default)
            except Exception:
                duration = slot_default
        else:
            duration = slot_default
    except (TypeError, ValueError):
        duration = slot_default

    try:
        base_dt = datetime.fromisoformat(selected_date)
    except ValueError as e:
        logger.error("Некорректный формат даты %s: %s", selected_date, e)
        lang = locale
        await cb.answer(t("invalid_date", lang))
        return

    slots = await get_available_time_slots(base_dt, callback_data.master_id, duration)
    lang = locale
    if not slots:
        if cb.message:
            await safe_edit(cb.message, t("no_time_for_date", lang), reply_markup=get_back_button())
        await cb.answer()
        return

    is_reschedule = bool(cur_state and "reschedule_select_date" in str(cur_state))
    booking_id_value = None
    if is_reschedule:
        data = await state.get_data()
        raw_booking_id = data.get("cres_booking_id") if isinstance(data, dict) else None
        try:
            booking_id_value = int(raw_booking_id) if raw_booking_id is not None else 0
        except (TypeError, ValueError):
            booking_id_value = 0

    try:
        master_id_value = int(getattr(callback_data, "master_id", 0))
    except (TypeError, ValueError):
        master_id_value = 0

    service_id_for_payload = str(getattr(callback_data, "service_id", ""))
    kb = await get_time_slots_kb(
        slots=slots,
        lang=lang,
        action="reschedule" if is_reschedule else "booking",
        date=selected_date,
        service_id=service_id_for_payload,
        master_id=master_id_value,
        booking_id=booking_id_value if is_reschedule else None,
    )

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


async def _fetch_services_and_totals(state: FSMContext | None, selected: set[str] | None = None):
    """Helper: return (services_dict, totals_dict) for the current context.

    - `services_dict` is the mapping service_id->name from get_filtered_services().
    - `totals_dict` is the result of get_services_duration_and_price(...) and
      will gracefully fallback to zeros on error.
    This centralizes forced_master handling so callers don't duplicate logic.
    """
    try:
        from bot.app.services.client_services import get_filtered_services, get_services_duration_and_price
        services_raw = await get_filtered_services()
        # get_filtered_services now returns a list of ServiceDTO; convert to
        # mapping service_id->name for keyboard builders which expect a dict.
        if isinstance(services_raw, list):
            services = {str(s.id): s.name for s in services_raw}
            service_ids = list(services.keys())
        else:
            services = dict(services_raw or {})
            service_ids = list(services.keys())
    except Exception:
        services = {}
        service_ids = []

    totals = {"total_minutes": 0, "total_price_cents": 0, "currency": "UAH"}
    try:
        fd = await state.get_data() if state is not None else {}
        forced_master = fd.get("forced_master_id") if isinstance(fd, dict) else None
        if selected:
            totals = await get_services_duration_and_price(list(selected), master_id=int(forced_master) if forced_master is not None else None)
    except Exception:
        totals = {"total_minutes": 0, "total_price_cents": 0, "currency": "UAH"}

    # Collect per-service master-specific durations (>0) for range display
    ranges: dict[str, tuple[int, int]] = {}
    try:
        if service_ids:
            from bot.app.core.db import get_session
            async with get_session() as session:
                ms_rows = (
                    await session.execute(
                        select(MasterService.service_id, MasterService.duration_minutes).where(MasterService.service_id.in_(service_ids))
                    )
                ).all()
                md: dict[str, list[int]] = {}
                for sid, d in ms_rows:
                    try:
                        dv = int(d) if d is not None else 0
                    except Exception:
                        continue
                    if dv and dv > 0:
                        md.setdefault(str(sid), []).append(dv)
                for sid, lst in md.items():
                    if lst:
                        mn = min(lst)
                        mx = max(lst)
                        ranges[str(sid)] = (mn, mx)
    except Exception:
        ranges = {}

    return services, totals, ranges


@client_router.callback_query(ClientMenuCB.filter(F.act == "services_multi"))
async def services_multi_entry(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Entry point for multi-service selection."""
    data = await state.get_data()
    selected = set(data.get("multi_selected") or [])
    import importlib
    services, totals, ranges = await _fetch_services_and_totals(state, selected)
    # Decorate service names with master-duration ranges when available
    from bot.app.translations import t as _t
    unit = _t("minutes_short", locale)
    decorated = {}
    for sid, name in services.items():
        if sid in (ranges or {}):
            mn, mx = ranges[sid]
            if mn == mx:
                decorated[sid] = f"{name} · {mn} {unit}"
            else:
                decorated[sid] = f"{name} · от {mn} до {mx} {unit}"
        else:
            decorated[sid] = name
    _ck = importlib.import_module("bot.app.telegram.client.client_keyboards")
    kb = await getattr(_ck, "get_service_menu_multi")(selected, decorated)
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    if selected:
        minutes_val = int(totals.get("total_minutes") or 0)
        hours = minutes_val / 60.0
        price_cents_val = int(totals.get("total_price_cents") or 0)
        price = price_cents_val // 100
        currency = totals.get("currency", "UAH")
        prompt = (t("multi_selected_summary", lang) or "Selected: {count} services ({hours} h, ~{price} {currency})").format(
            count=len(selected), hours=f"{hours:.1f}".rstrip("0").rstrip("."), price=price, currency=currency
        )
    else:
        prompt = t("choose_service", lang)
    if cb.message:
        await nav_push(state, prompt, kb)
        await safe_edit(cb.message, prompt, reply_markup=kb)
    await state.update_data(multi_selected=list(selected), current_screen="multi_select")
    await cb.answer()


@client_router.callback_query(ServiceToggleCB.filter())
async def svc_toggle(cb: CallbackQuery, callback_data: HasServiceId, state: FSMContext, locale: str) -> None:
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
    services, totals, ranges = await _fetch_services_and_totals(state, selected)
    from bot.app.translations import t as _t
    unit = _t("minutes_short", locale)
    decorated = {}
    for sid, name in services.items():
        if sid in (ranges or {}):
            mn, mx = ranges[sid]
            if mn == mx:
                decorated[sid] = f"{name} · {mn} {unit}"
            else:
                decorated[sid] = f"{name} · от {mn} до {mx} {unit}"
        else:
            decorated[sid] = name
    _ck = importlib.import_module("bot.app.telegram.client.client_keyboards")
    kb = await getattr(_ck, "get_service_menu_multi")(selected, decorated)
    if cb.message:
        from bot.app.telegram.common.navigation import nav_get_lang
        lang = (await nav_get_lang(state)) or locale
        # Use totals provided by helper for dynamic header
        if selected:
            minutes_val = int(totals.get("total_minutes") or 0)
            hours = minutes_val / 60.0
            price_cents_val = int(totals.get("total_price_cents") or 0)
            price = price_cents_val // 100
            currency = totals.get("currency", "UAH")
            header = (t("multi_selected_summary", lang) or "Selected: {count} services ({hours} h, ~{price} {currency})").format(
                count=len(selected), hours=f"{hours:.1f}".rstrip("0").rstrip("."), price=price, currency=currency
            )
        else:
            header = t("choose_service", lang)
        await safe_edit(cb.message, header, reply_markup=kb)
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
    # Find masters who offer all selected services via service facade
    masters = await MasterRepo.find_masters_for_services(selected)
    # If no master supports all selected services, provide explicit feedback
    if not masters:
        msg_text = t("no_masters_for_combination", lang)
        # Re-open the multi-service selection screen so user can tweak choices
        try:
                from bot.app.services.client_services import get_filtered_services
                services_raw = await get_filtered_services()
                # normalize to dict[id->name] for keyboard builder
                if isinstance(services_raw, list):
                    services = {str(s.id): s.name for s in services_raw}
                else:
                    services = dict(services_raw or {})
                from bot.app.telegram.client.client_keyboards import get_service_menu_multi
                kb = await get_service_menu_multi(set(selected), services)
        except (ImportError, AttributeError, TypeError, SQLAlchemyError):
            kb = None

        # Try to replace nav stack to the service chooser and edit message with helpful text
        try:
            if cb.message:
                prompt = f"{t('choose_service', lang)}\n\n{msg_text}"
                try:
                    await nav_replace(state, prompt, kb)
                except (TelegramAPIError, RuntimeError):
                    logger.exception("select_master: nav_replace failed")
                try:
                    await safe_edit(cb.message, prompt, reply_markup=kb)
                except (TelegramAPIError, RuntimeError):
                    # Last-resort: send a plain message
                    try:
                        await cb.message.answer(msg_text)
                    except (TelegramAPIError, RuntimeError):
                        logger.exception("select_master: fallback message send failed")
        except (ImportError, AttributeError, TypeError):
            # If navigation helpers/imports fail, fallback to best-effort message
            try:
                if cb.message:
                    await cb.message.answer(msg_text)
            except (TelegramAPIError, RuntimeError):
                logger.exception("select_master: fallback answer failed")
        return
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import MasterMultiCB
    for mid, name in masters:
        b.button(text=str(name or mid), callback_data=pack_cb(MasterMultiCB, master_id=int(mid)))
    b.adjust(2)
    b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    # Precompute total duration for later calendar
    try:
        agg = await get_services_duration_and_price(selected, online_payment=False)
        total_min = int(agg.get("total_minutes") or DEFAULT_SERVICE_FALLBACK_DURATION)
    except (SQLAlchemyError, TypeError, ValueError):
        total_min = 60
    await state.update_data(multi_selected=selected, multi_duration_min=total_min)
    if cb.message:
        await nav_push(state, t("choose_master", lang), b.as_markup())
        await safe_edit(cb.message, t("choose_master", lang), reply_markup=b.as_markup())
    await state.set_state(BookingStates.waiting_for_master)
    await cb.answer()


@client_router.callback_query(MasterMultiCB.filter())
async def master_multi(cb: CallbackQuery, callback_data: HasMasterId, state: FSMContext, locale: str) -> None:
    """Proceed to calendar for multi-service booking with combined duration."""
    master_id = int(callback_data.master_id)
    data = await state.get_data()
    selected = list(data.get("multi_selected") or [])
    if not selected:
        await cb.answer(i18n.t("invalid_data", locale), show_alert=True)
        return
    # Recompute total minutes using selected master so per-master overrides are applied
    try:
        from bot.app.services.client_services import get_services_duration_and_price
        totals = await get_services_duration_and_price(selected, online_payment=False, master_id=master_id)
        total_min = int(totals.get("total_minutes") or DEFAULT_SERVICE_FALLBACK_DURATION)
        # update state with accurate multi duration
        try:
            await state.update_data(multi_duration_min=total_min)
        except Exception:
            pass
    except Exception:
        total_min = int(data.get("multi_duration_min") or DEFAULT_SERVICE_FALLBACK_DURATION)
    service_id = "+".join(selected)
    try:
        from bot.app.services.client_services import get_available_days_for_month
        from datetime import datetime as _dt
        now = _dt.now()
        year, month = now.year, now.month
        available_days = await get_available_days_for_month(master_id, year, month, service_duration_min=total_min)
        sched = await MasterRepo.get_schedule(master_id)
        allowed_weekdays = sorted([int(k) for k, v in (sched or {}).items() if isinstance(v, list) and v]) if sched else []
    except (ImportError, AttributeError, TypeError, ValueError, SQLAlchemyError):
        available_days = set()
        allowed_weekdays = []

    user_id = _get_cb_user_id(cb)
    # Compute day states for current month
    try:
        from bot.app.services.client_services import compute_calendar_day_states
        day_states = compute_calendar_day_states(
            year,
            month,
            today=datetime.now(),
            allowed_weekdays=allowed_weekdays or [],
            available_days=available_days or set(),
        )
    except Exception:
        day_states = []

    kb = await get_calendar_keyboard(
        service_id=service_id,
        master_id=master_id,
        year=year,
        month=month,
        service_duration_min=total_min,
        user_id=user_id,
        available_days=available_days,
        allowed_weekdays=allowed_weekdays,
        day_states=day_states,
    )
    lang = locale
    if cb.message:
        from bot.app.translations import tr as _tr
        legend = _tr("calendar_legend_lines", lang)
        if isinstance(legend, list) and legend:
            legend_html = "\n".join([f"<b>{line}</b>" for line in legend])
        else:
            legend_html = f"<b>{t('calendar_legend', lang)}</b>"
        prompt = f"{t('choose_date', lang)}\n\n{legend_html}"
        await nav_push(state, prompt, kb, lang=lang, parse_mode="HTML")
        await safe_edit(cb.message, prompt, reply_markup=kb, parse_mode="HTML")
    await state.set_state(BookingStates.waiting_for_date)
    await state.update_data(master_id=master_id)
    await cb.answer()


@client_router.callback_query(TimeCB.filter())
async def select_time_and_create_booking(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Create booking immediately when a time is selected (single and multi-service).

    This replaces the previous two-step flow (time -> confirm -> create). Selecting a
    time now creates a RESERVED booking and immediately shows payment methods.
    """
    user_id = _get_cb_user_id(cb)
    if user_id is None:
        try:
            if cb.message:
                await safe_edit(cb.message, text=t("error_retry", locale))
            else:
                await cb.answer(t("error_retry", locale))
        except (TelegramAPIError, RuntimeError):
            pass
        return
    # Delegate booking creation to service layer (book_slot)
    service_id = callback_data.service_id
    master_id = int(callback_data.master_id)
    date_str = callback_data.date
    time_compact = callback_data.time
    result = await book_slot(user_id, master_id, service_id, date_str, time_compact, locale)
    if not result.get("ok"):
        code = result.get("error") or "booking_failed"
        lang = locale
        # Map known codes to translation keys with safe fallbacks
        if code == "client_already_has_booking_at_this_time":
            await cb.answer(t("you_already_have_booking_at_this_time", lang) or "У вас уже есть запись на это время", show_alert=True)
        elif code == "slot_unavailable":
            await cb.answer(t("slot_unavailable", lang) or "Слот недоступен, выберите другое время", show_alert=True)
        elif code == "invalid_data":
            await cb.answer(t("invalid_data", lang) or "Неверные данные", show_alert=True)
        else:
            await cb.answer(t("booking_failed", lang) or "Не удалось создать запись. Попробуйте снова.", show_alert=True)
        return
    booking = result["booking"]
    service_name = result.get("service_name") or t("service_label", locale)
    master_name = result.get("master_name") or t("master_label", locale)
    formatted_date = result.get("date")
    header, kb = await get_payment_keyboard(booking, service_name, master_name, cb.from_user.id, date=formatted_date)
    if cb.message:
        await nav_replace(state, header, kb)
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


@client_router.callback_query(PayCB.filter(F.action == "prep_cash"))
@with_booking_details
async def pay_cash_prepare(cb: CallbackQuery, callback_data: HasBookingId, locale: str, lang: str, booking_details=None) -> None:
    """Shows a confirmation screen before confirming cash payment (booking confirmation).

    Uses the canonical BookingDetails builder + pure formatter for consistent output.
    """
    booking_id = int(callback_data.booking_id)
    details_obj = booking_details
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
        await cb.answer()
        return
    # Attempt to record rating; service will validate booking status
    res = await record_booking_rating(booking_id, rating)
    lang = locale
    status = res.get("status") if isinstance(res, dict) else None
    # Acknowledge with a relevant message instead of a blind thank-you
    if status == "ok":
        await cb.answer(t("thanks_for_feedback", lang))
    elif status == "already":
        await cb.answer(t("already_rated", lang), show_alert=True)
    elif status == "not_done":
        await cb.answer(t("rating_only_after_done", lang), show_alert=True)
    elif status == "invalid":
        await cb.answer(t("rating_invalid_value", lang), show_alert=True)
    elif status == "not_found":
        await cb.answer(t("rating_not_found", lang), show_alert=True)
    else:
        await cb.answer(t("rating_save_failed", lang), show_alert=True)


@client_router.callback_query(PayCB.filter(F.action == "conf_cash"))
async def pay_cash(cb: CallbackQuery, callback_data: HasBookingId, state: FSMContext, locale: str) -> None:
    """
    Обрабатывает выбор оплаты наличными:
    - обновляет статус брони на CONFIRMED,
    - уведомляет клиента, мастера и админов.
    """
    booking_id = int(callback_data.booking_id)
    # Resolve language early for all messages
    lang = await resolve_locale(state, locale, cb.from_user.id)

    # Analytics: confirm cash clicked
    logger.info("analytics.cash_confirm_click user_id=%s booking_id=%s", cb.from_user.id, booking_id)

    # Обновляем бронь в базе через репозиторий
    b = await BookingRepo.get(booking_id)
    if not b:
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return
    master_id = int(getattr(b, 'master_id', 0) or 0)
    await BookingRepo.confirm_cash(booking_id)

    # Update client's bookings view (prefer back to bookings list rather than jumping to main menu)
    try:
        # Use my_bookings to refresh the bookings list and preserve navigation behavior
        await my_bookings(cb, None, state, locale=locale, replace_screen=True)
    except (TelegramAPIError, RuntimeError):
        # Fallback: show a simple confirmation and main menu keyboard
        if cb.message:
            lang = locale
            try:
                await safe_edit(cb.message, t("cash_confirmed_message", lang), reply_markup=home_kb())
            except (TelegramAPIError, RuntimeError):
                pass
        try:
            await cb.answer()
        except (TelegramAPIError, RuntimeError):
            pass
    logger.info("Бронь %s подтверждена как оплата наличными", booking_id)

    # Unified notifications via shared helper
    bot = getattr(cb, "bot", None)
    if bot and b:
        recipients = [int(getattr(b, "master_id", 0))] + get_admin_ids()
        await send_booking_notification(bot, booking_id, "cash_confirmed", recipients)



## Deprecated: old global_back handler removed; use NavCB(act='back'|'root'|'role_root')



## Удален специализированный back_to_masters: используется глобальная навигация


@client_router.callback_query(MyBookingsCB.filter())
async def my_bookings(cb: CallbackQuery, callback_data: HasModePage, state: FSMContext, locale: str | None = None, replace_screen: bool = False) -> None:
    """ Отображает активные и предстоящие брони пользователя. """
    user_id = cb.from_user.id
    logger.info("Запрос списка бронирований для пользователя %s", user_id)
    await cb.answer("Завантаження...", show_alert=False)
    user = await get_or_create_user(
        user_id, cb.from_user.full_name if cb.from_user else str(user_id)
    )
    now = datetime.now(UTC)

    # Определяем новый фильтр и страницу из callback_data
    mode_val = getattr(callback_data, "mode", None)
    new_filter = mode_val if mode_val in ("upcoming", "completed") else "upcoming"
    # Extract page number; CallbackData may carry page=None so coerce to 1 (1-based pages)
    page_raw = getattr(callback_data, "page", None)
    try:
        page = int(page_raw) if page_raw is not None else 1
    except (TypeError, ValueError):
        page = 1
    # Debug: log incoming callback payload to help diagnose tab presses
    try:
        logger.debug("my_bookings callback received: mode=%s page_raw=%r resolved_page=%s", mode_val, page_raw, page)
    except (AttributeError, TypeError, UnicodeEncodeError):
        pass
    if page < 1:
        page = 1  # Защита от отрицательных/нулевых страниц

    # Always replace the current screen to avoid jerky history and ensure Back is predictable
    effective_replace = True

    # Сохраняем новый фильтр, страницу и текущий экран in state
    await state.update_data(my_bookings_filter=new_filter, my_bookings_page=page, current_screen="my_bookings")
    # Ensure no preferred_role remains from admin/master flows so role_root
    # will return to client root when pressed from client bookings screens.
    try:
        await state.update_data(preferred_role=None)
    except (RuntimeError, AttributeError):
        pass

    filter_mode = new_filter

    # Delegate data retrieval to the client services implementation
    # Direct BookingRepo access (wrapper removed)
    from bot.app.services.client_services import BookingRepo
    rows, meta = await BookingRepo.get_paginated_list(
        user_id=user.id,
        mode=filter_mode,
        page=page,
        page_size=DEFAULT_PAGE_SIZE,
    )
    upcoming_count = int(meta.get("upcoming_count", 0) or 0)
    completed_count = int(meta.get("completed_count", 0) or 0)
    total_count = completed_count if filter_mode == "completed" else upcoming_count

    try:
        from bot.app.translations import t
    except ImportError:
        t = None  # type: ignore
    lang = await resolve_locale(state, locale or "", user.id)
    kb = None

    # Delegate rendering to the unified shared renderer so client/master/admin
    # flows share the same rendering contract. The shared renderer will call
    # BookingRepo.get_paginated_list internally and produce text + keyboard.
    from bot.app.telegram.client import client_keyboards as _ck

    # Format rows for UI (services now own formatting); keyboards remain UI-only
    try:
        formatted_rows = await format_bookings_for_ui(rows, lang)
        # Header text built here (handler orchestration)
        from bot.app.translations import tr as _tr
        m = meta or {}
        if filter_mode == "upcoming":
            title_key = "upcoming_bookings_title"
        else:
            title_key = "completed_bookings_title"
        title = _tr(title_key, lang=lang)
        total_all = int(m.get("total", 0))
        try:
            page_num = int(m.get("page", 1) or 1)
        except (TypeError, ValueError):
            page_num = 1
        try:
            total_pages = int(m.get("total_pages", 1) or 1)
        except (TypeError, ValueError):
            total_pages = 1
        if total_pages and total_pages > 1:
            page_label = f"{_tr('page_short', lang=lang)} {page_num}/{total_pages}"
            text = f"<b>{title} ({total_all}) ({page_label})</b>"
        else:
            text = f"<b>{title} ({total_all})</b>"

        kb = await build_my_bookings_keyboard(formatted_rows, upcoming_count, completed_count, filter_mode, page, lang, items_per_page=DEFAULT_PAGE_SIZE, cancelled_count=meta.get('cancelled_count', 0) if meta else 0, noshow_count=meta.get('noshow_count', 0) if meta else 0, total_pages=meta.get('total_pages') if meta else 1)
    except Exception as e:
        logger.exception("Failed to prepare bookings list UI for user %s: %s", user_id, e)
        text = i18n.t("no_bookings", lang)
        kb = get_back_button()

    if cb.message:
        from aiogram.types import InlineKeyboardMarkup
        final_kb = kb if isinstance(kb, InlineKeyboardMarkup) else get_back_button()
        await nav_replace(state, text, final_kb)
        await safe_edit(cb.message, text, reply_markup=final_kb)
        # Ограничиваем длину текста для Telegram
        text = text[:4096]

    await cb.answer()
    logger.info(
        "Список бронирований отображен для пользователя %s (количество=%d)",
        user_id, len(rows)
    )

@client_router.callback_query(BookingActionCB.filter(F.act == "details"))
async def client_booking_details(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Показывает детали записи клиента с расширенной информацией и действиями."""
    booking_id = int(callback_data.booking_id)

    # Resolve locale from nav/state or provided middleware locale
    lang = await resolve_locale(state, locale, cb.from_user.id)

    try:
        from bot.app.services.client_services import build_booking_details
        from bot.app.telegram.client.client_keyboards import (
            build_booking_card_kb,
        )

        bd = await build_booking_details(booking_id, user_id=None, lang=lang)
        text = format_booking_details_text(bd, lang, role="client")
        markup = build_booking_card_kb(bd, booking_id, role="client", lang=lang)
    except (ImportError, AttributeError, ValueError, SQLAlchemyError):
        # Fallback: best-effort not-found response without DB delegator
        from bot.app.translations import tr
        text = tr("booking_not_found", lang=lang) if tr("booking_not_found", lang=lang) else "—"
        from bot.app.telegram.client.client_keyboards import get_back_button
        markup = get_back_button()

    # Update navigation and UI
    if cb.message:
        await nav_push(state, text, markup, lang=lang)
        await safe_edit(cb.message, text, reply_markup=markup)

    await cb.answer()

@client_router.callback_query(BookingActionCB.filter(F.act == "cancel_confirm"))
async def cancel_booking_confirm(cb: CallbackQuery, callback_data, locale: str) -> None:
    """Ask for confirmation before cancelling a booking."""
    booking_id = int(callback_data.booking_id)
    # Enforce client cancellation lock window before even showing confirm
    lang = await resolve_locale(None, locale, cb.from_user.id)
    lock_h = await _int_setting(SettingsRepo.get_client_cancel_lock_hours, 3)
    b = await BookingRepo.get(booking_id)
    # Disallow showing confirmation for terminal bookings
    terminal_statuses = {"cancelled", "done", "no_show", "expired"}
    if b and str(getattr(getattr(b, 'status', ''), 'value', getattr(b, 'status', ''))).lower() in terminal_statuses:
        await cb.answer(t("booking_not_active", lang), show_alert=True)
        return
    if b and (b.starts_at - datetime.now(UTC)).total_seconds() < lock_h * 3600:
        logger.info("analytics.cancel_too_close preconfirm user_id=%s booking_id=%s hours=%s", cb.from_user.id, booking_id, lock_h)
        await cb.answer(t("cancel_too_close", lang).format(hours=lock_h), show_alert=True)
        return
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    b = InlineKeyboardBuilder()
    cancel_payload = pack_cb(BookingActionCB, act="cancel", booking_id=int(booking_id))
    b.button(text=t("confirm", lang), callback_data=cancel_payload)
    b.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    b.adjust(2)
    if cb.message:
        await safe_edit(cb.message, t("cancel_confirm_question", lang), reply_markup=b.as_markup())
    await cb.answer()


@client_router.callback_query(BookingActionCB.filter(F.act == "cancel_reservation"))
async def cancel_reservation_and_go_back(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Cancel a freshly created RESERVED booking (used by payment 'Back') and go back in nav stack.

    This mirrors the normal cancellation flow but is lightweight and intended
    to be used when the user abandons the payment screen.
    """
    booking_id = int(callback_data.booking_id)
    user_tg_id = cb.from_user.id

    # Ensure user exists and actually owns this booking
    user = await get_or_create_user(user_tg_id, cb.from_user.full_name if cb.from_user else str(user_tg_id))
    b = await BookingRepo.ensure_owner(int(user.id), booking_id)
    if not b:
        lang = await resolve_locale(state, locale, user_tg_id)
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return

    # Disallow cancelling terminal bookings
    if getattr(b, "status", None) and str(getattr(getattr(b, 'status', ''), 'value', getattr(b, 'status', ''))).lower() in {"cancelled", "done", "no_show", "expired"}:
        lang = await resolve_locale(state, locale, user_tg_id)
        await cb.answer(t("booking_not_active", lang), show_alert=True)
        return

    # Perform cancellation via service/repo
    await BookingRepo.set_cancelled(booking_id)

    # Try to navigate back to previous screen; if none, show main menu
    try:
        from bot.app.telegram.common.navigation import show_main_client_menu
        text, markup, popped = await nav_back(state)
        if popped and cb.message:
            try:
                # restore parse_mode stored by nav_push/nav_back
                data = await state.get_data()
                pm = data.get("current_parse_mode")
                # Avoid using a kwargs dict and unpacking it into safe_edit.
                # Some static analyzers may mis-assign unpacked values to the
                # keyword-only parameter `fallback_text`. Passing explicit
                # keyword args keeps types clear.
                if pm:
                    await safe_edit(cb.message, text or "", reply_markup=markup, parse_mode=pm)
                else:
                    await safe_edit(cb.message, text or "", reply_markup=markup)
            except (TelegramAPIError, RuntimeError):
                # If editing the message fails (network/TG error), fall back
                await show_main_client_menu(cb, state)
        else:
            await show_main_client_menu(cb, state)
    except (ImportError, AttributeError, TypeError):
        # Best-effort fallback for import or nav errors
        try:
            from bot.app.telegram.common.navigation import show_main_client_menu
            await show_main_client_menu(cb, state)
        except (TelegramAPIError, RuntimeError):
            # Give up silently on bot/network errors
            pass

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
    # Fetch data via master service and use pure formatter (now in client_keyboards)
    from bot.app.services import master_services

    data = await master_services.get_master_profile_data(master_id) or {}
    # Attach normalized schedule so profile formatter can render working windows.
    try:
        sched = await master_services.MasterRepo.get_schedule(master_id)
    except (AttributeError, SQLAlchemyError):
        sched = {}
    data["schedule"] = sched or {}
    from bot.app.telegram.master.master_keyboards import format_master_profile_text as _fmt
    text = _fmt(data, lang, with_title=False)

    # Создаем клавиатуру с кнопкой "Записаться" и "Назад"
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from typing import cast, Any
    builder = InlineKeyboardBuilder()
    book_text = t("book_button", lang) if t("book_button", lang) != "book_button" else "Записатися"
    # If service_id is empty (master-first flow), go to services filtered by master
    if not service_id:
        builder.button(
            text=book_text,
            callback_data=pack_cb(MasterServicesCB, master_id=master_id),
        )
    else:
        builder.button(
            text=book_text,
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
    contacts_map = await get_contact_info()
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
    # Unexpected exceptions are handled by centralized router-level error handlers

@client_router.callback_query(RescheduleCB.filter(F.action == "start"))
async def client_reschedule_start(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Start client reschedule: show calendar for the booking's master/service."""
    from bot.app.telegram.common.navigation import nav_get_lang
    lang = (await nav_get_lang(state)) or locale
    booking_id = int(callback_data.booking_id)
    from bot.app.services.client_services import can_client_reschedule, BookingRepo
    ok, reason = await can_client_reschedule(booking_id, cb.from_user.id)
    if not ok:
        await cb.answer(t(reason or "error_retry", lang), show_alert=True)
        return
    b = await BookingRepo.get(booking_id)
    if not b:
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return
    master_id = int(getattr(b, 'master_id', 0) or 0)
    service_id = getattr(b, 'service_id', None)
    service_id_str = str(service_id or "")
    slot_default = await _slot_duration_default()
    # Prepare calendar: compute effective duration using booking's services and master override
    try:
        from bot.app.services.client_services import get_services_duration_and_price, get_available_days_for_month
        from bot.app.core.db import get_session
        from bot.app.domain.models import BookingItem
        # Gather service ids from BookingItem rows (fallback to b.service_id)
        async with get_session() as session:
            bi_rows = (await session.execute(select(BookingItem.service_id).where(BookingItem.booking_id == booking_id))).all()
        if bi_rows:
            service_ids = [str(r[0]) for r in bi_rows]
        else:
            service_ids = [str(service_id_str)] if service_id_str else []
        if service_ids:
            try:
                totals = await get_services_duration_and_price(service_ids, online_payment=False, master_id=master_id)
                duration = int(totals.get("total_minutes") or slot_default)
            except Exception:
                duration = slot_default
        else:
            duration = slot_default

        from datetime import datetime as _dt
        now = _dt.now()
        year, month = now.year, now.month
        available_days = await get_available_days_for_month(master_id, year, month, service_duration_min=duration)
        sched = await MasterRepo.get_schedule(master_id)
        allowed_weekdays = sorted([int(k) for k, v in (sched or {}).items() if isinstance(v, list) and v]) if sched else []
    except (ImportError, AttributeError, TypeError, ValueError, SQLAlchemyError):
        available_days = set()
        allowed_weekdays = []

    # Compute day states for current month
    try:
        from bot.app.services.client_services import compute_calendar_day_states
        day_states = compute_calendar_day_states(
            year,
            month,
            today=datetime.now(),
            allowed_weekdays=allowed_weekdays or [],
            available_days=available_days or set(),
        )
    except Exception:
        day_states = []

    kb = await get_calendar_keyboard(
        service_id=service_id_str,
        master_id=master_id,
        year=year,
        month=month,
        service_duration_min=duration,
        user_id=cb.from_user.id,
        available_days=available_days,
        allowed_weekdays=allowed_weekdays,
        day_states=day_states,
    )
    if cb.message:
        await nav_push(state, f"{t('reschedule_pick_date', lang)}", kb, lang=lang)
        await safe_edit(cb.message, f"{t('reschedule_pick_date', lang)}", reply_markup=kb)
    await state.update_data(cres_booking_id=booking_id, service_id=service_id_str, master_id=master_id)
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
    lock_h = await _int_setting(SettingsRepo.get_client_reschedule_lock_hours, 3)
    if (local_dt.astimezone(_UTC) - datetime.now(_UTC)).total_seconds() < lock_h * 3600:
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

    # Ownership check and update via repository
    user = await get_or_create_user(cb.from_user.id, cb.from_user.full_name or str(cb.from_user.id))
    b = await BookingRepo.ensure_owner(int(user.id), booking_id)
    if not b:
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return
    await BookingRepo.reschedule(booking_id, new_dt_utc)

    # Notify master and admins (unified helper)
    bot = getattr(cb, "bot", None)
    if bot and b:
        recipients = [int(getattr(b, "master_id", 0))] + get_admin_ids()
        await send_booking_notification(bot, booking_id, "rescheduled_by_client", recipients)

        # After reschedule, navigate back to the bookings list (consistent with cancel flow).
        # Show the updated booking list immediately.
        await my_bookings(cb, callback_data, state, locale, replace_screen=True)
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
    if not await is_online_payments_available():
        await cb.answer(t("online_payments_unavailable", locale))
        return

    # Fetch from repositories
    booking = await BookingRepo.get(booking_id)
    if not booking:
        await cb.answer(t("booking_not_found", locale))
        return

    service_name = await BookingRepo.get_booking_service_names(booking_id)
    if not service_name:
        service_name = t("service_label", locale)
    master_name = await MasterRepo.get_master_name(int(getattr(booking, 'master_id', 0))) or t("master_label", locale)

    # Проверка цены (fallback to Service.price_cents if snapshots are missing)
    price_cents = getattr(booking, 'final_price_cents', None) or getattr(booking, 'original_price_cents', None)
    svc = await ServiceRepo.get(str(getattr(booking, 'service_id', '')))
    if not price_cents and svc and getattr(svc, 'price_cents', None):
        price_cents = int(getattr(svc, 'price_cents', 0) or 0)
    if not price_cents or int(price_cents) <= 0:
        await cb.answer(t("invoice_missing_price", locale), show_alert=True)
        return

    currency = getattr(svc, 'currency', None) or "UAH"

    prices = [LabeledPrice(label=f"{service_name} у {master_name}", amount=price_cents)]

    provider_token = (await get_telegram_provider_token()) or ""
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

    # Обновляем статус брони через репозиторий
    await BookingRepo.set_pending_payment(booking_id)

    await cb.answer()
    logger.info("Счет для онлайн-оплаты отправлен для брони %s", booking_id)


@client_router.callback_query(PayCB.filter(F.action == "prep_online"))
@with_booking_details
async def pay_online_prepare(cb: CallbackQuery, callback_data, locale: str, lang: str, booking_details=None) -> None:
    """Shows a confirmation screen before issuing the Telegram invoice."""
    booking_id = int(callback_data.booking_id)
    bd = booking_details
    if bd is None:
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
@with_booking_details
async def pay_back_methods(cb: CallbackQuery, callback_data, locale: str, lang: str, booking_details=None) -> None:
    """Return to the payment method selection for a booking."""
    booking_id = int(callback_data.booking_id)
    bd = booking_details
    if bd is None:
        await cb.answer(t("booking_not_found", lang), show_alert=True)
        return
    # Delegate to the shared keyboard builder; pass canonical booking id to
    # ensure payment callbacks are built with the correct booking identifier.
    # bd.raw may be a dict lacking an `id` attribute which would cause the
    # payment keyboard to produce callbacks with booking_id=0. Use the
    # normalized booking id instead.
    booking_payload = int(getattr(bd, "booking_id", 0) or 0)
    header, kb = await get_payment_keyboard(booking_payload, bd.service_name or t("service_label", lang), bd.master_name, cb.from_user.id, date=bd.date_str)
    if cb.message:
        await safe_edit(cb.message, header, reply_markup=kb)
    await cb.answer()


@client_router.pre_checkout_query()
async def pre_checkout_query(pre_checkout_query: PreCheckoutQuery) -> None:
    """Handle Telegram PreCheckoutQuery by acknowledging it.

    This is a minimal safe handler: for full payment handling the
    successful_payment update should be used. We acknowledge the
    pre-checkout query to satisfy Telegram API requirements and log
    unexpected failures.
    """
    try:
        await pre_checkout_query.answer(ok=True)
    except Exception:
        logger.exception("pre_checkout_query: failed to answer pre-checkout query")


@client_router.callback_query(BookingActionCB.filter(F.act == "cancel"))
async def cancel_booking(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
    """Позволяет пользователю отменить свою будущую бронь и обновляет список."""
    user_tg_id = cb.from_user.id
    booking_id = int(callback_data.booking_id)
    # Centralized cancel logic
    from bot.app.services.client_services import cancel_client_booking
    bot = getattr(cb, "bot", None) or getattr(getattr(cb, "message", None), "bot", None)
    ok, msg_key, params = await cancel_client_booking(booking_id, user_tg_id, bot=bot)
    lang = await resolve_locale(state, locale, user_tg_id)
    if not ok:
        # Show reason in alert
        try:
            await cb.answer(t(msg_key, lang).format(**params), show_alert=True)
        except Exception:
            await cb.answer(t("error_retry", lang), show_alert=True)
        return
    # Refresh UI list and show success toast
    await my_bookings(cb, None, state, replace_screen=True)
    try:
        await cb.answer(t(msg_key, lang))
    except Exception:
        pass

    # Unexpected exceptions will be handled by centralized router-level error handlers


from bot.app.telegram.common.callbacks import MasterInfoCB


@client_router.callback_query(MasterInfoCB.filter())
async def master_info_handler(callback: CallbackQuery, callback_data: Any, locale: str) -> None:
    """Show concise master information in a modal alert to the client."""
    if not callback.data:
        await callback.answer()
        return

    master_id = int(callback_data.master_id)
    lang = await resolve_locale(None, locale, callback.from_user.id)

    # Prefer the centralized service + formatter
    try:
        from bot.app.services import master_services

        data = await master_services.get_master_profile_data(master_id)
        if not data:
            await callback.answer(t("master_not_found", lang), show_alert=True)
            return

        from bot.app.telegram.master.master_keyboards import format_master_profile_text as _fmt
        text = _fmt(data, lang)
        await callback.answer(text, show_alert=True)
        logger.info("Информация о мастере %s показана пользователю %s", master_id, callback.from_user.id)
        return
    except (ImportError, AttributeError, ValueError):
        # Fall back to older inline logic only if service/formatter fail
        logger.exception("Ошибка в master_services.format_master_profile_data или formatter, falling back to inline", exc_info=True)

    # If we get here, try a last-resort repo fetch (keeps previous behavior)
    master = await MasterRepo.get_master(master_id)
    if not master:
        await callback.answer(t("master_not_found", lang), show_alert=True)
        return

    # Delegate to shared formatter using minimal data
    try:
        minimal = {
            "master": master,
            "master_id": master_id,
        }
        from bot.app.telegram.master.master_keyboards import format_master_profile_text as _fmt
        text = _fmt(minimal, lang)
        await callback.answer(text, show_alert=True)
        logger.info("Информация о мастере %s показана пользователю %s (fallback)", master_id, callback.from_user.id)
    except (AttributeError, ValueError, SQLAlchemyError):
        logger.exception("Ошибка при получении информации о мастере (fallback)", exc_info=True)
        lang = await resolve_locale(None, locale, callback.from_user.id)
        await callback.answer("⚠️ " + t("error_retry", lang), show_alert=True)

@client_router.message(
    F.text
    & ~F.entities
    & ~F.text.startswith("/")
    & (F.state == None)
)
async def debug_any_message(message: Message, state: FSMContext, locale: str) -> None:
    """Show the main menu when arbitrary text arrives without active FSM."""
    user_id = message.from_user.id if message.from_user else 0
    text = message.text or ""
    logger.info("DEBUG_ANY_MESSAGE: text=%r от пользователя %s", text, user_id)
    # If the user currently has an active FSM state, don't swallow the
    # message here — let stateful handlers (admin/master/etc.) receive it.
    try:
        cur_state = await state.get_state()
    except (RuntimeError, AttributeError, TypeError):
        cur_state = None
    if cur_state:
        logger.debug("debug_any_message skipping handling because user %s has FSM state %r", user_id, cur_state)
        return
    # Commands (starting with '/') are intentionally ignored here so that
    # their specific handlers (e.g. cmd_start) are invoked by the router.
    lang = await resolve_locale(None, locale, user_id)
    try:
        await show_main_menu(message, state, prefer_edit=False)
    except Exception:
        logger.exception("debug_any_message: failed to show main menu")
        await message.answer(t("bot_started_notice", lang))
    # Unexpected exceptions will be handled by centralized router-level error handlers


@client_router.message(F.text & ~F.entities & (F.state == None))
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
