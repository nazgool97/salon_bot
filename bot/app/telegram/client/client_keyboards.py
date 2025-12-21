from __future__ import annotations
import logging
from datetime import date, datetime, timedelta, time as dt_time
from typing import Any, Literal, Protocol, Sequence, runtime_checkable

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Keyboards are presentation-only. Handlers must prefetch data (available_days,
# allowed_weekdays, services, etc.) and pass them into these factories.
# Avoid importing DB/service helpers here to prevent cyclic deps and hidden
# side-effects.

from bot.app.telegram.common.callbacks import (
    pack_cb,
    CalendarCB,
    DateCB,
    MasterSelectCB,
    ServiceSelectCB,
    TimeCB,
    RescheduleCB,
    HoursViewCB,
    HourCB,
    CancelTimeCB,
    TimeAdjustCB,
)
from bot.app.telegram.common.callbacks import MasterMenuCB, NavCB, ClientMenuCB, RatingCB
from bot.app.telegram.common.callbacks import MasterServicesCB, MastersListCB
from bot.app.telegram.common.callbacks import PayCB
from bot.app.telegram.common.roles import is_admin, is_master
from bot.app.domain.models import Master, MasterService, Service
from bot.app.services.shared_services import (
    safe_get_locale as _get_locale,
    default_language,
    format_date,
    format_money_cents,
    local_now,
    format_slot_label,
    is_online_payments_available,
)

from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from bot.app.translations import t, tr as _tr
from bot.app.telegram.common.navigation import nav_push  # Добавляем импорт
from bot.app.telegram.common.ui_fail_safe import safe_edit  # Добавляем импорт
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def get_back_button() -> InlineKeyboardMarkup:
    """One universal Back button keyboard.

    Kept here so all roles (admin/master/client) can reuse without importing
    service-layer helpers. Only localization is applied; no role logic.
    """
    from bot.app.telegram.common.callbacks import pack_cb, NavCB
    lang = default_language()
    back_txt = _localize("back", lang, "⬅️ Назад")
    b = InlineKeyboardBuilder()
    b.button(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
    return b.as_markup()


def get_simple_kb(
    buttons: list[tuple[str, str]],
    cols: int = 1,
    *,
    add_back: bool | str = False,
    back_cb: str | None = None,
    lang: str | None = None,
) -> InlineKeyboardMarkup:
    """Build a simple inline keyboard from (text, callback_data) pairs.

    - buttons: list of (text, callback) in order
    - cols: number of columns
    - add_back: True or str to append a Back button; str overrides label
    - back_cb: custom callback for back (defaults to NavCB/back)
    - lang: used for localizing back label
    """
    from bot.app.telegram.common.callbacks import pack_cb, NavCB
    kb = InlineKeyboardBuilder()
    for text, data in buttons:
        kb.button(text=text, callback_data=data)
    if add_back:
        if isinstance(add_back, str):
            back_text = add_back
        else:
            use_lang = lang or default_language()
            back_text = _localize("back", use_lang, "⬅️ Назад")
        payload = back_cb or pack_cb(NavCB, act="back")
        kb.button(text=back_text, callback_data=payload)
    try:
        cols = max(1, int(cols))
    except Exception:
        cols = 1
    kb.adjust(cols)
    return kb.as_markup()


async def get_time_slots_kb(
    slots: Sequence[datetime | dt_time],
    *,
    action: Literal["booking", "reschedule"],
    date: str,
    lang: str,
    service_id: str | None = None,
    master_id: int | None = None,
    booking_id: int | None = None,
) -> InlineKeyboardMarkup:
    """Builds the booking or reschedule keyboard for a list of time slots."""

    builder = InlineKeyboardBuilder()

    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    normalized_service = str(service_id or "")
    normalized_master = _to_int(master_id)
    normalized_booking = _to_int(booking_id)

    for slot in slots:
        label = format_slot_label(slot)
        compact_time = slot.strftime("%H%M")
        if action == "reschedule":
            callback_data = pack_cb(
                RescheduleCB,
                action="time",
                booking_id=normalized_booking,
                date=date,
                time=compact_time,
            )
        else:
            callback_data = pack_cb(
                TimeCB,
                service_id=normalized_service,
                master_id=normalized_master,
                date=date,
                time=compact_time,
            )
        builder.button(text=label, callback_data=callback_data)

    # Offer a stepwise hour->minute picker entry
    choose_by_hour = _localize("choose_time_by_hour", lang, "Выбрать время")
    builder.button(text=choose_by_hour, callback_data=pack_cb(HoursViewCB, service_id=str(service_id or ""), master_id=int(master_id or 0), date=date))

    # Compact picker button: open +/- picker prefilled with first available slot
    compact_label = _localize("compact_picker", lang, "Compact picker")
    # Determine initial hour/minute from first slot if available
    ih = 0
    im = 0
    try:
        if slots:
            first = slots[0]
            if isinstance(first, str):
                ih = int(first[:2])
                im = int(first[2:4])
            else:
                # datetime or time
                ih = int(getattr(first, 'hour', 0) or 0)
                im = int(getattr(first, 'minute', 0) or 0)
    except Exception:
        ih = 0
        im = 0
    builder.button(text=compact_label, callback_data=pack_cb(TimeAdjustCB, op="noop", hour=int(ih), minute=int(im), service_id=str(service_id or ""), master_id=int(master_id or 0), date=date))

    back_text = _localize("back", lang, "⬅️ Назад")
    builder.button(text=back_text, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(3, 3, 3, 1, 1)
    return builder.as_markup()


async def get_hour_picker_kb(hours: Sequence[int], *, service_id: str | None = None, master_id: int | None = None, date: str, lang: str, cols: int = 4) -> InlineKeyboardMarkup:
    """Build an hours keyboard (e.g. 09:00, 10:00)."""
    builder = InlineKeyboardBuilder()
    normalized_service = str(service_id or "")
    normalized_master = int(master_id or 0)
    for h in sorted(set(int(x) for x in hours)):
        label = f"{h:02d}:00"
        builder.button(text=label, callback_data=pack_cb(HourCB, service_id=normalized_service, master_id=normalized_master, date=date, hour=int(h)))
    back_text = _localize("back", lang, "⬅️ Назад")
    # Cancel returns to nav back and clears FSM (handler will manage state)
    builder.button(text=back_text, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(cols)
    return builder.as_markup()


async def get_minute_picker_kb(minutes: Sequence[int], *, service_id: str | None = None, master_id: int | None = None, date: str, hour: int, lang: str, action: Literal["booking", "reschedule"] = "booking", booking_id: int | None = None, cols: int = 4) -> InlineKeyboardMarkup:
    """Build minutes keyboard (e.g. 00,15,30,45). Back returns to hour picker."""
    builder = InlineKeyboardBuilder()
    normalized_service = str(service_id or "")
    normalized_master = int(master_id or 0)
    normalized_booking = int(booking_id or 0)
    for m in sorted(set(int(x) for x in minutes)):
        # Display minutes as requested: '00', '5', '10', '15', ...
        if int(m) == 0:
            label = "00"
        else:
            label = str(int(m))
        compact = f"{int(hour):02d}{int(m):02d}"
        if action == "reschedule":
            cb = pack_cb(RescheduleCB, action="time", booking_id=normalized_booking, date=date, time=compact)
        else:
            cb = pack_cb(TimeCB, service_id=normalized_service, master_id=normalized_master, date=date, time=compact)
        builder.button(text=label, callback_data=cb)

    back_text = _localize("back", lang, "⬅️ Назад")
    # Back returns to the previous step (handler will manage exact nav)
    builder.button(text=back_text, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(cols)
    return builder.as_markup()


async def get_compact_time_picker_kb(
    hour: int,
    minute: int,
    *,
    service_id: str | None = None,
    master_id: int | None = None,
    date: str,
    lang: str,
    minute_step: int = 5,
    cols: int = 2,
    action: Literal["booking", "reschedule"] = "booking",
    booking_id: int | None = None,
) -> InlineKeyboardMarkup:
    """Compact +/- time picker keyboard.

    Layout (approx):
    [  HH:MM  ]
    [ -hr | +hr ]
    [ -min | +min ]
    [ submit ]
    [ cancel ]

    Buttons call `TimeAdjustCB` to increment/decrement and reconstruct the keyboard.
    The submit button packs `TimeCB` with the currently selected time.
    """
    from bot.app.telegram.common.callbacks import pack_cb, TimeAdjustCB, TimeCB, CancelTimeCB, RescheduleCB

    builder = InlineKeyboardBuilder()
    normalized_service = str(service_id or "")
    normalized_master = int(master_id or 0)

    # Top label (more visible): formatted time (emoji removed per UX request).
    top_label = f"{hour:02d}:{minute:02d}"
    builder.button(text=top_label, callback_data=pack_cb(TimeAdjustCB, op="noop", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))

    # Localized labels for Hours / Minutes
    hours_label = _localize("picker_hours_label", lang, "Hours")
    minutes_label = _localize("picker_minutes_label", lang, "Minutes")
    # Use noop callbacks for label buttons to keep layout consistent
    builder.button(text=hours_label, callback_data=pack_cb(TimeAdjustCB, op="noop", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))
    builder.button(text=minutes_label, callback_data=pack_cb(TimeAdjustCB, op="noop", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))

    # Adjustment rows: place hour +/- in the left column and minute +/- in the right column
    # Plus row (on top) so users see increment above and decrement below
    builder.button(text="➕", callback_data=pack_cb(TimeAdjustCB, op="hour_inc", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))
    builder.button(text="➕", callback_data=pack_cb(TimeAdjustCB, op="min_inc", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))
    # Minus row (below)
    builder.button(text="➖", callback_data=pack_cb(TimeAdjustCB, op="hour_dec", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))
    builder.button(text="➖", callback_data=pack_cb(TimeAdjustCB, op="min_dec", hour=int(hour), minute=int(minute), service_id=normalized_service, master_id=normalized_master, date=date))

    # Submit: pack TimeCB or RescheduleCB with compact time, localized label
    compact = f"{int(hour):02d}{int(minute):02d}"
    submit_text = _localize("picker_submit", lang, "Submit")

    # If this picker is used in a reschedule flow, pack RescheduleCB
    if action == "reschedule":
        cb_data = pack_cb(RescheduleCB, action="time", booking_id=int(booking_id or 0), date=date, time=compact)
    else:
        cb_data = pack_cb(TimeCB, service_id=normalized_service, master_id=normalized_master, date=date, time=compact)

    builder.button(text=submit_text, callback_data=cb_data)

    # Cancel localized
    cancel_text = _localize("picker_cancel", lang, "Cancel")
    builder.button(text=cancel_text, callback_data=pack_cb(CancelTimeCB))

    # Arrange: top single, then two-column rows, then single-column actions
    builder.adjust(1, 2, 2, 2, 1, 1)
    return builder.as_markup()

logger = logging.getLogger(__name__)

def _localize(key: str, lang: str, fallback: str) -> str:
    # Ignore hardcoded fallbacks; rely on translations. If missing in the
    # requested locale, fallback to English, then the raw key.
    try:
        val = t(key, lang)
        if val == key:
            val = t(key, "en")
        return val
    except Exception:
        return t(key, "en") if key else fallback


async def _resolve_lang(user_id: int | None = None) -> str:
    if user_id:
        try:
            return await _get_locale(user_id)
        except Exception:
            logger.exception("get_masters_catalog_keyboard: failed to build pagination row")
    return default_language()


def _default_currency() -> str:
    # UI-only synchronous fallback used by keyboard factories.
    # Delegate to the canonical helper in shared_services so the SSoT is
    # centralized. This keeps keyboard code lightweight and consistent.
    from bot.app.services.shared_services import _default_currency
    return _default_currency()


@runtime_checkable
class _HasMasterAttrs(Protocol):
    """Протокол для объектов мастера с необходимыми атрибутами."""
    name: str
    telegram_id: int


# `_allowed_weekdays` removed — handlers should prefetch and pass allowed weekdays.


def _build_week_row_states(
    service_id: str,
    master_id: int,
    year: int,
    month: int,
    week_states: list[tuple[int, str]],
) -> list[InlineKeyboardButton]:
    """Convert precomputed week day states into InlineKeyboardButtons.

    BUGFIX: Previously used datetime.now() year/month causing ValueError for days
    that don't exist in the current month when viewing a different month (e.g.
    month=12 while current month=11 with day=31). We now use the calendar's
    explicit year/month arguments.

        States from compute_calendar_day_states:
            empty -> space placeholder
            past -> ✖
            not_allowed -> —
            available -> clickable day number (packs real ISO date)
            full -> 🔴 (fully booked / no slots)
    """
    from datetime import date as _date
    row: list[InlineKeyboardButton] = []
    for day, state in week_states:
        if state == 'empty':
            row.append(InlineKeyboardButton(text=" ", callback_data=pack_cb(NavCB, act="noop")))
            continue
        if state == 'past':
            row.append(InlineKeyboardButton(text="✖", callback_data=pack_cb(NavCB, act="noop")))
            continue
        if state == 'not_allowed':
            row.append(InlineKeyboardButton(text="—", callback_data=pack_cb(NavCB, act="noop")))
            continue
        if state == 'available':
            try:
                day_date = _date(year, month, day)
                cb = pack_cb(DateCB, service_id=service_id, master_id=master_id, date=str(day_date))
                row.append(InlineKeyboardButton(text=str(day), callback_data=cb))
            except Exception:
                row.append(InlineKeyboardButton(text="🔴", callback_data=pack_cb(NavCB, act="noop")))
            continue
        # full / fallback
        row.append(InlineKeyboardButton(text="🔴", callback_data=pack_cb(NavCB, act="noop")))
    return row

def _build_month_nav_row(service_id: str, master_id: int, year: int, month: int, month_label: str) -> list[InlineKeyboardButton]:
    """Return navigation row (prev, current month label, next) for calendar.

    Extracted from get_calendar_keyboard to reduce its size. Accepts a
    pre-localized month_label so localization stays outside of this pure
    structural helper.
    """
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    return [
        InlineKeyboardButton(
            text="◀️",
            callback_data=pack_cb(CalendarCB, service_id=service_id, master_id=master_id, year=prev_year, month=prev_month),
        ),
        InlineKeyboardButton(text=month_label, callback_data=pack_cb(NavCB, act="noop")),
        InlineKeyboardButton(
            text="▶️",
            callback_data=pack_cb(CalendarCB, service_id=service_id, master_id=master_id, year=next_year, month=next_month),
        ),
    ]


async def get_calendar_keyboard(
    service_id: str,
    master_id: int,
    year: int | None = None,
    month: int | None = None,
    service_duration_min: int = 60,
    user_id: int | None = None,
    available_days: set[int] | None = None,
    *,
    allowed_weekdays: list[int] | None = None,
    max_days: int = 365,
    day_states: list[list[tuple[int, str]]] | None = None,
) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру календаря из уже подготовленных состояний дней.

    Внимание: вычисление доступных дней/состояний должно происходить в хендлере.
    Этот билдер не обращается к сервисам/БД и использует переданные `day_states`.
    """
    lang = await _resolve_lang(user_id)
    _t = t

    # Ограничение на выбор дат передается обработчиком (UI-only)
    try:
        max_days = int(max_days)
    except Exception:
        max_days = 365
    max_date = local_now().date() + timedelta(days=int(max_days))

    if year is None or month is None:
        now = local_now()
        year, month = now.year, now.month
    if date(year, month, 1) > max_date:
        logger.warning("Попытка открыть календарь для слишком далекого будущего: %d-%d", year, month)
        year, month = max_date.year, max_date.month

    today = local_now().date()
    buttons: list[list[InlineKeyboardButton]] = []

    # Заголовок месяца с локализацией
    try:
        from bot.app.services.client_services import compute_month_label

        month_label = compute_month_label(year, month, lang)
    except Exception:
        try:
            # Fallback: numeric month/year if localization failed
            month_label = f"{int(month):02d}.{year}"
        except Exception:
            month_label = f"{month}/{year}"

    # Navigation row (prev/current/next month)
    buttons.append(_build_month_nav_row(service_id, master_id, year, month, month_label))
    # Недели с локализованными короткими днями недели
    try:
        weekdays = _tr("weekday_short", lang) if _tr is not None else None
        if isinstance(weekdays, list) and weekdays:
            wd = weekdays
        else:
            wd = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
    except Exception:
        wd = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
    buttons.append([InlineKeyboardButton(text=n, callback_data=pack_cb(NavCB, act="noop")) for n in wd])

    # Use precomputed day_states provided by handler
    if day_states is None:
        # As a safe fallback, render empty calendar rows
        day_states = []

    for week_states in day_states:
        try:
            buttons.append(_build_week_row_states(service_id, master_id, year, month, week_states))
        except Exception as e:
            logger.exception("Error building week row from states: %s", e)
            buttons.append([InlineKeyboardButton(text=(_t("error", lang) if _t is not None else "Ошибка"), callback_data=pack_cb(NavCB, act="noop"))])

    # Кнопка назад
    try:
        back_txt = t("back", lang)
    except Exception:
        back_txt = "⬅️ Back"
    buttons.append([InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back"))])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


# Service lookup/filtering moved to `bot.app.services.client_services.get_filtered_services`
# to keep keyboards presentation-only.


async def get_service_menu(services: dict[str, str], include_back: bool = True) -> InlineKeyboardMarkup:
    """Генерирует меню выбора услуг из переданного отображения service_id->name.

    Handlers must prefetch `services` (use `client_services.get_filtered_services`).
    """
    builder = InlineKeyboardBuilder()
    for service_id, name in services.items():
        builder.button(
            text=name,
            callback_data=pack_cb(ServiceSelectCB, service_id=service_id),
        )
    # Extra entry for multi-select flow
    lang = await _resolve_lang(None)
    multi_txt = _localize("multi_services_button", lang, "🧰 Кілька послуг")
    builder.button(text=multi_txt, callback_data=pack_cb(ClientMenuCB, act="services_multi"))

    if include_back:
        back_txt = _localize("back", lang, "⬅️ Назад")
    builder.button(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(1)
    logger.debug("Меню услуг сгенерировано, количество услуг: %d", len(services))
    return builder.as_markup()


async def get_service_menu_multi(selected: set[str], services: dict[str, str]) -> InlineKeyboardMarkup:
    """Генерирует меню для множественного выбора услуг с отметками выбранных.

    `services` must be provided by the caller (handlers should call
    `client_services.get_filtered_services()` before invoking this).
    """
    builder = InlineKeyboardBuilder()
    lang = await _resolve_lang(None)
    cont_txt = _localize("continue", lang, "✅ Продовжити")
    back_txt = _localize("back", lang, "⬅️ Назад")

    # Формируем кнопки для услуг с отметками
    from bot.app.telegram.common.callbacks import ServiceToggleCB
    for service_id, name in services.items():
        mark = "✅" if service_id in selected else "☑️"
        builder.button(
            text=f"{mark} {name}",
            callback_data=pack_cb(ServiceToggleCB, service_id=service_id),
        )

    # Кнопки управления
    builder.button(text=cont_txt, callback_data=pack_cb(ClientMenuCB, act="svc_done"))
    builder.button(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(1, 1)  # Две колонки для услуг, одна для управления
    logger.debug("Меню мультивыбора услуг сгенерировано, услуг: %d, выбрано: %d", len(services), len(selected))
    return builder.as_markup()


async def get_master_keyboard(service_id: str, masters: list | None) -> InlineKeyboardMarkup:
    """Build master selection keyboard from pre-fetched masters list.

    Args:
        service_id: service id used for callback payloads.
        masters: list of master-like objects (with .name and .telegram_id) provided by caller.
    """
    builder = InlineKeyboardBuilder()
    masters_list: list[_HasMasterAttrs] = list(masters or [])
    lang = await _resolve_lang(None)
    # Lazy import to avoid cyclic deps
    from bot.app.telegram.common.callbacks import MasterProfileCB

    if masters_list:
        for master in masters_list:
            name = getattr(master, "name", str(getattr(master, "telegram_id", "?")))
            # Presentation-only: rely on provided fields; do not resolve via services.
            raw_mid = getattr(master, "id", None)
            if raw_mid is None:
                raw_mid = getattr(master, "telegram_id", 0)
            try:
                mid = int(raw_mid or 0)
            except Exception:
                mid = 0

            # Кнопка 1: Просмотреть карточку мастера (био/расписание/услуги)
            builder.button(
                text=f"👤 {name}",
                callback_data=pack_cb(MasterProfileCB, service_id=service_id or "", master_id=mid),
            )

            # Кнопка 2: Сразу к записи (пропустить профиль)
            builder.button(
                text=f"{t('book', lang)}",
                callback_data=pack_cb(MasterSelectCB, master_id=mid, service_id=service_id),
            )

        # расположить кнопки мастеров по 2 в ряд (Профіль | Запис)
        builder.adjust(2)
    else:
        builder.button(
            text=t("no_masters", lang),
            callback_data="no_masters"
        )

    back_txt = t("back", lang)

    # кнопка "Назад" отдельной строкой в самом низу
    builder.row(InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back")))

    return builder.as_markup()


async def get_masters_catalog_keyboard(masters: list | None, *, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Build a generic masters catalog keyboard (no service preselected).

    Renders per-master row: [👤 Profile] [🛠️ Послуги]
    """
    builder = InlineKeyboardBuilder()
    lang = await _resolve_lang(None)
    masters_list = list(masters or [])
    # Pagination row at top if multiple pages
    if isinstance(total_pages, int) and total_pages > 1:
        nav_buttons: list[InlineKeyboardButton] = []
        try:
            if page > 1:
                nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=pack_cb(MastersListCB, page=page-1)))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=pack_cb(MastersListCB, page=page+1)))
            if nav_buttons:
                builder.row(*nav_buttons)
        except Exception:
            pass

    if masters_list:
        # Lazy import to avoid cyclic deps when module is imported by services
        from bot.app.telegram.common.callbacks import MasterProfileCB

        for m in masters_list:
            try:
                if isinstance(m, tuple) and len(m) >= 2:
                    # tuples returned by repos are (id, name)
                    mid = int(m[0])
                    name = str(m[1])
                else:
                    # Presentation-only: use provided attributes without service calls
                    raw_mid = getattr(m, "id", None)
                    if raw_mid is None:
                        raw_mid = getattr(m, "telegram_id", 0)
                    mid = int(raw_mid or 0)
                    name = str(getattr(m, "name", mid))
            except Exception:
                mid = int(getattr(m, "telegram_id", 0))
                name = str(getattr(m, "name", mid))

            # Left: Просмотр профиля мастера (карточка с расписанием/услугами)
            builder.button(
                text=f"👤 {name}",
                callback_data=pack_cb(MasterProfileCB, service_id="", master_id=mid),
            )
            # Do not include a direct "Запис" button in the public masters catalog.
            # Users should view the profile and then start booking from there.
            builder.adjust(1)
    else:
        builder.button(text=_localize("no_masters", lang, "❌ Нема доступних майстрів"), callback_data="no_masters")

    back_txt = _localize("back", lang, "⬅️ Назад")
    builder.row(InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back")))
    return builder.as_markup()



# `get_back_button` moved to `bot.app.services.shared_services` (avoid duplication).


STAR_EMOJI = {1: "⭐", 2: "⭐⭐", 3: "⭐⭐⭐", 4: "⭐⭐⭐⭐", 5: "⭐⭐⭐⭐⭐"}


def build_rating_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для выбора рейтинга бронирования."""
    lang = default_language()
    builder = InlineKeyboardBuilder()
    from typing import cast, Any
    for i in range(1, 6):
        builder.row(
            InlineKeyboardButton(
                text=STAR_EMOJI[i],
                callback_data=pack_cb(RatingCB, booking_id=int(booking_id), rating=int(i)),
            )
        )
    skip_txt = _localize("skip", lang, "Пропустити")
    builder.button(text=skip_txt, callback_data=pack_cb(NavCB, act="skip_rating"))
    builder.adjust(1)
    logger.debug("Клавиатура рейтинга сгенерирована для брони %d", booking_id)
    return builder.as_markup()


async def get_main_menu(telegram_id: int) -> InlineKeyboardMarkup:
    """Генерирует главное меню с учетом прав администратора и мастера."""
    logger.debug("Генерация главного меню для telegram_id=%s", telegram_id)
    try:
        admin_flag = await is_admin(telegram_id)
        master_flag = await is_master(telegram_id)
        logger.debug("is_admin(%s) -> %s, is_master(%s) -> %s", telegram_id, admin_flag, telegram_id, master_flag)

        builder = InlineKeyboardBuilder()
        lang = await _resolve_lang(telegram_id)
        _t = lambda key, default: _localize(key, lang, default)

        builder.button(text=_t("book", "Записатися"), callback_data=pack_cb(ClientMenuCB, act="booking_service"))
        # New entry: browse by master
        builder.button(text=_t("masters_button", "Наші майстри"), callback_data=pack_cb(ClientMenuCB, act="masters_list"))
        from bot.app.telegram.common.callbacks import MyBookingsCB
        # Default entry to 'Мої записи' should show upcoming bookings only.
        builder.button(text=_t("my_bookings_button", "Мої записи"), callback_data=pack_cb(MyBookingsCB, mode="upcoming"))
        builder.button(text=_t("contacts", "Контакти"), callback_data=pack_cb(ClientMenuCB, act="contacts"))

        if admin_flag:
            from bot.app.telegram.common.callbacks import AdminMenuCB
            builder.button(text=_t("admin_panel_button", "Управління"), callback_data=pack_cb(AdminMenuCB, act="panel"))
        if master_flag:
            # Use typed MasterMenuCB for master menu navigation
            from bot.app.telegram.common.callbacks import MasterMenuCB
            builder.button(text=_t("master_menu_button", "Меню майстра"), callback_data=pack_cb(MasterMenuCB, act="menu"))

        builder.adjust(2)
        logger.debug("Главное меню сгенерировано для telegram_id=%s", telegram_id)
        return builder.as_markup()
    except Exception as e:
        logger.error("Ошибка построения главного меню для telegram_id=%s: %s", telegram_id, e)
        return InlineKeyboardBuilder().as_markup()
    

def build_bookings_dashboard_kb(role: str, meta: dict | None, lang: str = "uk"):
    """Build a unified bookings dashboard keyboard for client/master/admin.

    This function was moved from shared_services to keep UI builders in
    telegram-specific modules. It intentionally does lazy imports to avoid
    import cycles with services.
    """
    try:
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        from bot.app.telegram.common.callbacks import pack_cb, NavCB, BookingsPageCB
        from bot.app.translations import tr

        # Import role-specific booking callbacks lazily to avoid cycles
        if str(role).lower() == "client":
            from bot.app.telegram.common.callbacks import MyBookingsCB as RoleCB
        elif str(role).lower() == "master":
            from bot.app.telegram.common.callbacks import MasterBookingsCB as RoleCB
        else:
            from bot.app.telegram.common.callbacks import AdminBookingsCB as RoleCB

        FiltersCB = None
        if str(role).lower() == "master":
            try:
                from bot.app.telegram.common.callbacks import MasterMenuCB

                FiltersCB = MasterMenuCB
            except Exception:
                FiltersCB = None
        elif str(role).lower() == "admin":
            try:
                from bot.app.telegram.common.callbacks import AdminMenuCB

                FiltersCB = AdminMenuCB
            except Exception:
                FiltersCB = None

        kb = InlineKeyboardBuilder()

        # Prepare concise labels (no numeric indicators).
        # Use short canonical labels; translations may provide localized equivalents
        upcoming_label = _localize("upcoming", lang, "Upcoming")
        done_label = _localize("master_completed", lang, "Done")
        cancelled_label = _localize("cancelled", lang, "Cancelled")
        noshow_label = _localize("no_show", lang, "No-show")

        mode = (meta.get("mode") if meta else None) or "upcoming"

        def mark(lbl: str, tab: str) -> str:
            return f"✅ {lbl}" if tab == mode else lbl

        # For client role we render tabs at the BOTTOM alongside Back so UX is:
        # - upcoming mode: show upcoming list; bottom row = [Done, Back]
        # - completed mode: show completed+cancelled; top pagination row appears; bottom row = [Upcoming, Back]
        if str(role).lower() == "client":
            # Pagination (render at top for completed mode)
            try:
                page = int(meta.get("page", 1) if meta else 1)
                total_pages = int(meta.get("total_pages", 1) if meta else 1)
                nav_buttons: list[InlineKeyboardButton] = []
                # Allow pagination for both upcoming and completed modes
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=pack_cb(RoleCB, mode=mode, page=page - 1)))
                if page < max(1, int(total_pages or 1)):
                    nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=pack_cb(RoleCB, mode=mode, page=page + 1)))
                if nav_buttons:
                    kb.row(*nav_buttons)
            except Exception:
                logger.exception("get_masters_catalog_keyboard: failed to extract master id/name")

            # Back button callback (client-specific)
            try:
                if mode == "completed":
                    back_cb = pack_cb(RoleCB, mode="upcoming", page=1)
                else:
                    back_cb = pack_cb(NavCB, act="root")
            except Exception:
                back_cb = pack_cb(NavCB, act="role_root")

            # Bottom row: two buttons
            bottom_row: list[InlineKeyboardButton] = []
            try:
                if mode == "upcoming":
                    # show Done + Back
                    done_cb = pack_cb(RoleCB, mode="completed", page=1)
                    bottom_row.append(InlineKeyboardButton(text=done_label, callback_data=done_cb))
                else:
                    # completed mode: show Upcoming + Back
                    upcoming_cb = pack_cb(RoleCB, mode="upcoming", page=1)
                    bottom_row.append(InlineKeyboardButton(text=upcoming_label, callback_data=upcoming_cb))
            except Exception:
                pass

            # Back on the right
            bottom_row.append(InlineKeyboardButton(text=_localize("back", lang, "⬅️ Назад"), callback_data=back_cb))
            if bottom_row:
                kb.row(*bottom_row)
            return kb.as_markup()

        # Non-client roles: keep existing top-tabs + pagination behavior
        client_done_mode = "completed"
        master_done_mode = "done"
        # masters/admins see both upcoming and done tabs
        kb.button(
            text=mark(upcoming_label, "upcoming"),
            callback_data=pack_cb(RoleCB, mode="upcoming") if str(role).lower() != "client" else pack_cb(RoleCB, mode="upcoming", page=1),
        )
        kb.button(
            text=mark(done_label, client_done_mode if str(role).lower() == "client" else master_done_mode),
            callback_data=pack_cb(RoleCB, mode=(client_done_mode if str(role).lower() == "client" else master_done_mode)) if str(role).lower() != "client" else pack_cb(RoleCB, mode=(client_done_mode if str(role).lower() == "client" else master_done_mode), page=1),
        )

        if str(role).lower() != "client":
            kb.button(text=mark(cancelled_label, "cancelled"), callback_data=pack_cb(RoleCB, mode="cancelled", page=1) if str(role).lower() == "client" else pack_cb(RoleCB, mode="cancelled"))
            kb.button(text=mark(noshow_label, "no_show"), callback_data=pack_cb(RoleCB, mode="no_show", page=1) if str(role).lower() == "client" else pack_cb(RoleCB, mode="no_show"))

        kb.adjust(4) # Adjust the 4 tabs

        # Pagination row for non-client roles
        try:
            page = int(meta.get("page", 1) if meta else 1)
            total_pages = int(meta.get("total_pages", 1) if meta else 1)
            nav_buttons: list[InlineKeyboardButton] = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=pack_cb(RoleCB, mode=mode, page=page - 1)))
            if page < max(1, int(total_pages or 1)):
                nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=pack_cb(RoleCB, mode=mode, page=page + 1)))
            if nav_buttons:
                kb.row(*nav_buttons) # Add pagination row
        except Exception:
            pass

        # Back button for non-client roles
        try:
            if str(role).lower() == "master":
                from bot.app.telegram.common.callbacks import MasterMenuCB

                back_cb = pack_cb(MasterMenuCB, act="menu")
            elif str(role).lower() == "admin":
                # For admin dashboards prefer an explicit target. If a master
                # filter is present, Back should return to that master's card;
                # otherwise return to Admin Panel.
                try:
                    from bot.app.telegram.common.callbacks import AdminMenuCB, AdminMasterCardCB

                    if isinstance(meta, dict) and meta.get("master_id"):
                        back_cb = pack_cb(AdminMasterCardCB, master_id=int(meta.get("master_id")))
                    else:
                        back_cb = pack_cb(AdminMenuCB, act="panel")
                except Exception:
                    back_cb = pack_cb(NavCB, act="back")
            else:
                if str(role).lower() == "client" and mode == "completed":
                    back_cb = pack_cb(RoleCB, mode="upcoming", page=1)
                else:
                    back_cb = pack_cb(NavCB, act="root")
        except Exception:
            back_cb = pack_cb(NavCB, act="role_root")
        kb.row(InlineKeyboardButton(text=_localize("back", lang, "⬅️ Назад"), callback_data=back_cb))
        return kb.as_markup()
    except Exception as e:
        logger.exception("build_bookings_dashboard_kb failed: %s", e)
        try:
            # Avoid importing InlineKeyboardButton here to prevent it being
            # treated as a local variable in the function scope (which causes
            # UnboundLocalError when referenced earlier). Use the module-level
            # InlineKeyboardButton imported at top-level instead.
            from aiogram.types import InlineKeyboardMarkup
            from bot.app.telegram.common.callbacks import pack_cb, NavCB
            return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=_localize('back', lang, "⬅️ Назад"), callback_data=pack_cb(NavCB, act="role_root"))]])
        except Exception:
            from aiogram.types import InlineKeyboardMarkup

            return InlineKeyboardMarkup(inline_keyboard=[])


async def get_payment_keyboard(
    booking: object,
    service_name: str,
    master_name: str | None,
    user_id: int,
    date: str | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    """Генерирует клавиатуру выбора оплаты и текст заголовка."""
    lang = await _resolve_lang(user_id)
    _t = lambda key, default: _localize(key, lang, default)

    try:
        from bot.app.services.client_services import build_booking_details
        from bot.app.services.shared_services import format_booking_details_text, format_date

        bd = await build_booking_details(
            booking,
            service_name=service_name,
            master_name=master_name,
            user_id=user_id,
            date=date,
            lang=lang,
        )
        header = format_booking_details_text(bd, lang)

        # Добавляем время только в строку даты
        starts = getattr(bd, "starts_at", None)
        if starts:
            start_time_str = format_date(starts, fmt="%H:%M")
            # заменяем "Дата: <b>25.12.2025</b>" → "Дата: <b>25.12.2025 09:00</b>"
            header = header.replace(
                f"{_t('date_label','Дата')}: <b>{date}</b>",
                f"{_t('date_label','Дата')}: <b>{date} {start_time_str}</b>"
            )

        # Отступ и выбор оплаты
        header = f"{header}\n\n{_t('choose_payment_label', 'Оберіть спосіб оплати')}:"

    except Exception:
        logger.exception("get_payment_keyboard: failed to build canonical header")
        master_txt = master_name or _t("master_label", "Майстер")
        booking_date = date or "—"
        header = (
            f"<b>{_t('booking_label', 'Запис')}</b>\n"
            f"{_t('service_label', 'Послуга')}: <b>{service_name}</b>\n"
            f"{_t('master_label', 'Майстер')}: {master_txt}\n"
            f"{_t('date_label', 'Дата')}: <b>{booking_date}</b>\n\n"
            f"{_t('choose_payment_label', 'Оберіть спосіб оплати')}:"
        )

    # booking_id
    booking_id_val = 0
    try:
        if isinstance(booking, int):
            booking_id_val = int(booking)
        elif isinstance(booking, dict):
            booking_id_val = int(booking.get("id") or booking.get("booking_id") or 0)
        else:
            booking_id_val = int(getattr(booking, "id", None) or getattr(booking, "booking_id", None) or 0)
    except Exception:
        pass

    builder = InlineKeyboardBuilder()
    if await is_online_payments_available():
        builder.button(
            text=_t("online_payment_button", "💳 Онлайн-оплата"),
            callback_data=pack_cb(PayCB, action="prep_online", booking_id=booking_id_val),
        )
    builder.button(
        text=_t("cash_button", " Готівка"),
        callback_data=pack_cb(PayCB, action="prep_cash", booking_id=booking_id_val),
    )
    from bot.app.telegram.common.callbacks import BookingActionCB
    builder.button(
        text=_t("back", "⬅️ Назад"),
        callback_data=pack_cb(BookingActionCB, act="cancel_reservation", booking_id=booking_id_val),
    )
    builder.button(
        text=_t("menu", "🏠 Меню"),
        callback_data=pack_cb(BookingActionCB, act="cancel_and_root", booking_id=booking_id_val),
    )
    builder.adjust(1, 1, 2)

    logger.debug("Клавиатура оплаты сгенерирована для брони %s", booking_id_val)
    return header, builder.as_markup()


def home_kb() -> InlineKeyboardMarkup:
    """Генерирует клавиатуру с кнопкой возврата в главное меню."""
    builder = InlineKeyboardBuilder()
    lang = default_language()
    menu_txt = _localize("menu", lang, "🏠 Меню")
    builder.button(text=menu_txt, callback_data=pack_cb(NavCB, act="root"))
    logger.debug("Клавиатура возврата в меню сгенерирована")
    return builder.as_markup()


__all__ = [
    "get_calendar_keyboard",
    "get_service_menu",
    "get_master_keyboard",
    "build_rating_keyboard",
    "get_main_menu",
    "get_payment_keyboard",
    "home_kb",
]

# ---------------- UI renderers moved from shared_services (SoC) ---------------- #
from typing import Any, Sequence
from datetime import datetime, UTC


# format_master_profile_text lives in `bot.app.services.master_services` (service layer formatter)


# format_booking_list_item moved to bot.app.services.client_services (formatting belongs in services)


async def build_my_bookings_keyboard(
    formatted_rows: list[tuple[str, int]],
    upcoming_count: int,
    completed_count: int,
    filter_mode: str,
    page: int,
    lang: str,
    items_per_page: int = 5,
    cancelled_count: int = 0,
    noshow_count: int = 0,
    total_pages: int | None = None,
    current_page: int | None = None,
    role: str = "client",
    master_id: int | None = None,
):
    """Build InlineKeyboardMarkup for the `my_bookings` handler.

    Accepts preformatted_rows (list of (text, booking_id)) so this module
    remains UI-only and does not perform formatting or DB access.
    """
    try:
        # Delegate top/dashboard portion to shared builder and then insert per-booking rows
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
        from bot.app.telegram.common.callbacks import BookingActionCB, pack_cb
        from bot.app.telegram.common.callbacks import NavCB
        from bot.app.services import shared_services

        meta = {
            "mode": filter_mode,
            "page": int(page or 1),
            "total_pages": int(total_pages or 1) if total_pages is not None else 1,
            "upcoming_count": upcoming_count,
            "completed_count": completed_count,
            "cancelled_count": cancelled_count,
            "noshow_count": noshow_count,
            "master_id": int(master_id) if master_id is not None else None,
        }

        # Use UI module's dashboard builder directly (keep UI out of services)
        try:
            from bot.app.telegram.client.client_keyboards import build_bookings_dashboard_kb
            # dashboard builder accepts role so tabs/callbacks are typed per role
            dashboard_kb = build_bookings_dashboard_kb(role, meta, lang=lang)
        except Exception:
            # Fallback: return a minimal back-only keyboard when UI module isn't available
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            from bot.app.telegram.common.callbacks import pack_cb, NavCB
            dashboard_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))]])
        # Extract existing rows from dashboard (list of lists)
        existing_rows: list[list[InlineKeyboardButton]] = []
        try:
            existing_rows = list(getattr(dashboard_kb, "inline_keyboard", []) or [])
        except Exception:
            existing_rows = []

        # If there is a Back row at the end (usual case), keep it to append after booking rows
        back_row = None
        if existing_rows:
            back_row = existing_rows[-1]
            top_rows = existing_rows[:-1]
        else:
            top_rows = []

        # Build per-booking rows
        booking_rows: list[list[InlineKeyboardButton]] = []
        # Choose per-row action depending on role so master/admin get detail card
        if str(role).lower() == "master":
            row_act = "master_detail"
        else:
            # client and admin default to the client-detail handler 'details'
            row_act = "details"

        for row in formatted_rows:
            try:
                # Expect callers to pass pre-formatted rows (text, booking_id) so
                # this UI builder does not perform formatting or DB access.
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    text, bid = row[0], row[1]
                else:
                    # Backward compatibility: fall back to a safe placeholder
                    text, bid = ("—", None)
            except Exception:
                text, bid = ("—", None)
            if bid is None:
                continue
            booking_rows.append([
                InlineKeyboardButton(text=text, callback_data=pack_cb(BookingActionCB, act=row_act, booking_id=int(bid)))
            ])

        final_rows: list[list[InlineKeyboardButton]] = []
        final_rows.extend(top_rows)
        final_rows.extend(booking_rows)
        if back_row is not None:
            final_rows.append(back_row)
        else:
            # ensure there is at least a back button
            try:
                from bot.app.telegram.common.callbacks import pack_cb as _pack
                final_rows.append([InlineKeyboardButton(text=t("back", lang), callback_data=_pack(NavCB, act="back"))])
            except Exception:
                pass

        return InlineKeyboardMarkup(inline_keyboard=final_rows)
    except Exception:
        logger.exception("Failed to build my_bookings keyboard")
        # Fallback: minimal keyboard
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from bot.app.telegram.common.callbacks import pack_cb, NavCB
        return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Back", callback_data=pack_cb(NavCB, act="back"))]])
    # end build_my_bookings_keyboard


# format_booking_details_text moved to bot.app.services.client_services


def build_booking_card_kb(data: dict | Any, booking_id: int, role: str = "client", lang: str | None = None):
    """Build InlineKeyboardMarkup for a booking card (moved from services)."""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from bot.app.telegram.common.callbacks import pack_cb, BookingActionCB, RescheduleCB, NavCB
    from bot.app.translations import tr as _tr

    kb = InlineKeyboardBuilder()
    # Determine read-only
    try:
        s_val = getattr(data, 'status', None) if hasattr(data, 'status') else (data.get('status') if isinstance(data, dict) else None)
        if s_val is None and getattr(data, 'raw', None) and isinstance(getattr(data, 'raw', None), dict):
            s_val = getattr(data, 'raw', {}).get('status')
        s_norm = str(s_val).lower() if s_val is not None else None
        terminal_statuses = {"cancelled", "done", "no_show", "expired"}
        read_only = bool(s_norm in terminal_statuses)
    except Exception:
        read_only = False

    r = str(role).lower() if role else "client"
    if not read_only:
        if r == "client":
            try:
                can_reschedule = getattr(data, 'can_reschedule', False) if hasattr(data, 'can_reschedule') else data.get('can_reschedule', False)
                can_cancel = getattr(data, 'can_cancel', False) if hasattr(data, 'can_cancel') else data.get('can_cancel', False)
                if can_reschedule:
                    master_id = None
                    try:
                        raw_mid = None
                        if hasattr(data, 'raw') and isinstance(getattr(data, 'raw'), dict):
                            raw_mid = getattr(data, 'raw', {}).get('master_id')
                        elif isinstance(data, dict):
                            raw_mid = data.get('master_id')
                        master_id = int(raw_mid) if raw_mid is not None else None
                    except Exception:
                        master_id = None
                    kb.button(text=_tr("reschedule", lang=lang), callback_data=pack_cb(RescheduleCB, action="start", booking_id=int(booking_id), master_id=master_id))
                if can_cancel:
                    kb.button(text=_tr("cancel", lang=lang), callback_data=pack_cb(BookingActionCB, act="cancel_confirm", booking_id=int(booking_id)))
            except Exception:
                pass
        else:
            try:
                kb.button(text=_tr("booking_mark_done_button", lang=lang), callback_data=pack_cb(BookingActionCB, act="mark_done", booking_id=int(booking_id)))
                kb.button(text=_tr("booking_mark_noshow_button", lang=lang), callback_data=pack_cb(BookingActionCB, act="mark_noshow", booking_id=int(booking_id)))
                kb.button(text=_tr("booking_client_history_button", lang=lang), callback_data=pack_cb(BookingActionCB, act="client_history", booking_id=int(booking_id)))
                # Determine whether a client note already exists; adjust button label accordingly
                try:
                    note = None
                    if hasattr(data, 'raw') and isinstance(getattr(data, 'raw'), dict):
                        note = getattr(data, 'raw', {}).get("note")
                    elif isinstance(data, dict):
                        note = data.get("note")
                    if note and isinstance(note, str) and note.strip():
                        add_note_label = _tr("booking_edit_note_button", lang=lang)
                    else:
                        add_note_label = _tr("booking_add_note_button", lang=lang)
                    kb.button(text=add_note_label, callback_data=pack_cb(BookingActionCB, act="add_note", booking_id=int(booking_id)))
                    if note and isinstance(note, str) and len(note) > 120:
                        kb.button(text=_tr("show_full_note_button", lang=lang), callback_data=pack_cb(BookingActionCB, act="show_full_note", booking_id=int(booking_id)))
                except Exception:
                    # Fallback to generic add note label
                    try:
                        kb.button(text=_tr("booking_add_note_button", lang=lang), callback_data=pack_cb(BookingActionCB, act="add_note", booking_id=int(booking_id)))
                    except Exception:
                        pass
                can_cancel = getattr(data, 'can_cancel', False) if hasattr(data, 'can_cancel') else data.get('can_cancel', False)
                if can_cancel:
                    kb.button(text=_tr("cancel", lang=lang), callback_data=pack_cb(BookingActionCB, act="cancel_confirm", booking_id=int(booking_id)))
            except Exception:
                pass
    # For masters viewing a terminal (read-only) booking card we prefer a
    # direct 'menu' button returning them to their master menu instead of a
    # generic Back button which steps one level back. This avoids UI dead-ends
    # after actions like marking a booking done/no-show.
    try:
        if r == "master" and read_only:
            from bot.app.telegram.common.callbacks import MasterMenuCB

            kb.button(text=_tr("menu", lang=lang), callback_data=pack_cb(MasterMenuCB, act="menu"))
        else:
            kb.button(text=_tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    except Exception:
        # Fallback to a generic back action if anything goes wrong
        kb.button(text=_tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(2, 1)
    return kb.as_markup()


__all__ = [
    "get_calendar_keyboard",
    "get_service_menu",
    "get_service_menu_multi",  # Добавляем новую функцию
    "get_master_keyboard",
    "build_rating_keyboard",
    "get_main_menu",
    "get_payment_keyboard",
    "home_kb",
]