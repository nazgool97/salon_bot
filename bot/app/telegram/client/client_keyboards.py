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
)
from bot.app.telegram.common.callbacks import MasterMenuCB, NavCB, ClientMenuCB, RatingCB
from bot.app.telegram.common.callbacks import MasterProfileCB, MasterServicesCB, MastersListCB
from bot.app.telegram.common.callbacks import PayCB
from bot.app.telegram.common.roles import is_admin, is_master
from bot.app.domain.models import Master, MasterService, Service, MasterProfile
from bot.app.services.shared_services import safe_get_locale as _get_locale, default_language, format_date, format_money_cents

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
    try:
        from bot.app.translations import t
        lang = default_language()
        back_txt = t("back", lang)
    except Exception:
        back_txt = "⬅️ Назад"
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
        try:
            if isinstance(add_back, str):
                back_text = add_back
            else:
                from bot.app.translations import tr as _tr
                use_lang = lang or default_language()
                back_text = _tr("back", lang=use_lang)
        except Exception:
            back_text = "⬅️ Назад"
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
        label = slot.strftime("%H:%M")
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

    try:
        back_text = t("back", lang)
    except Exception:
        back_text = "⬅️ Назад"
    builder.button(text=back_text, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(3, 3, 3, 1, 1)
    return builder.as_markup()

logger = logging.getLogger(__name__)

# Стандартные русские месяцы (fallback, если i18n недоступен)
_MONTH_NAMES = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]


def _localize(key: str, lang: str, fallback: str) -> str:
    try:
        val = t(key, lang)
        return fallback if val == key else val
    except Exception:
        return fallback


async def _resolve_lang(user_id: int | None = None) -> str:
    if user_id:
        try:
            return await _get_locale(user_id)
        except Exception:
            logger.exception("get_masters_catalog_keyboard: failed to build pagination row")
    return default_language()


def _default_currency() -> str:
    # UI-only: do not access DB; use environment or default
    import os
    return os.getenv("CURRENCY", "UAH")


@runtime_checkable
class _HasMasterAttrs(Protocol):
    """Протокол для объектов мастера с необходимыми атрибутами."""
    name: str
    telegram_id: int


# Note: _allowed_weekdays has been removed — handlers must prefetch allowed_weekdays
# and pass them into keyboard builders. Keeping keyboard factories UI-only.


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
      full -> 🗓️ (fully booked / no slots)
    """
    from datetime import date as _date
    row: list[InlineKeyboardButton] = []
    for day, state in week_states:
        if state == 'empty':
            row.append(InlineKeyboardButton(text=" ", callback_data="dummy"))
            continue
        if state == 'past':
            row.append(InlineKeyboardButton(text="✖", callback_data="dummy"))
            continue
        if state == 'not_allowed':
            row.append(InlineKeyboardButton(text="—", callback_data="dummy"))
            continue
        if state == 'available':
            try:
                day_date = _date(year, month, day)
                cb = pack_cb(DateCB, service_id=service_id, master_id=master_id, date=str(day_date))
                row.append(InlineKeyboardButton(text=str(day), callback_data=cb))
            except Exception:
                row.append(InlineKeyboardButton(text="🗓️", callback_data="dummy"))
            continue
        # full / fallback
        row.append(InlineKeyboardButton(text="🗓️", callback_data="dummy"))
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
        InlineKeyboardButton(text=month_label, callback_data="dummy"),
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
    max_date = datetime.now().date() + timedelta(days=int(max_days))

    if year is None or month is None:
        now = datetime.now()
        year, month = now.year, now.month
    if date(year, month, 1) > max_date:
        logger.warning("Попытка открыть календарь для слишком далекого будущего: %d-%d", year, month)
        year, month = max_date.year, max_date.month

    today = date.today()
    buttons: list[list[InlineKeyboardButton]] = []

    # Заголовок месяца с локализацией
    try:
        months = _tr("month_names_full", lang) if _tr is not None else None
        if isinstance(months, list) and months:
            month_label = f"{months[month - 1]} {year}"
        else:
            month_label = f"{_MONTH_NAMES[month - 1]} {year}"
    except Exception:
        month_label = f"{_MONTH_NAMES[month - 1]} {year}"

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
    buttons.append([InlineKeyboardButton(text=n, callback_data="dummy") for n in wd])

    # Use precomputed day_states provided by handler
    if day_states is None:
        # As a safe fallback, render empty calendar rows
        day_states = []

    for week_states in day_states:
        try:
            buttons.append(_build_week_row_states(service_id, master_id, year, month, week_states))
        except Exception as e:
            logger.exception("Error building week row from states: %s", e)
            buttons.append([InlineKeyboardButton(text=(_t("error", lang) if _t is not None else "Ошибка"), callback_data="dummy")])

    # Кнопка назад
    try:
        back_txt = _t("back", lang) if _t is not None else "⬅️ Назад"
    except Exception:
        back_txt = "⬅️ Назад"
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
    cont_txt = _localize("continue_button", lang, "✅ Продовжити")
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

    if masters_list:
        for master in masters_list:
            name = getattr(master, "name", str(getattr(master, "telegram_id", "?")))
            mid = int(getattr(master, "telegram_id", 0))

            # Кнопка 1: Просмотр профиля мастера
            builder.button(
                text=f"👤 {name}",
                callback_data=pack_cb(MasterProfileCB, master_id=mid, service_id=service_id),
            )

            # Кнопка 2: Сразу к записи (пропустить профиль)
            builder.button(
                text="🗓️ Запис",
                callback_data=pack_cb(MasterSelectCB, master_id=mid, service_id=service_id),
            )

        # расположить кнопки мастеров по 2 в ряд (Профіль | Запис)
        builder.adjust(2)
    else:
        builder.button(
            text=_localize("no_masters", lang, "❌ Нема доступних майстрів"),
            callback_data="no_masters"
        )

    back_txt = _localize("back", lang, "⬅️ Назад")

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
        for m in masters_list:
            try:
                if isinstance(m, tuple) and len(m) >= 2:
                    mid = int(m[0])
                    name = str(m[1])
                else:
                    mid = int(getattr(m, "telegram_id", 0))
                    name = str(getattr(m, "name", mid))
            except Exception:
                mid = int(getattr(m, "telegram_id", 0))
                name = str(getattr(m, "name", mid))

            # Profile button
            builder.button(
                text=f"👤 {name}",
                callback_data=pack_cb(MasterProfileCB, master_id=mid, service_id=""),
            )
            # Services for master
            services_label = _localize("services_for_master", lang, "🛠️ Послуги")
            builder.button(
                text=services_label,
                callback_data=pack_cb(MasterServicesCB, master_id=mid),
            )
        builder.adjust(2)
    else:
        builder.button(text=_localize("no_masters", lang, "❌ Нема доступних майстрів"), callback_data="no_masters")

    back_txt = _localize("back", lang, "⬅️ Назад")
    builder.row(InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back")))
    return builder.as_markup()



# Note: `get_back_button` is provided by `bot.app.services.shared_services`.
# Handlers should import it from there. This module previously contained a
# duplicate fallback implementation; it was removed to avoid duplication.


STAR_EMOJI = {1: "⭐", 2: "⭐⭐", 3: "⭐⭐⭐", 4: "⭐⭐⭐⭐", 5: "⭐⭐⭐⭐⭐"}


def build_rating_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для выбора рейтинга бронирования."""
    lang = default_language()
    builder = InlineKeyboardBuilder()
    from typing import cast, Any
    builder.row(*[
        InlineKeyboardButton(
            text=STAR_EMOJI[i],
            callback_data=pack_cb(RatingCB, booking_id=int(booking_id), rating=int(i)),
        )
        for i in range(1, 6)
    ])
    skip_txt = _localize("skip", lang, "Пропустити")
    builder.button(text=skip_txt, callback_data=pack_cb(NavCB, act="root"))
    logger.debug("Клавиатура рейтинга сгенерирована для брони %d", booking_id)
    return builder.as_markup()


async def is_online_payment_available() -> bool:
    """Determines if online payments are available for the client UI.

    Uses centralized logic from shared_services to ensure the admin toggle and
    provider token are both respected.
    """
    try:
        from bot.app.services.shared_services import is_online_payments_available as _avail
        return bool(await _avail())
    except Exception as e:
        logger.warning("Проверка онлайн-оплаты не удалась: %s", e)
        return False


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

        builder.button(text=_t("book_button", "Записатися"), callback_data=pack_cb(ClientMenuCB, act="booking_service"))
        # New entry: browse by master
        builder.button(text=_t("masters_button", "Наші майстри"), callback_data=pack_cb(ClientMenuCB, act="masters_list"))
        from bot.app.telegram.common.callbacks import MyBookingsCB
        # Default entry to 'Мої записи' should show upcoming bookings only.
        builder.button(text=_t("my_bookings_button", "Мої записи"), callback_data=pack_cb(MyBookingsCB, mode="upcoming"))
        builder.button(text=_t("contacts_button", "Контакти"), callback_data=pack_cb(ClientMenuCB, act="contacts"))

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
        try:
            upcoming_label = tr("upcoming", lang=lang) or "Upcoming"
        except Exception:
            upcoming_label = "Upcoming"
        try:
            done_label = tr("master_completed", lang=lang) or "Done"
        except Exception:
            done_label = "Done"
        try:
            cancelled_label = tr("cancelled", lang=lang) or "Cancelled"
        except Exception:
            cancelled_label = "Cancelled"
        try:
            noshow_label = tr("no_show", lang=lang) or "No-show"
        except Exception:
            noshow_label = "No-show"

        mode = (meta.get("mode") if meta else None) or "upcoming"

        def mark(lbl: str, tab: str) -> str:
            return f"✔️ {lbl}" if tab == mode else lbl

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
            bottom_row.append(InlineKeyboardButton(text=tr("back", lang=lang), callback_data=back_cb))
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
                from bot.app.telegram.common.callbacks import AdminMenuCB

                back_cb = pack_cb(AdminMenuCB, act="panel")
            else:
                if str(role).lower() == "client" and mode == "completed":
                    back_cb = pack_cb(RoleCB, mode="upcoming", page=1)
                else:
                    back_cb = pack_cb(NavCB, act="root")
        except Exception:
            back_cb = pack_cb(NavCB, act="role_root")
        kb.row(InlineKeyboardButton(text=tr("back", lang=lang), callback_data=back_cb))
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
            from bot.app.translations import tr
            return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr('back', lang=lang), callback_data=pack_cb(NavCB, act="role_root"))]])
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
    currency = _default_currency()
    _t = lambda key, default: _localize(key, lang, default)
    # Use canonical builder/formatter to produce booking summary text.
    # If the canonical builder is unavailable or fails, fall back to a
    # UI-only header built from the provided `booking`, `service_name`,
    # `master_name` and `date`. DO NOT import DB/domain models here.
    try:
        from bot.app.services.client_services import build_booking_details
        from bot.app.services.shared_services import format_booking_details_text
        bd = await build_booking_details(
            booking,
            service_name=service_name,
            master_name=master_name,
            user_id=user_id,
            date=date,
            lang=lang,
        )
        header = format_booking_details_text(bd, lang)
        # Append payment prompt
        choose_txt = _t("choose_payment_label", "Оберіть спосіб оплати")
        header = header + "\n\n" + choose_txt + ":"
    except Exception:
        # UI-only fallback: build a minimal header without DB access.
        try:
            # Prefer explicitly provided values, then object attributes or mapping keys.
            from collections.abc import Mapping
            if isinstance(booking, Mapping):
                svc = service_name or booking.get("service_name") or booking.get("service")
                master = master_name or booking.get("master_name") or booking.get("master") or "—"
            else:
                svc = service_name or getattr(booking, "service_name", None) or getattr(booking, "service", None)
                master = master_name or getattr(booking, "master_name", None) or getattr(booking, "master", None) or "—"
            # Try to obtain a human-friendly date string without timezone conversions.
            try:
                booking_date = date or format_date(getattr(booking, "starts_at", None) or datetime.now(), fmt="%d.%m.%Y")
            except Exception:
                booking_date = date or "—"
            # Try to display a price if present on the booking object; avoid DB lookups.
            price_cents = None
            from collections.abc import Mapping
            if isinstance(booking, Mapping):
                for key in ("final_price_cents", "original_price_cents", "price_cents"):
                    val = booking.get(key)
                    if isinstance(val, int):
                        price_cents = val
                        break
            else:
                for attr in ("final_price_cents", "original_price_cents", "price_cents"):
                    val = getattr(booking, attr, None)
                    if isinstance(val, int):
                        price_cents = val
                        break
            # Try to format using shared helper if available, otherwise fall
            # back to a simple human-readable formatting.
            if price_cents is not None:
                try:
                    human_price = format_money_cents(price_cents, currency)
                except Exception:
                    try:
                        human_price = f"{price_cents/100:.2f} {currency}"
                    except Exception:
                        human_price = "—"
            else:
                human_price = "—"

            header = (
                f"<b>{_t('booking_label', 'Запис')}</b>\n"
                f"{_t('service_label', 'Послуга')}: <b>{svc or '—'}</b>\n"
                f"{_t('master_label', 'Майстер')}: {master}\n"
                f"{_t('date_label', 'Дата')}: <b>{booking_date}</b>\n"
                f"{_t('amount_label', 'Сума до оплати')}: {human_price}\n\n"
                f"{_t('choose_payment_label', 'Оберіть спосіб оплати')}:"
            )
        except Exception:
            # As last resort keep a tiny header
            header = f"<b>{_t('booking_label', 'Запис')}</b>"

    # Determine canonical booking id for callbacks. `booking` may be:
    # - an ORM Booking instance (has .id)
    # - a mapping/dict with 'id' or 'booking_id'
    # - an int booking id
    booking_id_val = None
    try:
        if isinstance(booking, int):
            booking_id_val = int(booking)
        elif isinstance(booking, dict):
            booking_id_val = int(booking.get("id") or booking.get("booking_id") or 0)
        else:
            booking_id_val = int(getattr(booking, "id", None) or getattr(booking, "booking_id", None) or 0)
    except Exception:
        booking_id_val = 0

    builder = InlineKeyboardBuilder()
    if await is_online_payment_available():
        builder.button(
            text=_t("online_payment_button", "💳 Онлайн-оплата"),
            callback_data=pack_cb(PayCB, action="prep_online", booking_id=booking_id_val),
        )
    builder.button(
        text=_t("cash_button", " Готівка"),
        callback_data=pack_cb(PayCB, action="prep_cash", booking_id=booking_id_val),
    )
    # Используем специальный callback, который отменит бронь
    from bot.app.telegram.common.callbacks import BookingActionCB 
    builder.button(
        text=_t("back", "⬅️ Назад"),
        callback_data=pack_cb(BookingActionCB, act="cancel_reservation", booking_id=booking_id_val)
    )
    builder.button(
        text=_t("menu", "🏠 Меню"),
        callback_data=pack_cb(NavCB, act="root"),
    )
    builder.adjust(1)  # Каждая кнопка в отдельной строке
    logger.debug("Клавиатура оплаты сгенерирована для брони %s", getattr(booking, "id", 0))
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


# format_master_profile_text moved to master_keyboards


# format_booking_list_item moved to bot.app.services.client_services (formatting belongs in services)


async def build_my_bookings_keyboard(formatted_rows: list[tuple[str, int]], upcoming_count: int, completed_count: int, filter_mode: str, page: int, lang: str, items_per_page: int = 5, cancelled_count: int = 0, noshow_count: int = 0, total_pages: int | None = None, current_page: int | None = None, role: str = "client"):
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
            dashboard_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=("⬅️ Назад"), callback_data=pack_cb(NavCB, act="back"))]])
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
                final_rows.append([InlineKeyboardButton(text=("⬅️ Назад"), callback_data=_pack(NavCB, act="back"))])
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