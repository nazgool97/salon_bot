from __future__ import annotations
import importlib
import logging
import os
from calendar import monthcalendar
from datetime import date, datetime, timedelta
from typing import Protocol, runtime_checkable, Sequence, cast, Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from bot.app.telegram.common.callbacks import (
    pack_cb,
    CalendarCB,
    DateCB,
    MasterSelectCB,
    ServiceSelectCB,
)
from bot.app.telegram.common.callbacks import MasterMenuCB, NavCB, ClientMenuCB, RatingCB
from bot.app.telegram.common.callbacks import PayCB
from bot.app.services.client_services import get_available_time_slots, get_available_days_for_month
import asyncio
from bot.app.core.db import get_session
from bot.app.telegram.common.roles import is_admin, is_master
from bot.app.domain.models import Master, MasterService, Service, MasterProfile
import bot.config as cfg
from bot.app.services.shared_services import safe_get_locale as _get_locale, format_date, services_cache
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from bot.app.translations import t
from bot.app.telegram.common.navigation import nav_push  # Добавляем импорт
from bot.app.telegram.common.ui_fail_safe import safe_edit  # Добавляем импорт

logger = logging.getLogger(__name__)

# Стандартные русские месяцы (fallback, если i18n недоступен)
_MONTH_NAMES = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]

# Загружаем конфигурацию с обработкой ошибок
try:
    _cfg_mod = importlib.import_module("bot.config")
except ImportError as e:
    logger.error("Не удалось загрузить bot.config: %s", e)
    _cfg_mod = None


@runtime_checkable
class _HasMasterAttrs(Protocol):
    """Протокол для объектов мастера с необходимыми атрибутами."""
    name: str
    telegram_id: int


async def _allowed_weekdays(master_id: int) -> list[int]:
    """Дни недели, указанные мастером в расписании (из MasterProfile.bio.schedule).

    Возвращает отсортированный список индексов дней (0=Пн..6=Нд), где есть хотя бы одно окно.
    Если расписание отсутствует — ничего не разрешаем (пусть мастер задаст через "Розклад").
    """
    logger.debug("Получение дней недели из расписания мастера %s", master_id)
    try:
        async with get_session() as session:
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_id))
            if not prof or not getattr(prof, "bio", None):
                return []


            # no-op here; filtering lives at module level via _get_filtered_services
                return services
            import json
            cfg_obj = json.loads(prof.bio or "{}") or {}
            sched = cfg_obj.get("schedule", {}) or {}
            days = [int(k) for k, v in sched.items() if isinstance(v, list) and len(v) > 0]
            return sorted([d for d in days if 0 <= d <= 6])
    except Exception as e:
        logger.warning("Не удалось получить расписание для мастера %s: %s", master_id, e)
        return []


async def _build_week_row(
    service_id: str,
    master_id: int,
    year: int,
    month: int,
    week: list[int],
    today: date,
    service_duration_min: int = 60,
    allowed_weekdays: list[int] | None = None,
    available_days: set[int] | None = None,
) -> list[InlineKeyboardButton]:
    """
    Строит ряд кнопок для одной недели календаря.
    - Пустые ячейки (0) → пробел.
    - Прошедшие дни → ✖ (прошлое).
    - Дни, когда мастер не работает (выходной) → — (минус).
    - Будущие дни с доступными слотами → {day} (кликабельно).
    - Дни без доступных слотов (полностью заняты) → 🗓️.
    """
    row: list[InlineKeyboardButton] = []

    # allowed_weekdays should be provided by the caller to avoid repeated DB calls
    if allowed_weekdays is None:
        # defensive fallback (should be rare) — compute once
        allowed_weekdays = await _allowed_weekdays(master_id)

    for day in week:
        if day == 0:
            row.append(InlineKeyboardButton(text=" ", callback_data="dummy"))
            continue

        day_date = date(year, month, day)

        # ✖ прошедшие дни
        if day_date < today:
            row.append(InlineKeyboardButton(text="✖", callback_data="dummy"))
            continue

        # Дни, когда мастер не работает (не входят в allowed_weekdays) → помечаем знаком «—»
        if day_date.weekday() not in (allowed_weekdays or []):
            row.append(InlineKeyboardButton(text="—", callback_data="dummy"))
            continue

        # Проверяем наличие доступных слотов при помощи заранее загруженного набора дней
        has_slots = False
        if available_days is not None:
            has_slots = day in available_days
        else:
            # Fallback: conservative approach — mark as no slots if we couldn't prefetch
            has_slots = False

        if has_slots:
            cb = pack_cb(DateCB, service_id=service_id, master_id=master_id, date=str(day_date))
            row.append(InlineKeyboardButton(text=str(day), callback_data=cb))
        else:
            # Нет доступных слотов — показываем иконку «полностью забронировано»
            row.append(InlineKeyboardButton(text="🗓️", callback_data="dummy"))

    return row

def _month_nav(service_id: str, master_id: int, year: int, month: int) -> list[InlineKeyboardButton]:
    """Создает кнопки навигации по месяцам в календаре."""
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    return [
        InlineKeyboardButton(
            text="◀️",
            callback_data=pack_cb(CalendarCB, service_id=service_id, master_id=master_id, year=prev_year, month=prev_month),
        ),
        InlineKeyboardButton(text=f"{_MONTH_NAMES[month - 1]} {year}", callback_data="dummy"),
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
) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру календаря с доступными датами."""
    # Определяем язык пользователя
    try:
        from bot.app.translations import t as _t
        if user_id is None:
            # fallback к общему языку
            lang = getattr(_cfg_mod, "SETTINGS", {}).get("language", "uk") if _cfg_mod else "uk"
        else:
            # Use centralized safe_get_locale helper (aliased above)
            lang = await _get_locale(user_id)
    except Exception:
        _t = None  # type: ignore
        lang = getattr(_cfg_mod, "SETTINGS", {}).get("language", "uk") if _cfg_mod else "uk"

    # Ограничение на выбор дат (конфигурируемое) — всегда считаем, т.к. ниже используется
    try:
        max_days = int(getattr(cfg, "SETTINGS", {}).get("calendar_max_days_ahead", 365))
    except Exception:
        max_days = 365
    max_date = datetime.now().date() + timedelta(days=max_days)

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
        months = (_t("month_names", lang) if _t else None)  # type: ignore
        month_label = f"{(months or _MONTH_NAMES)[month - 1]} {year}"
    except Exception:
        month_label = f"{_MONTH_NAMES[month - 1]} {year}"

    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    buttons.append([
        InlineKeyboardButton(
            text="◀️",
            callback_data=pack_cb(CalendarCB, service_id=service_id, master_id=master_id, year=prev_year, month=prev_month),
        ),
        InlineKeyboardButton(text=month_label, callback_data="dummy"),
        InlineKeyboardButton(
            text="▶️",
            callback_data=pack_cb(CalendarCB, service_id=service_id, master_id=master_id, year=next_year, month=next_month),
        ),
    ])
    # Недели с локализованными короткими днями недели
    try:
        weekdays = (_t("weekday_short", lang) if _t else None)  # type: ignore
        wd = weekdays or ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
    except Exception:
        wd = ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")
    buttons.append([InlineKeyboardButton(text=n, callback_data="dummy") for n in wd])

    try:
        # Batch-load available days for the month to avoid per-day DB queries
        try:
            available_days = await get_available_days_for_month(master_id, year, month, service_duration_min)
        except Exception:
            available_days = set()

        # Load allowed weekdays once to avoid repeated reads of MasterProfile
        try:
            allowed_weekdays = await _allowed_weekdays(master_id)
        except Exception:
            allowed_weekdays = []

        for week in monthcalendar(year, month):
            buttons.append(
                await _build_week_row(
                    service_id,
                    master_id,
                    year,
                    month,
                    week,
                    today,
                    service_duration_min,
                    allowed_weekdays,
                    available_days,
                )
            )
        logger.info(
            "Календарь сгенерирован для service_id=%s, master_id=%s, year=%d, month=%d",
            service_id, master_id, year, month
        )
    except Exception as e:
        logger.error(
            "Ошибка построения календаря для service_id=%s, master_id=%s, year=%d, month=%d: %s",
            service_id, master_id, year, month, e
        )
        # Локализованная ошибка
        try:
            err_txt = _t("error", lang) if _t else "Ошибка"
        except Exception:
            err_txt = "Ошибка"
        buttons.append([InlineKeyboardButton(text=err_txt, callback_data="dummy")])

    # Кнопка назад
    try:
        back_txt = _t("back", lang) if _t else "⬅️ Назад"
    except Exception:
        back_txt = "⬅️ Назад"
    buttons.append([InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back"))])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def _get_filtered_services() -> dict[str, str]:
    """Load services from configuration module and filter to those that have at least one MasterService.

    Returns mapping service_id -> name.
    """
    services: dict[str, str] = {}
    try:
        # Prefer DB-backed services via shared_services.services_cache
        services = await services_cache()
        if not isinstance(services, dict):
            services = {}
    except Exception as e:
        logger.error("Ошибка получения услуг из БД/кэша: %s", e)
        # Fallback to config provider if available
        if _cfg_mod:
            _get_services = getattr(_cfg_mod, "get_services", None)
            if callable(_get_services):
                try:
                    services_raw = await _get_services() if asyncio.iscoroutinefunction(_get_services) else _get_services()
                    if isinstance(services_raw, dict):
                        services = {str(k): str(v) for k, v in services_raw.items()}
                except Exception as e:
                    logger.error("Ошибка получения услуг из конфигурации: %s", e)

    # Filter by MasterService presence
    try:
        if services:
            wanted_ids = set(services.keys())
            async with get_session() as session:
                stmt = select(MasterService.service_id).where(MasterService.service_id.in_(wanted_ids)).distinct()
                result = await session.execute(stmt)
                has_masters = {row[0] for row in result.all()}
            # If we found MasterService links, keep only services that have masters.
            # If none were found, don't filter everything out — return configured services
            # so multi-select menus remain usable when DB linking table is empty.
            if has_masters:
                services = {sid: name for sid, name in services.items() if sid in has_masters}
            else:
                logger.debug("_get_filtered_services: no MasterService links found, returning configured services")
    except Exception as e:
        logger.warning("Не удалось отфильтровать услуги по MasterService: %s", e)
    return services


async def get_service_menu(include_back: bool = True) -> InlineKeyboardMarkup:
    """Генерирует меню выбора услуг из конфигурации."""
    services = await _get_filtered_services()

    builder = InlineKeyboardBuilder()
    texts_map = getattr(_cfg_mod, "TEXTS", {})
    for service_id, name in services.items():
        builder.button(
            text=name,
            callback_data=pack_cb(ServiceSelectCB, service_id=service_id),
        )
    # Extra entry for multi-select flow
    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        multi_txt = t("multi_services_button", lang)
    except Exception:
        multi_txt = texts_map.get("multi_services_button", "🧰 Кілька послуг")
    builder.button(text=multi_txt, callback_data=pack_cb(ClientMenuCB, act="services_multi"))

    if include_back:
        try:
            from bot.app.translations import t
            lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
            back_txt = t("back", lang)
        except Exception:
            back_txt = texts_map.get("back_button", "⬅️ Назад")
    builder.button(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
    builder.adjust(1)
    logger.debug("Меню услуг сгенерировано, количество услуг: %d", len(services))
    return builder.as_markup()


async def get_service_menu_multi(selected: set[str]) -> InlineKeyboardMarkup:
    """Генерирует меню для множественного выбора услуг с отметками выбранных."""
    services = await _get_filtered_services()

    builder = InlineKeyboardBuilder()
    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        cont_txt = t("continue_button", lang)
        back_txt = t("back", lang)
    except Exception:
        cont_txt = "✅ Продовжити"
        back_txt = "⬅️ Назад"

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


async def get_master_keyboard(service_id: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    masters: list[_HasMasterAttrs] = []
    try:
        async with get_session() as session:
            stmt = (
                select(Master)
                .join(MasterService, MasterService.master_telegram_id == Master.telegram_id)
                .where(MasterService.service_id == service_id)
            )
            result = await session.execute(stmt)
            masters = cast(list[_HasMasterAttrs], list(result.scalars().all()))
        logger.info("Получено %d мастеров для услуги %s", len(masters), service_id)
    except Exception as e:
        logger.error("Ошибка при получении мастеров для услуги %s: %s", service_id, e)

    texts_map = getattr(_cfg_mod, "TEXTS", {})

    if masters:
        from bot.app.telegram.common.callbacks import MasterProfileCB, MasterSelectCB
        for master in masters:
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
            text=texts_map.get("no_masters", "❌ Нема доступних майстрів"),
            callback_data="no_masters"
        )

    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        back_txt = t("back", lang)
    except Exception:
        back_txt = texts_map.get("back_button", "⬅️ Назад")

    # кнопка "Назад" отдельной строкой в самом низу
    builder.row(InlineKeyboardButton(text=back_txt, callback_data=pack_cb(NavCB, act="back")))

    return builder.as_markup()



def get_back_button() -> InlineKeyboardMarkup:
    """Одна универсальная кнопка 'Назад' для всего бота."""
    texts_map = getattr(_cfg_mod, "TEXTS", {})
    builder = InlineKeyboardBuilder()
    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        back_txt = t("back", lang)
    except Exception:
        back_txt = texts_map.get("back_button", "⬅️ Назад")
    builder.button(text=back_txt, callback_data=pack_cb(NavCB, act="back"))
    return builder.as_markup()


STAR_EMOJI = {1: "⭐", 2: "⭐⭐", 3: "⭐⭐⭐", 4: "⭐⭐⭐⭐", 5: "⭐⭐⭐⭐⭐"}


def build_rating_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    """Генерирует клавиатуру для выбора рейтинга бронирования."""
    texts_map = getattr(_cfg_mod, "TEXTS", {})
    builder = InlineKeyboardBuilder()
    from typing import cast, Any
    builder.row(*[
        InlineKeyboardButton(
            text=STAR_EMOJI[i],
            callback_data=pack_cb(RatingCB, booking_id=int(booking_id), rating=int(i)),
        )
        for i in range(1, 6)
    ])
    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        skip_txt = t("skip", lang)
    except Exception:
        skip_txt = texts_map.get("skip_button", "Пропустити")
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
        return bool(_avail())
    except Exception as e:
        logger.warning("Проверка онлайн-оплаты не удалась: %s", e)
        return False


async def get_main_menu(telegram_id: int) -> InlineKeyboardMarkup:
    """Генерирует главное меню с учетом прав администратора и мастера."""
    logger.debug("Генерация главного меню для telegram_id=%s", telegram_id)
    try:
        texts_map = getattr(_cfg_mod, "TEXTS", {})
        admin_flag = await is_admin(telegram_id)
        master_flag = await is_master(telegram_id)
        logger.debug("is_admin(%s) -> %s, is_master(%s) -> %s", telegram_id, admin_flag, telegram_id, master_flag)

        builder = InlineKeyboardBuilder()
        try:
            from bot.app.translations import t
            # Use centralized safe_get_locale aliased as _get_locale at module level
            lang = await _get_locale(telegram_id)
        except Exception:
            t = None  # type: ignore
            lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        _t = (lambda k, d: t(k, lang) if t else texts_map.get(k, d))
        builder.button(text=_t("book_button", "Записатися"), callback_data=pack_cb(ClientMenuCB, act="booking_service"))
        from bot.app.telegram.common.callbacks import MyBookingsCB
        builder.button(text=_t("my_bookings_button", "Мої записи"), callback_data=pack_cb(MyBookingsCB, mode="all"))
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


async def get_payment_keyboard(
    booking: object,
    service_name: str,
    master_name: str | None,
    user_id: int,
    date: str | None = None,
) -> tuple[str, InlineKeyboardMarkup]:
    """Генерирует клавиатуру выбора оплаты и текст заголовка."""
    texts_map = getattr(_cfg_mod, "TEXTS", {})
    try:
        from bot.app.translations import t
        lang = await _get_locale(user_id)
    except Exception:
        t = None  # type: ignore
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
    # localized text helper (works even if translation function 't' is unavailable)
    _t = (lambda k, d: t(k, lang) if t else texts_map.get(k, d))
    # Use canonical builder/formatter to produce booking summary text
    try:
        from bot.app.services import shared_services
        bd = await shared_services.build_booking_details(booking, service_name=service_name, master_name=master_name, user_id=user_id, date=date, lang=lang)
        header = shared_services.format_booking_details_text(bd, lang)
        # Append payment prompt
        try:
            choose_txt = t("choose_payment_label", lang) if t else texts_map.get("choose_payment_label", "Оберіть спосіб оплати")
        except Exception:
            choose_txt = texts_map.get("choose_payment_label", "Оберіть спосіб оплати")
        header = header + "\n\n" + choose_txt + ":"
    except Exception:
        # Fallback to legacy header when builder fails
        price = None
        for attr in ("final_price_cents", "original_price_cents"):
            val = getattr(booking, attr, None)
            if isinstance(val, int):
                price = val
                break
        if price is None:
            try:
                svc_id = getattr(booking, "service_id", None)
                if svc_id is not None:
                    from bot.app.core.db import get_session
                    from bot.app.domain.models import Service
                    async with get_session() as session:
                        svc = await session.get(Service, svc_id)
                        if svc is not None:
                            price = getattr(svc, "final_price_cents", None) or getattr(svc, "price_cents", None)
            except Exception:
                pass
        human_price = f"{(price or 0)/100:.2f} грн" if price is not None else "—"
        try:
            booking_date = date or format_date(getattr(booking, "starts_at", None) or datetime.now(), fmt="%d.%m.%Y")
        except Exception:
            booking_date = date or "N/A"
        header = (
            f"<b>{_t('booking_label', 'Запис')}</b>\n"
            f"{_t('service_label', 'Послуга')}: <b>{service_name}</b>\n"
            f"{_t('master_label', 'Майстер')}: {master_name or '—'}\n"
            f"{_t('date_label', 'Дата')}: <b>{booking_date}</b>\n"
            f"{_t('amount_label', 'Сума до оплати')}: {human_price}\n\n"
            f"{_t('choose_payment_label', 'Оберіть спосіб оплати')}:"
        )

    builder = InlineKeyboardBuilder()
    if await is_online_payment_available():
        builder.button(
            text=_t("online_payment_button", "💳 Онлайн-оплата"),
            callback_data=pack_cb(PayCB, action="prep_online", booking_id=getattr(booking, 'id', 0)),
        )
    builder.button(
        text=_t("cash_button", " Готівка"),
        callback_data=pack_cb(PayCB, action="prep_cash", booking_id=getattr(booking, 'id', 0)),
    )
    builder.button(
        text=_t("back", "⬅️ Назад"),
        callback_data=pack_cb(NavCB, act="back")
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
    texts_map = getattr(_cfg_mod, "TEXTS", {})
    builder = InlineKeyboardBuilder()
    try:
        from bot.app.translations import t
        lang = _cfg_mod.SETTINGS.get("language", "uk") if _cfg_mod else "uk"
        menu_txt = t("menu", lang)
    except Exception:
        menu_txt = texts_map.get("menu_button", "🏠 Меню")
    builder.button(text=menu_txt, callback_data=pack_cb(NavCB, act="root"))
    logger.debug("Клавиатура возврата в меню сгенерирована")
    return builder.as_markup()


__all__ = [
    "get_calendar_keyboard",
    "get_service_menu",
    "get_master_keyboard",
    "get_back_button",
    "build_rating_keyboard",
    "get_main_menu",
    "get_payment_keyboard",
    "home_kb",
    "get_simple_kb",
]

def get_simple_kb(
    buttons: list[tuple[str, str]],
    cols: int = 1,
    *,
    add_back: bool | str = False,
    back_cb: str = pack_cb(NavCB, act="back"),
    lang: str | None = None,
) -> InlineKeyboardMarkup:
    """Build a simple inline keyboard from (text, callback_data) pairs.

    Args:
        buttons: List of (text, callback) pairs in top-to-bottom order.
        cols: Number of columns to arrange buttons into.
        add_back: If True or str, append a localized back button at the end; when str, it's used as the button text.
        back_cb: Callback data for the back button (default 'global_back').
        lang: Optional language code for localizing the back label when add_back=True.
    """
    builder = InlineKeyboardBuilder()
    for text, data in buttons:
        builder.button(text=text, callback_data=data)
    # Optional back button
    if add_back:
        try:
            if isinstance(add_back, str):
                back_text = add_back
            else:
                from bot.app.translations import tr as _tr
                use_lang = lang or getattr(cfg, "SETTINGS", {}).get("language", "uk")
                back_text = _tr("back", lang=use_lang)
        except Exception:
            back_text = "⬅️ Назад"
        builder.button(text=back_text, callback_data=back_cb)
    try:
        cols = max(1, int(cols))
    except Exception:
        cols = 1
    builder.adjust(cols)
    return builder.as_markup()

__all__ = [
    "get_calendar_keyboard",
    "get_service_menu",
    "get_service_menu_multi",  # Добавляем новую функцию
    "get_master_keyboard",
    "get_back_button",
    "build_rating_keyboard",
    "get_main_menu",
    "get_payment_keyboard",
    "home_kb",
    "get_simple_kb",
]