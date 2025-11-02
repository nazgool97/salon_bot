from __future__ import annotations
import logging
from typing import Any, Mapping, cast

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.app.telegram.client.client_keyboards import get_simple_kb
from bot.app.services.shared_services import (
    is_telegram_payments_enabled,
    get_telegram_provider_token,
    render_stats_overview,
)
from bot.app.services.shared_services import get_setting, get_hold_minutes
from bot.app.translations import t
import bot.config as cfg
from bot.app.telegram.common.callbacks import pack_cb, AdminMenuCB, NavCB
from bot.app.telegram.common.callbacks import (
    BookingsPageCB,
    DelMasterPageCB,
    DelServicePageCB,
)

logger = logging.getLogger(__name__)

def admin_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует главное меню админ-панели.

    Returns:
        InlineKeyboardMarkup с кнопками меню.
    """
    from bot.app.telegram.common.callbacks import AdminMenuCB
    buttons = [
        (t("admin_menu_add_master", lang), pack_cb(AdminMenuCB, act="add_master")),
        (t("admin_menu_delete_master", lang), pack_cb(AdminMenuCB, act="delete_master")),
        (t("admin_menu_add_service", lang), pack_cb(AdminMenuCB, act="add_service")),
        (t("admin_menu_delete_service", lang), pack_cb(AdminMenuCB, act="delete_service")),
        (t("admin_menu_bookings", lang), pack_cb(AdminMenuCB, act="show_bookings")),
        (t("admin_menu_link_ms", lang), pack_cb(AdminMenuCB, act="link_ms")),
        (t("admin_menu_manage_prices", lang), pack_cb(AdminMenuCB, act="manage_prices")),
        (t("admin_menu_unlink_ms", lang), pack_cb(AdminMenuCB, act="unlink_ms")),
        (t("admin_menu_settings", lang), pack_cb(AdminMenuCB, act="settings")),
        (t("admin_menu_stats", lang), pack_cb(AdminMenuCB, act="stats")),
        (t("admin_menu_biz", lang), pack_cb(AdminMenuCB, act="biz")),
        (t("admin_menu_test", lang), pack_cb(AdminMenuCB, act="test")),
    ]
    buttons.append((t("back", lang), pack_cb(NavCB, act="role_root")))
    logger.debug("Главное меню админ-панели сгенерировано")
    return get_simple_kb(buttons, cols=2)


def services_list_kb(services: list[tuple[str, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Список послуг з кнопками для редагування ціни.

    Args:
        services: список кортежів (service_id, name)
    """
    from bot.app.telegram.common.callbacks import AdminEditPriceCB
    items: list[tuple[str, str]] = [
        (f"{name}", pack_cb(AdminEditPriceCB, service_id=str(sid))) for sid, name in services[:100]
    ]
    from bot.app.telegram.common.callbacks import AdminMenuCB
    items.append((t("back", lang), pack_cb(NavCB, act="role_root")))
    return get_simple_kb(items, cols=1)


def edit_price_kb(service_id: str, lang: str = "uk") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # Quick adjust row (-50, -20, -5)
    from bot.app.telegram.common.callbacks import AdminPriceAdjCB, AdminSetPriceCB, AdminSetCurrencyCB
    for d in (-50, -20, -5):
        kb.button(text=f"{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    # Quick adjust row (+5, +20, +50)
    for d in (5, 20, 50):
        kb.button(text=f"+{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    # Manual edit and currency
    kb.button(text=(t("set_price", lang) if t("set_price", lang) != "set_price" else "✏️ "+t("enter_price", lang)), callback_data=pack_cb(AdminSetPriceCB, service_id=str(service_id)))
    kb.button(text=(t("set_currency", lang) if t("set_currency", lang) != "set_currency" else t("enter_currency", lang)), callback_data=pack_cb(AdminSetCurrencyCB, service_id=str(service_id)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(3, 3, 1, 1)
    return kb.as_markup()


def admin_settings_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню настроек админ-панели."""
    kb = InlineKeyboardBuilder()

    token = get_telegram_provider_token() or ""
    enabled = is_telegram_payments_enabled()
    if not token:
        state_txt = t("payments_token_missing_state", lang)
    else:
        state_txt = (
            t("payments_enabled_state", lang)
            if enabled else t("payments_disabled_state", lang)
        )
    kb.button(text=state_txt, callback_data=pack_cb(AdminMenuCB, act="toggle_telegram_payments"))

    # Reservation hold minutes control
    try:
        hold_min = int(get_hold_minutes(10))
    except Exception:
        hold_min = int(getattr(cfg, "SETTINGS", {}).get("hold_minutes", 10) or 10)
    kb.button(
        text=t("hold_label", lang).format(minutes=hold_min),
        callback_data=pack_cb(AdminMenuCB, act="hold_menu")
    )

    # Client cancel lock hours control
    try:
        cancel_h = int(get_setting("client_cancel_lock_hours", 3) or 3)
    except Exception:
        cancel_h = int(getattr(cfg, "SETTINGS", {}).get("client_cancel_lock_hours", 3) or 3)
    kb.button(
        text=t("cancel_lock_label", lang).format(hours=cancel_h),
    callback_data=pack_cb(AdminMenuCB, act="cancel_menu")
    )

    # Expiration worker frequency display and menu
    try:
        expire_sec = int(get_setting("reservation_expire_check_seconds", 30) or 30)
    except Exception:
        expire_sec = int(getattr(cfg, "SETTINGS", {}).get("reservation_expire_check_seconds", 30))
    # Humanize label (minutes/hours/days)
    if expire_sec >= 86400 and expire_sec % 86400 == 0:
        days = expire_sec // 86400
        expire_label = f"{days} {t('day', lang) if t('day', lang) != 'day' else 'day'}"
    elif expire_sec >= 3600 and expire_sec % 3600 == 0:
        hours = expire_sec // 3600
        expire_label = f"{hours} {t('hours_short', lang) or 'h'}"
    elif expire_sec >= 60 and expire_sec % 60 == 0:
        mins = expire_sec // 60
        expire_label = f"{mins} {t('minutes_short', lang) or 'min'}"
    else:
        expire_label = f"{expire_sec} s"

    kb.button(
        text=f"⏱ {t('expire_check_frequency', lang) if t('expire_check_frequency', lang) != 'expire_check_frequency' else 'Expiration check'}: {expire_label}",
    callback_data=pack_cb(AdminMenuCB, act="expire_menu"),
    )

    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))

    # раскладываем кнопки по 2 в ряд
    kb.adjust(2)

    logger.debug("Меню настроек админ-панели сгенерировано")
    return kb.as_markup()


def admin_hold_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Меню выбора времени удержания резерва."""
    kb = InlineKeyboardBuilder()
    options = [1, 5, 10, 15, 20, 30, 45, 60]
    from bot.app.telegram.common.callbacks import AdminSetHoldCB
    for m in options:
        suffix = t("minutes_short", lang)
        label = f"{m} {suffix}" if suffix else f"{m}"
        kb.button(text=label, callback_data=pack_cb(AdminSetHoldCB, minutes=int(m)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки удержания резерва сгенерировано")
    return kb.as_markup()


def admin_expire_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Меню выбора частоты проверки просроченных броней (в секундах)."""
    kb = InlineKeyboardBuilder()
    # options in seconds: 1m,5m,15m,1h,1d
    options = [60, 5 * 60, 15 * 60, 60 * 60, 24 * 60 * 60]
    from bot.app.telegram.common.callbacks import AdminSetExpireCB
    # Pre-read current value for selection highlighting
    try:
        current_expire = int(get_setting('reservation_expire_check_seconds', 0) or 0)
    except Exception:
        current_expire = int(getattr(cfg, 'SETTINGS', {}).get('reservation_expire_check_seconds', 0) or 0)

    for s in options:
        if s >= 86400 and s % 86400 == 0:
            lbl = f"{s // 86400} {t('day', lang) if t('day', lang) != 'day' else 'day'}"
        elif s >= 3600 and s % 3600 == 0:
            lbl = f"{s // 3600} {t('hours_short', lang) or 'h'}"
        elif s >= 60 and s % 60 == 0:
            lbl = f"{s // 60} {t('minutes_short', lang) or 'min'}"
        else:
            lbl = f"{s} s"
        kb.button(text=(f"✔️ {lbl}" if current_expire == s else lbl), callback_data=pack_cb(AdminSetExpireCB, seconds=int(s)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки частоты проверки просроченных броней сгенерировано")
    return kb.as_markup()


def admin_cancel_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Меню выбора окна запрета отмены (в часах)."""
    kb = InlineKeyboardBuilder()
    options = [1, 2, 3, 6, 12, 24, 48]
    from bot.app.telegram.common.callbacks import AdminSetCancelCB
    for h in options:
        label = f"{h} {t('hours_short', lang) or 'h'}"
        kb.button(text=label, callback_data=pack_cb(AdminSetCancelCB, hours=int(h)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки окна отмены (часы) сгенерировано")
    return kb.as_markup()


def show_bookings_filter_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует клавиатуру фильтров для записей."""
    upcoming_txt = t("upcoming", lang) if t("upcoming", lang) != "upcoming" else "📅 Upcoming"
    today_txt = t("today", lang) if t("today", lang) != "today" else "Сегодня"
    this_week_txt = t("this_week", lang) if t("this_week", lang) != "this_week" else "Эта неделя"
    this_month_txt = t("this_month", lang) if t("this_month", lang) != "this_month" else "Этот месяц"
    last_month_txt = t("last_month", lang) if t("last_month", lang) != "last_month" else "Прошлый месяц"

    from bot.app.telegram.common.callbacks import AdminBookingsCB

    items = [
        (t("all_bookings", lang), pack_cb(AdminBookingsCB, mode="all")),
        (upcoming_txt, pack_cb(AdminBookingsCB, mode="upcoming")),
        (t("paid_bookings", lang), pack_cb(AdminBookingsCB, mode="paid")),
        (t("awaiting_bookings", lang), pack_cb(AdminBookingsCB, mode="awaiting")),
        (t("cancelled_bookings", lang), pack_cb(AdminBookingsCB, mode="cancelled")),
        (t("done_bookings", lang), pack_cb(AdminBookingsCB, mode="done")),
        (t("no_show_bookings", lang), pack_cb(AdminBookingsCB, mode="no_show")),
        (today_txt, pack_cb(AdminBookingsCB, mode="today")),
        (this_week_txt, pack_cb(AdminBookingsCB, mode="week")),
        (this_month_txt, pack_cb(AdminBookingsCB, mode="this_month")),
        (last_month_txt, pack_cb(AdminBookingsCB, mode="last_month")),
        (t("export_month_csv", lang), pack_cb(AdminMenuCB, act="export_csv")),
        (t("back", lang), pack_cb(NavCB, act="role_root")),
    ]
    logger.debug("Клавиатура фильтров записей сгенерирована")
    return get_simple_kb(items, cols=2)


def stats_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню статистики.

    Returns:
        InlineKeyboardMarkup с кнопками статистики.
    """
    logger.debug("Меню статистики сгенерировано")
    return get_simple_kb([
              (t("stats_week", lang), pack_cb(AdminMenuCB, act="stats_range_week")),
              (t("stats_month", lang), pack_cb(AdminMenuCB, act="stats_range_month")),
              (t("stats_by_master", lang), pack_cb(AdminMenuCB, act="stats_by_master")),
              (t("stats_by_service", lang), pack_cb(AdminMenuCB, act="stats_by_service")),
          (t("back", lang), pack_cb(NavCB, act="role_root")),
    ], cols=2)


def biz_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню бизнес-аналитики.

    Returns:
        InlineKeyboardMarkup с кнопками аналитики.
    """
    logger.debug("Меню бизнес-аналитики сгенерировано")
    return get_simple_kb([
              (t("revenue", lang), pack_cb(AdminMenuCB, act="biz_rev")),
              (t("retention", lang), pack_cb(AdminMenuCB, act="biz_ret")),
              (t("no_show", lang), pack_cb(AdminMenuCB, act="biz_ns")),
              (t("top_ltv", lang), pack_cb(AdminMenuCB, act="biz_ltv")),
          (t("back", lang), pack_cb(NavCB, act="role_root")),
    ], cols=2)


def pagination_kb(prefix: str, page: int, total_pages: int, lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует клавиатуру пагинации.

    Args:
        prefix: Префикс для callback_data кнопок пагинации.
        page: Текущая страница.
        total_pages: Общее количество страниц.

    Returns:
        InlineKeyboardMarkup с кнопками пагинации.
    """
    # Mapping of prefix -> typed CallbackData classes for pagination
    PAGINATION_CB_MAP: Mapping[str, type] = {
        "bookings": BookingsPageCB,
        "del_master": DelMasterPageCB,
        "del_service": DelServicePageCB,
    }

    builder = InlineKeyboardBuilder()
    # Emit typed CallbackData for known prefixes; fallback to legacy string for unknown prefixes
    CB_Class = PAGINATION_CB_MAP.get(prefix)
    if page > 1:
        if CB_Class:
            cb = pack_cb(CB_Class, page=page - 1)
        else:
            cb = f"{prefix}_page_{page - 1}"
        builder.button(text=t("prev_page", lang), callback_data=cb)
    if page < total_pages:
        if CB_Class:
            cb = pack_cb(CB_Class, page=page + 1)
        else:
            cb = f"{prefix}_page_{page + 1}"
        builder.button(text=t("next_page", lang), callback_data=cb)
    builder.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    builder.adjust(2, 1)
    logger.debug("Клавиатура пагинации сгенерирована: prefix=%s, page=%d, total_pages=%d", prefix, page, total_pages)
    return builder.as_markup()


# render_stats_overview now lives in shared_services


__all__ = [
    "admin_menu_kb",
    "admin_settings_kb",
    "admin_hold_menu_kb",
    "services_list_kb",
    "edit_price_kb",
    "show_bookings_filter_kb",
    "stats_menu_kb",
    "biz_menu_kb",
    "pagination_kb",
]