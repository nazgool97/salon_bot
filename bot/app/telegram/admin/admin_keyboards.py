from __future__ import annotations
import logging
from typing import Any, Mapping, cast

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.app.telegram.client.client_keyboards import get_simple_kb
from bot.app.services.admin_services import render_stats_overview
# Settings/facade values are supplied by handlers; keyboards must remain UI-only.
from bot.app.translations import t, tr
from bot.app.telegram.common.callbacks import pack_cb, AdminMenuCB, NavCB, AdminEditSettingCB
from bot.app.telegram.common.callbacks import AdminMasterCardCB
from bot.app.telegram.common.callbacks import (
    BookingsPageCB,
    DelMasterPageCB,
    DelServicePageCB,
)

logger = logging.getLogger(__name__)


def _resolve_setting_int_direct(arg: int | str | None, default: int) -> int:
    """UI-only parser: cast arg to int or return default (no DB access)."""
    if arg is not None:
        try:
            return int(arg)
        except Exception:
            return default
    return default

def admin_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует главное меню админ-панели.

    Returns:
        InlineKeyboardMarkup с кнопками меню.
    """
    # New grouped main menu: four primary groups that open submenus
    from bot.app.telegram.common.callbacks import AdminMenuCB

    builder = InlineKeyboardBuilder()
    # Row 1: Bookings management and Analytics
    builder.button(text=tr("admin_menu_bookings", lang=lang), callback_data=pack_cb(AdminMenuCB, act="show_bookings"))
    # Use dedicated Analytics submenu (act="analytics") instead of legacy Stats direct entry
    builder.button(text=tr("admin_analytics_title", lang=lang) if tr("admin_analytics_title", lang=lang) != "admin_analytics_title" else (tr("analytics", lang=lang) if tr("analytics", lang=lang) != "analytics" else "Аналитика"), callback_data=pack_cb(AdminMenuCB, act="analytics"))
    # Row 2: CRUD management (opens a submenu) and Settings
    builder.button(text=tr("admin_menu_manage_crud", lang=lang) if tr("admin_menu_manage_crud", lang=lang) != "admin_menu_manage_crud" else "Управление (CRUD)", callback_data=pack_cb(AdminMenuCB, act="manage_crud"))
    builder.button(text=tr("admin_menu_settings", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))
    # Role-root Back
    builder.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    # Arrange as 2 columns (two rows plus back)
    builder.adjust(2, 2, 1)
    logger.debug("Главное меню админ-панели (группированное) сгенерировано")
    return builder.as_markup()


def management_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for CRUD management: masters, services, linking, admins.

    Price management moved to Settings (Business) submenu; removed from CRUD.
    """
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    # Grouped entries: each button opens a focused submenu
    kb.button(text=("👩‍🔧 " + tr("manage_masters_label", lang=lang)), callback_data=pack_cb(AdminMenuCB, act="manage_masters"))
    kb.button(text=("💅 " + tr("manage_services_label", lang=lang)), callback_data=pack_cb(AdminMenuCB, act="manage_services"))
    kb.button(text=("🔗 " + tr("manage_links_label", lang=lang)), callback_data=pack_cb(AdminMenuCB, act="manage_links"))
    # Admins management (list and revoke admin rights)
    kb.button(text=("👥 " + tr("manage_admins_label", lang=lang) if tr("manage_admins_label", lang=lang) != "manage_admins_label" else "Адміни"), callback_data=pack_cb(AdminMenuCB, act="manage_admins"))
    # Price management removed from CRUD; now accessible via Business settings.
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    # Layout: two rows of two and a back row
    kb.adjust(2, 2, 1)
    logger.debug("Submenu управления (CRUD) сгруппировано с подменю")
    return kb.as_markup()


def masters_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for master management (Add / Delete / View links)."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_master"))
    kb.button(text=tr("admin_menu_delete_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
    kb.button(text=tr("admin_view_links_prompt", lang=lang) if tr("admin_view_links_prompt", lang=lang) != "admin_view_links_prompt" else tr("view_links_by_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="view_links_master"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def admin_masters_list_kb(masters: dict[int, str] | Mapping[int, str], lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard showing list of masters for admin to pick a master object to manage.

    Args:
        masters: mapping of master_id -> name
    """
    builder = InlineKeyboardBuilder()
    # Add new master action
    builder.button(text="➕ " + (tr("admin_menu_add_master", lang=lang) if tr("admin_menu_add_master", lang=lang) != "admin_menu_add_master" else "Добавить нового мастера"), callback_data=pack_cb(AdminMenuCB, act="add_master"))

    # Master buttons: one per row
    # Normalize to iterable of (id, name) pairs
    iterable = None
    try:
        if hasattr(masters, "items"):
            iterable = list(masters.items())
        else:
            iterable = list(masters)
    except Exception:
        iterable = []

    for pair in iterable[:200]:
        try:
            # Support both (id, name) tuples and single-value entries
            if isinstance(pair, (list, tuple)):
                mid = int(pair[0])
                name = pair[1] if len(pair) > 1 else str(mid)
            else:
                mid = int(pair)
                name = str(mid)
            label = f"👩‍🔧 {name or f'#{mid}'}"
            builder.button(text=label, callback_data=pack_cb(AdminMasterCardCB, master_id=int(mid)))
        except Exception:
            continue

    # Return explicitly to the admin panel root to avoid popping into
    # previously visited object cards (e.g. a master card). Use `role_root`
    # so the navigation helper shows the admin menu regardless of nav stack.
    builder.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    builder.adjust(1)
    return builder.as_markup()


def services_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for service management (Add / Delete)."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_service"))
    kb.button(text=tr("admin_menu_delete_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="delete_service"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1)
    return kb.as_markup()


def links_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for linking/unlinking masters and services."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_link_ms", lang=lang), callback_data=pack_cb(AdminMenuCB, act="link_ms"))
    kb.button(text=tr("admin_menu_unlink_ms", lang=lang), callback_data=pack_cb(AdminMenuCB, act="unlink_ms"))
    kb.button(text=tr("admin_menu_view_links", lang=lang), callback_data=pack_cb(AdminMenuCB, act="view_links"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def analytics_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Analytics submenu grouping stats and business metrics."""
    from bot.app.telegram.common.callbacks import AdminMenuCB
    kb = InlineKeyboardBuilder()
    # Simplified hub: clear choice between operational stats and financial business metrics.
    # Row 1: Stats (operations)
    kb.button(
        text=(tr("stats_menu_label", lang=lang) if tr("stats_menu_label", lang=lang) != "stats_menu_label" else tr("stats", lang=lang)),
        callback_data=pack_cb(AdminMenuCB, act="stats"),
    )
    # Row 2: Biz (financial)
    kb.button(
        text=(tr("biz_menu_label", lang=lang) if tr("biz_menu_label", lang=lang) != "biz_menu_label" else tr("biz", lang=lang)),
        callback_data=pack_cb(AdminMenuCB, act="biz"),
    )
    # Row 3: Export CSV (month/current filter)
    kb.button(text=tr("export_month_csv", lang=lang), callback_data=pack_cb(AdminMenuCB, act="export_csv"))
    # Row 4: Back to role root
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    # Layout: single-column stacked choices for clarity
    kb.adjust(1, 1, 1, 1)
    logger.debug("Analytics hub (Stats vs Biz) generated")
    return kb.as_markup()


def services_list_kb(services: list[tuple[str, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Список послуг з кнопками для редагування ціни.

    Args:
        services: список кортежів (service_id, name)
    """
    from bot.app.telegram.common.callbacks import AdminEditPriceCB
    items: list[tuple[str, str]] = [
        (f"{name}", pack_cb(AdminEditPriceCB, service_id=str(sid))) for sid, name in services[:100]
    ]
    from bot.app.telegram.common.callbacks import NavCB
    items.append((tr("back", lang=lang), pack_cb(NavCB, act="back")))
    return get_simple_kb(items, cols=1)


def masters_list_kb(masters: list[tuple[int, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard listing masters for admin selection.

    Args:
        masters: list of tuples (telegram_id, name)
    """
    from bot.app.telegram.common.callbacks import SelectViewMasterCB
    items = [(str(name), pack_cb(SelectViewMasterCB, master_id=int(tid))) for tid, name in masters[:200]]
    items.append((tr("back", lang=lang), pack_cb(NavCB, act="role_root")))
    return get_simple_kb(items, cols=1)


def services_select_kb(services: list[tuple[str, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard listing services for admin selection (view links by service)."""
    from bot.app.telegram.common.callbacks import SelectViewServiceCB
    items = [(f"{name}", pack_cb(SelectViewServiceCB, service_id=str(sid))) for sid, name in services[:200]]
    items.append((tr("back", lang=lang), pack_cb(NavCB, act="role_root")))
    return get_simple_kb(items, cols=1)


def no_masters_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard shown when there are no masters: offer to add one or go back."""
    from bot.app.telegram.common.callbacks import AdminMenuCB, NavCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_master"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(2)
    return kb.as_markup()


def no_services_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard shown when there are no services: offer to add one or go back."""
    from bot.app.telegram.common.callbacks import AdminMenuCB, NavCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_service"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(2)
    return kb.as_markup()


def edit_price_kb(service_id: str, lang: str = "uk") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # Quick adjust row (-50, -20, -5)
    from bot.app.telegram.common.callbacks import AdminPriceAdjCB, AdminSetPriceCB, AdminSetCurrencyCB, NavCB
    for d in (-50, -20, -5):
        kb.button(text=f"{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    # Quick adjust row (+5, +20, +50)
    for d in (5, 20, 50):
        kb.button(text=f"+{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    # Manual edit and currency
    kb.button(text=(tr("set_price", lang=lang) if tr("set_price", lang=lang) != "set_price" else "✏️ "+tr("enter_price", lang=lang)), callback_data=pack_cb(AdminSetPriceCB, service_id=str(service_id)))
    kb.button(text=(tr("set_currency", lang=lang) if tr("set_currency", lang=lang) != "set_currency" else tr("enter_currency", lang=lang)), callback_data=pack_cb(AdminSetCurrencyCB, service_id=str(service_id)))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1, 1)
    return kb.as_markup()


def settings_categories_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Top-level settings categories to reduce visual noise."""
    kb = InlineKeyboardBuilder()
    # Wizard button first for onboarding
    kb.button(text=(tr("wizard_start_title", lang=lang) or "⚙️ Setup Wizard"), callback_data=pack_cb(AdminMenuCB, act="settings_wizard_start"))
    kb.button(text=(tr("settings_category_contacts", lang=lang) or "Contacts"), callback_data=pack_cb(AdminMenuCB, act="settings_contacts"))
    kb.button(text=(tr("settings_category_business", lang=lang) or "Business"), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(1, 1, 1, 1, 1)
    return kb.as_markup()



def contacts_settings_kb(lang: str = "uk", *, phone: str | None = None, address: str | None = None, instagram: str | None = None) -> InlineKeyboardMarkup:
    """Contacts settings: phone, address, Instagram."""
    kb = InlineKeyboardBuilder()
    if phone:
        kb.button(text=f"{tr('phone_label', lang=lang)}: {phone}", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_phone"))
    else:
        kb.button(text=f"{tr('phone_label', lang=lang)} ➕", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_phone"))
    if address:
        kb.button(text=f"{tr('address_label', lang=lang)}", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_address"))
    else:
        kb.button(text=f"{tr('address_label', lang=lang)} ➕", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_address"))
    if instagram:
        kb.button(text=f"{tr('instagram_label', lang=lang)}", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_instagram"))
    else:
        kb.button(text=f"{tr('instagram_label', lang=lang)} ➕", callback_data=pack_cb(AdminEditSettingCB, setting_key="contact_instagram"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1, 1, 1)
    return kb.as_markup()


def business_settings_kb(
    lang: str = "uk",
    *,
    telegram_provider_token: str | None = None,
    payments_enabled: bool | None = None,
    hold_min: int | None = None,
    cancel_h: int | None = None,
    reminder_min: int | None = None,
    timezone: str | None = None,
) -> InlineKeyboardMarkup:
    """Business logic: payments, hold/cancel menus, manage service prices."""
    kb = InlineKeyboardBuilder()
    token = telegram_provider_token or ""
    enabled = bool(payments_enabled)
    if not token:
        state_txt = tr("payments_token_missing_state", lang=lang)
    else:
        state_txt = (
            tr("payments_enabled_state", lang=lang) if enabled else tr("payments_disabled_state", lang=lang)
        )
    kb.button(text=state_txt, callback_data=pack_cb(AdminMenuCB, act="toggle_telegram_payments"))

    _hold = _resolve_setting_int_direct(hold_min, 10)
    kb.button(text=tr("hold_label", lang=lang).format(minutes=_hold), callback_data=pack_cb(AdminMenuCB, act="hold_menu"))
    _cancel = _resolve_setting_int_direct(cancel_h, 3)
    kb.button(text=tr("cancel_lock_label", lang=lang).format(hours=_cancel), callback_data=pack_cb(AdminMenuCB, act="cancel_menu"))

    # Add manage prices relocated from CRUD
    from bot.app.translations import tr as _tr
    kb.button(text=_tr("admin_menu_manage_prices", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_prices"))
    # Reminder lead-time button
    _rem = _resolve_setting_int_direct(reminder_min, 60)
    # Present reminder label in hours when configured value is >= 2 hours
    if _rem >= 120 and _rem % 60 == 0:
        hrs = _rem // 60
        hr_label = tr('hours_short', lang=lang) or 'h'
        kb.button(text=tr("reminder_lead_label", lang=lang).format(minutes=f"{hrs} {hr_label}"), callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))
    else:
        kb.button(text=tr("reminder_lead_label", lang=lang).format(minutes=_rem), callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))
    # Timezone display / picker
    tz_label = timezone or (tr("timezone_label", lang=lang) if tr("timezone_label", lang=lang) != "timezone_label" else "Timezone")
    # Show current timezone value if provided
    if timezone:
        kb.button(text=f"🌐 {tz_label}", callback_data=pack_cb(AdminMenuCB, act="settings_timezone"))
    else:
        kb.button(text=(tr("timezone_label", lang=lang) or "Timezone"), callback_data=pack_cb(AdminMenuCB, act="settings_timezone"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1, 1, 1, 1)
    return kb.as_markup()


def service_currency_picker_kb(service_id: str, lang: str = "uk") -> InlineKeyboardMarkup:
    """Currency picker for a specific service."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetServiceCurrencyCB, NavCB
    for code in ("UAH", "USD", "EUR"):
        kb.button(text=code, callback_data=pack_cb(AdminSetServiceCurrencyCB, service_id=str(service_id), code=code))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 1)
    return kb.as_markup()


def confirm_delete_service_kb(service_id: str, lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard for confirming deletion of a service (admin flow).

    Keeps UI centralized so handlers can reuse a single source of truth.
    """
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import ExecDelServiceCB
    kb.button(text=t("confirm_delete", lang), callback_data=pack_cb(ExecDelServiceCB, service_id=str(service_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_service"))
    kb.adjust(1, 1)
    return kb.as_markup()


def confirm_delete_master_kb(master_id: int, lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard to confirm deletion of a master (admin flow).

    Includes: Confirm delete, Cancel (returns to delete_master menu), and
    an option to mass-cancel bookings for the master.
    """
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import ExecDelMasterCB, ConfirmCancelAllMasterCB
    kb.button(text=t("confirm_delete", lang), callback_data=pack_cb(ExecDelMasterCB, master_id=int(master_id)))
    # Force delete entry: leads to a stronger confirmation dialog (destructive)
    from bot.app.telegram.common.callbacks import ConfirmForceDelMasterCB
    kb.button(text="⚠️ " + (tr("force_delete_label", lang=lang) if tr("force_delete_label", lang=lang) != "force_delete_label" else "Force delete"), callback_data=pack_cb(ConfirmForceDelMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
    kb.button(text=t("cancel_all_bookings_button", lang), callback_data=pack_cb(ConfirmCancelAllMasterCB, master_id=int(master_id)))
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def confirm_force_delete_master_kb(master_id: int, lang: str = "uk") -> InlineKeyboardMarkup:
    """Strong confirmation keyboard for physical deletion (force delete).

    This keyboard performs an explicit final confirmation before executing
    the destructive `ExecForceDelMasterCB` callback.
    """
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import ExecForceDelMasterCB
    kb.button(text=(tr("confirm_force_delete", lang) if tr("confirm_force_delete", lang) != "confirm_force_delete" else "🔥 Delete permanently"), callback_data=pack_cb(ExecForceDelMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
    kb.adjust(1, 1)
    return kb.as_markup()


def confirm_cancel_all_master_kb(master_id: int, linked_count: int | None = None, lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard to confirm cancelling all bookings for a master.

    Shows a Confirm and Cancel button; UI-only helper, no DB access.
    """
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import ExecCancelAllMasterCB
    kb.button(text=t("confirm", lang), callback_data=pack_cb(ExecCancelAllMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
    kb.adjust(2)
    return kb.as_markup()


def admin_settings_kb(
    lang: str = "uk",
    *,
    telegram_provider_token: str | None = None,
    payments_enabled: bool | None = None,
    hold_min: int | None = None,
    cancel_h: int | None = None,
    hours_summary: str | None = None,
    reminder_min: int | None = None,
) -> InlineKeyboardMarkup:
    """Генерирует меню настроек админ-панели.

    This factory is UI-only and must not perform DB access. All runtime
    values (provider token, toggles and numeric settings) should be
    pre-fetched by handlers and passed in. If a value is omitted, a
    conservative fallback from shared settings helpers is used.
    """
    kb = InlineKeyboardBuilder()

    token = telegram_provider_token or ""
    enabled = bool(payments_enabled)
    if not token:
        state_txt = tr("payments_token_missing_state", lang=lang)
    else:
        state_txt = (
            tr("payments_enabled_state", lang=lang) if enabled else tr("payments_disabled_state", lang=lang)
        )
    kb.button(text=state_txt, callback_data=pack_cb(AdminMenuCB, act="toggle_telegram_payments"))

    # Slot duration removed from admin UI per UX decision.

    # Hold & Cancel lock (keep but after core business settings)
    _hold = _resolve_setting_int_direct(hold_min, 10)
    kb.button(text=tr("hold_label", lang=lang).format(minutes=_hold), callback_data=pack_cb(AdminMenuCB, act="hold_menu"))
    _cancel = _resolve_setting_int_direct(cancel_h, 3)
    kb.button(text=tr("cancel_lock_label", lang=lang).format(hours=_cancel), callback_data=pack_cb(AdminMenuCB, act="cancel_menu"))

    # Working hours summary/edit
    if hours_summary:
        kb.button(text=f"🗓️ {hours_summary}", callback_data=pack_cb(AdminMenuCB, act="settings_hours"))
    else:
        kb.button(text="🗓️ Робочі години", callback_data=pack_cb(AdminMenuCB, act="settings_hours"))

    # Reminder lead-time quick button
    _rem2 = _resolve_setting_int_direct(reminder_min, 60)
    if _rem2 >= 120 and _rem2 % 60 == 0:
        hrs2 = _rem2 // 60
        hr_label2 = tr('hours_short', lang=lang) or 'h'
        kb.button(text=tr("reminder_lead_label", lang=lang).format(minutes=f"{hrs2} {hr_label2}"), callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))
    else:
        kb.button(text=tr("reminder_lead_label", lang=lang).format(minutes=_rem2), callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))

    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))

    # раскладываем кнопки по 2 в ряд
    kb.adjust(2, 2, 2, 2, 2, 1)

    logger.debug("Меню настроек админ-панели сгенерировано")
    return kb.as_markup()


def admin_hold_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Меню выбора времени удержания резерва."""
    kb = InlineKeyboardBuilder()
    options = [1, 5, 10, 15, 20, 30, 45, 60]
    from bot.app.telegram.common.callbacks import AdminSetHoldCB, NavCB
    for m in options:
        suffix = tr("minutes_short", lang=lang)
        label = f"{m} {suffix}" if suffix else f"{m}"
        kb.button(text=label, callback_data=pack_cb(AdminSetHoldCB, minutes=int(m)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки удержания резерва сгенерировано")
    return kb.as_markup()


def admin_reminder_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Меню выбора за сколько минут напоминать клиенту перед записью."""
    kb = InlineKeyboardBuilder()
    # options expressed in minutes — provide common lead times including minutes
    # Provide: 15m, 30m, 1h, 2h, 3h, 6h, 12h, 24h
    options = [15, 30, 60, 120, 180, 360, 720, 1440]
    from bot.app.telegram.common.callbacks import AdminSetReminderCB, NavCB
    # Localized short labels: prefer hours when divisible by 60
    for m in options:
        if m % 60 == 0:
            hrs = m // 60
            hr_label = tr('hours_short', lang=lang) or 'h'
            label = f"{hrs} {hr_label}"
        else:
            suffix = tr("minutes_short", lang=lang) or "min"
            label = f"{m} {suffix}"
        kb.button(text=label, callback_data=pack_cb(AdminSetReminderCB, minutes=int(m)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки времени напоминания сгенерировано")
    return kb.as_markup()


def currency_picker_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard for picking global currency (safe whitelist)."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetGlobalCurrencyCB, NavCB
    # Present a small, commonly used whitelist
    for code in ("UAH", "USD", "EUR"):
        kb.button(text=code, callback_data=pack_cb(AdminSetGlobalCurrencyCB, code=code))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    kb.adjust(3, 1)
    return kb.as_markup()


def timezone_picker_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard for picking a timezone from a small curated list."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetTimezoneCB, NavCB
    # Curated list of 10 common zones (UTC and regionals)
    zones = [
        "UTC",
        "Europe/Kyiv",
        "Europe/Moscow",
        "Europe/Warsaw",
        "Europe/Berlin",
        "Asia/Kiev",
        "Europe/London",
        "Europe/Paris",
        "America/New_York",
        "Asia/Tbilisi",
    ]
    for z in zones:
        kb.button(text=z, callback_data=pack_cb(AdminSetTimezoneCB, tz=z))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    # layout small rows
    kb.adjust(3, 3, 3, 1)
    return kb.as_markup()


def work_hours_start_kb(lang: str = "uk", min_hour: int = 6, max_hour: int = 22) -> InlineKeyboardMarkup:
    """Keyboard to pick working day start hour."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetWorkStartCB, NavCB
    hours = list(range(min_hour, max_hour + 1))
    for h in hours:
        kb.button(text=f"{h}:00", callback_data=pack_cb(AdminSetWorkStartCB, hour=int(h)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    # arrange in rows of 4
    cols = 4
    kb.adjust(*([cols] * ((len(hours) + cols - 1) // cols)), 1)
    return kb.as_markup()


def work_hours_end_kb(lang: str = "uk", start_hour: int = 6, max_hour: int = 23) -> InlineKeyboardMarkup:
    """Keyboard to pick working day end hour (must be > start_hour)."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetWorkEndCB, NavCB
    hours = list(range(start_hour + 1, max_hour + 1))
    for h in hours:
        kb.button(text=f"{h}:00", callback_data=pack_cb(AdminSetWorkEndCB, start=int(start_hour), hour=int(h)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
    cols = 4
    kb.adjust(*([cols] * ((len(hours) + cols - 1) // cols)), 1)
    return kb.as_markup()


def work_hours_days_kb(lang: str = "uk", hours_map: dict[int, tuple[int, int] | None] | None = None) -> InlineKeyboardMarkup:
    """Keyboard to pick a day for editing working hours."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminWorkHoursDayCB, NavCB
    labels = [
        (0, tr("mon_short", lang=lang) or "Mon"),
        (1, tr("tue_short", lang=lang) or "Tue"),
        (2, tr("wed_short", lang=lang) or "Wed"),
        (3, tr("thu_short", lang=lang) or "Thu"),
        (4, tr("fri_short", lang=lang) or "Fri"),
        (5, tr("sat_short", lang=lang) or "Sat"),
        (6, tr("sun_short", lang=lang) or "Sun"),
    ]
    for day, lbl in labels:
        suffix = ""
        if hours_map is not None and day in hours_map:
            rng = hours_map.get(day)
            if rng is None:
                suffix = f" ({tr('closed_label', lang=lang) or 'Closed'})"
            else:
                suffix = f" {rng[0]:02d}:00–{rng[1]:02d}:00"
        kb.button(text=f"{lbl}{suffix}", callback_data=pack_cb(AdminWorkHoursDayCB, day=int(day)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1, 1)
    return kb.as_markup()


def work_hours_day_start_kb(lang: str, day: int, min_hour: int = 0, max_hour: int = 23) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminWorkHoursStartCB, AdminWorkHoursClosedCB, NavCB
    for h in range(min_hour, max_hour):
        kb.button(text=f"{h:02d}:00", callback_data=pack_cb(AdminWorkHoursStartCB, day=int(day), hour=int(h)))
    kb.button(text=tr("mark_closed", lang=lang) or "Set Closed", callback_data=pack_cb(AdminWorkHoursClosedCB, day=int(day)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="settings_work_hours_days"))
    cols = 4
    kb.adjust(*([cols] * ((max_hour - min_hour + cols - 1) // cols)), 1, 1)
    return kb.as_markup()


def work_hours_day_end_kb(lang: str, day: int, start_hour: int, max_hour: int = 23) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminWorkHoursEndCB, NavCB
    for h in range(start_hour + 1, max_hour + 1):
        kb.button(text=f"{h:02d}:00", callback_data=pack_cb(AdminWorkHoursEndCB, day=int(day), start=int(start_hour), hour=int(h)))
    kb.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="settings_work_hours_days"))
    cols = 4
    kb.adjust(*([cols] * ((max_hour - (start_hour + 1) + cols) // cols)), 1)
    return kb.as_markup()


def admin_expire_menu_kb(lang: str = "uk", current_expire: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора частоты проверки просроченных броней (в секундах)."""
    kb = InlineKeyboardBuilder()
    # options in seconds: 1m,5m,15m,1h,1d
    options = [60, 5 * 60, 15 * 60, 60 * 60, 24 * 60 * 60]
    from bot.app.telegram.common.callbacks import AdminSetExpireCB
    # Current value should be provided by the caller (handler). UI must not access DB here.

    for s in options:
        if s >= 86400 and s % 86400 == 0:
            lbl = f"{s // 86400} {tr('day', lang=lang) if tr('day', lang=lang) != 'day' else 'day'}"
        elif s >= 3600 and s % 3600 == 0:
            lbl = f"{s // 3600} {tr('hours_short', lang=lang) or 'h'}"
        elif s >= 60 and s % 60 == 0:
            lbl = f"{s // 60} {tr('minutes_short', lang=lang) or 'min'}"
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
    from bot.app.telegram.common.callbacks import AdminSetCancelCB, NavCB
    for h in options:
        label = f"{h} {tr('hours_short', lang=lang) or 'h'}"
        kb.button(text=label, callback_data=pack_cb(AdminSetCancelCB, hours=int(h)))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1)
    logger.debug("Меню настройки окна отмены (часы) сгенерировано")
    return kb.as_markup()


# The old bookings filter keyboard has been removed: handlers now use the
# unified dashboard renderer from `client_keyboards.render_bookings_list_page`.
# Keyboard factories should remain UI-only; any filter state is persisted in
# handler FSM state and passed into the shared renderer.


def get_admin_bookings_dashboard_kb(lang: str = "uk", mode: str = "upcoming", page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Delegator: use shared build_bookings_dashboard_kb for admin dashboard."""
    try:
        from bot.app.telegram.client.client_keyboards import build_bookings_dashboard_kb

        meta = {"mode": mode, "page": int(page or 1), "total_pages": int(total_pages or 1)}
        return build_bookings_dashboard_kb("admin", meta, lang=lang)
    except Exception:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from bot.app.telegram.common.callbacks import pack_cb, NavCB
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))]])


def stats_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню статистики.

    Returns:
        InlineKeyboardMarkup с кнопками статистики.
    """
    logger.debug("Меню статистики сгенерировано")
    return get_simple_kb([
              (tr("stats_week", lang=lang), pack_cb(AdminMenuCB, act="stats_range_week")),
              (tr("stats_month", lang=lang), pack_cb(AdminMenuCB, act="stats_range_month")),
              (tr("stats_by_master", lang=lang), pack_cb(AdminMenuCB, act="stats_by_master")),
              (tr("stats_by_service", lang=lang), pack_cb(AdminMenuCB, act="stats_by_service")),
          (tr("back", lang=lang), pack_cb(NavCB, act="role_root")),
    ], cols=2)


def biz_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню бизнес-аналитики.

    Returns:
        InlineKeyboardMarkup с кнопками аналитики.
    """
    logger.debug("Меню бизнес-аналитики сгенерировано")
    return get_simple_kb([
              (tr("revenue", lang=lang), pack_cb(AdminMenuCB, act="biz_rev")),
              (tr("retention", lang=lang), pack_cb(AdminMenuCB, act="biz_ret")),
              (tr("no_show", lang=lang), pack_cb(AdminMenuCB, act="biz_ns")),
              (tr("top_ltv", lang=lang), pack_cb(AdminMenuCB, act="biz_ltv")),
          (tr("back", lang=lang), pack_cb(NavCB, act="role_root")),
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
    if CB_Class is None:
        logger.warning("pagination_kb: unsupported prefix %s, paging buttons skipped", prefix)
    else:
        if page > 1:
            builder.button(text=tr("prev_page", lang=lang), callback_data=pack_cb(CB_Class, page=page - 1))
        if page < total_pages:
            builder.button(text=tr("next_page", lang=lang), callback_data=pack_cb(CB_Class, page=page + 1))
    builder.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="role_root"))
    builder.adjust(2, 1)
    logger.debug("Клавиатура пагинации сгенерирована: prefix=%s, page=%d, total_pages=%d", prefix, page, total_pages)
    return builder.as_markup()


# render_stats_overview now lives in admin_services


__all__ = [
    "admin_menu_kb",
    "admin_settings_kb",
    "admin_hold_menu_kb",
    "services_list_kb",
    "edit_price_kb",
    # legacy filter keyboard removed; admin handlers now show unified dashboard
    "stats_menu_kb",
    "biz_menu_kb",
    "pagination_kb",
]