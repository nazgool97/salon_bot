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
from bot.app.services.shared_services import _coerce_int, format_minutes_short
from bot.app.telegram.common.callbacks import AdminMasterCardCB
from bot.app.telegram.common.callbacks import (
    BookingsPageCB,
    DelMasterPageCB,
    DelServicePageCB,
    PricePageCB,
    AdminEditPriceCB,
)

logger = logging.getLogger(__name__)

MAX_ADMIN_LIST_ITEMS = 200
MAX_ADMIN_PRICE_ITEMS = 100

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
    builder.button(text=t("admin_analytics_title", lang=lang), callback_data=pack_cb(AdminMenuCB, act="analytics"))
    # Row 2: CRUD management (opens a submenu) and Settings
    builder.button(text=t("admin_menu_manage_crud", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_crud"))
    builder.button(text=tr("admin_menu_settings", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))
    # Role-root Back
    # Use nav_back so admins return to the previous screen instead of jumping to root
    builder.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
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
    kb.button(text=t("manage_masters_label", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_masters"))
    kb.button(text=t("manage_services_label", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_services"))
    kb.button(text=t("manage_links_label", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_links"))
    # Admins management (list and revoke admin rights)
    kb.button(text=t("manage_admins_label", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_admins"))
    # Price management removed from CRUD; now accessible via Business settings.
    # Back should return to the admin panel (root of admin role) to avoid
    # hopping back into recently visited CRUD submenus and causing loops.
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="panel"))
    # Layout: two rows of two and a back row
    kb.adjust(2, 2, 1)
    logger.debug("Submenu управления (CRUD) сгруппировано с подменю")
    return kb.as_markup()


def masters_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for master management (Add / Delete / View links)."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=t("admin_menu_add_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_master"))
    kb.button(text=t("admin_menu_delete_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="delete_master"))
    kb.button(text=t("admin_view_links_prompt", lang=lang), callback_data=pack_cb(AdminMenuCB, act="view_links_master"))
    # Follow nav stack (back to previous screen)
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1, 1)
    return kb.as_markup()


def admin_masters_list_kb(
    masters: dict[int, str] | Mapping[int, str],
    lang: str = "uk"
) -> InlineKeyboardMarkup:
    """Keyboard showing list of masters for admin to pick a master object to manage.

    Args:
        masters: mapping of master_id -> name
    """
    builder = InlineKeyboardBuilder()

    # Add new master action
    builder.button(
        text=t("admin_menu_add_master", lang=lang),
        callback_data=pack_cb(AdminMenuCB, act="add_master"),
    )

    # Master buttons: one per row
    iterable = None
    try:
        if hasattr(masters, "items"):
            iterable = list(masters.items())
        else:
            iterable = list(masters)
    except Exception:
        iterable = []

    for pair in iterable[:MAX_ADMIN_LIST_ITEMS]:
        try:
            # Support both (id, name) tuples and single-value entries
            if isinstance(pair, (list, tuple)):
                mid = int(pair[0])
                name = pair[1] if len(pair) > 1 else str(mid)
            else:
                mid = int(pair)
                name = str(mid)
            label = f"{name or f'#{mid}'}"
            builder.button(
                text=label,
                callback_data=pack_cb(AdminMasterCardCB, master_id=int(mid)),
            )
        except Exception:
            continue

    # Follow navigation stack (returns to previous menu)
    builder.button(
        text=tr("back", lang=lang),
        callback_data=pack_cb(NavCB, act="back"),
    )
    builder.adjust(1)
    return builder.as_markup()


def services_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for service management (Add / Delete)."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_service"))
    kb.button(text=tr("admin_menu_delete_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="delete_service"))
    # Return to CRUD submenu explicitly to avoid returning to an unrelated screen
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_crud"))
    kb.adjust(1, 1)
    return kb.as_markup()


def links_crud_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Submenu for linking/unlinking masters and services."""
    from bot.app.telegram.common.callbacks import AdminMenuCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_link_ms", lang=lang), callback_data=pack_cb(AdminMenuCB, act="link_ms"))
    kb.button(text=tr("admin_menu_unlink_ms", lang=lang), callback_data=pack_cb(AdminMenuCB, act="unlink_ms"))
    kb.button(text=tr("admin_menu_view_links", lang=lang), callback_data=pack_cb(AdminMenuCB, act="view_links"))
    # Use nav-back so cancelling this submenu returns to the previous screen
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
        text=t("stats_menu_label", lang=lang),
        callback_data=pack_cb(AdminMenuCB, act="stats"),
    )
    # Row 2: Biz (financial)
    kb.button(
        text=t("biz_menu_label", lang=lang),
        callback_data=pack_cb(AdminMenuCB, act="biz"),
    )
    # Row 3: Export CSV (month/current filter)
    kb.button(text=tr("export_month_csv", lang=lang), callback_data=pack_cb(AdminMenuCB, act="export_csv"))
    # Row 4: Back to Admin Panel (explicit target per UX requirement)
    from bot.app.telegram.common.callbacks import AdminMenuCB
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="panel"))
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
        (f"{name}", pack_cb(AdminEditPriceCB, service_id=str(sid))) for sid, name in services[:MAX_ADMIN_PRICE_ITEMS]
    ]
    # Use nav-back so this selection returns to the previous screen
    items.append((tr("back", lang=lang), pack_cb(NavCB, act="back")))
    return get_simple_kb(items, cols=1)


def services_prices_kb(
    services: list[tuple[str, str]],
    page: int,
    total_pages: int,
    lang: str = "uk",
) -> InlineKeyboardMarkup:
    """Paginated list of services for price management."""
    rows: list[list[InlineKeyboardButton]] = []
    for sid, name in services:
        label = str(name or sid)
        rows.append([
            InlineKeyboardButton(
                text=label,
                callback_data=pack_cb(AdminEditPriceCB, service_id=str(sid)),
            )
        ])

    # Navigation row: put Prev, Back and Next on the same line (back centered)
    nav_row: list[InlineKeyboardButton] = []
    if total_pages > 1 and page > 1:
        nav_row.append(InlineKeyboardButton(text=tr("prev_page", lang=lang), callback_data=pack_cb(PricePageCB, page=page - 1)))
    # Back button should always be present and sit on the same row as Next
    # Use nav-back so Back pops the nav stack to the previous screen
    nav_row.append(InlineKeyboardButton(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back")))
    if total_pages > 1 and page < total_pages:
        nav_row.append(InlineKeyboardButton(text=tr("next_page", lang=lang), callback_data=pack_cb(PricePageCB, page=page + 1)))
    rows.append(nav_row)

    return InlineKeyboardMarkup(inline_keyboard=rows)


def masters_list_kb(masters: list[tuple[int, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard listing masters for admin selection.

    Args:
        masters: list of tuples (telegram_id, name)
    """
    from bot.app.telegram.common.callbacks import SelectViewMasterCB
    items = [(str(name), pack_cb(SelectViewMasterCB, master_id=int(tid))) for tid, name in masters[:MAX_ADMIN_LIST_ITEMS]]
    # Follow nav stack to return to previous menu
    items.append((tr("back", lang=lang), pack_cb(NavCB, act="back")))
    return get_simple_kb(items, cols=1)


def services_select_kb(services: list[tuple[str, str]], lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard listing services for admin selection (view links by service)."""
    from bot.app.telegram.common.callbacks import SelectViewServiceCB
    items = [(f"{name}", pack_cb(SelectViewServiceCB, service_id=str(sid))) for sid, name in services[:MAX_ADMIN_LIST_ITEMS]]
    items.append((tr("back", lang=lang), pack_cb(AdminMenuCB, act="manage_services")))
    return get_simple_kb(items, cols=1)


def no_masters_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard shown when there are no masters: offer to add one or go back."""
    from bot.app.telegram.common.callbacks import AdminMenuCB, NavCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_master", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_master"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(2)
    return kb.as_markup()


def no_services_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard shown when there are no services: offer to add one or go back."""
    from bot.app.telegram.common.callbacks import AdminMenuCB, NavCB

    kb = InlineKeyboardBuilder()
    kb.button(text=tr("admin_menu_add_service", lang=lang), callback_data=pack_cb(AdminMenuCB, act="add_service"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_services"))
    kb.adjust(2)
    return kb.as_markup()


def edit_price_kb(service_id: str, lang: str = "uk") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # Quick adjust row (-50, -20, -5)
    from bot.app.telegram.common.callbacks import AdminPriceAdjCB, AdminSetPriceCB, NavCB
    for d in (-50, -20, -5):
        kb.button(text=f"{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    # Quick adjust row (+5, +20, +50)
    for d in (5, 20, 50):
        kb.button(text=f"+{d}", callback_data=pack_cb(AdminPriceAdjCB, service_id=str(service_id), delta=int(d)))
    kb.button(text=t("set_price", lang=lang), callback_data=pack_cb(AdminSetPriceCB, service_id=str(service_id)))
    # Back should pop to the previous screen (e.g., services CRUD)
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(3, 3, 1, 1)
    return kb.as_markup()


def settings_categories_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Top-level settings categories to reduce visual noise."""
    kb = InlineKeyboardBuilder()
    kb.button(text=(tr("settings_category_contacts", lang=lang) or "Contacts"), callback_data=pack_cb(AdminMenuCB, act="settings_contacts"))
    kb.button(text=(tr("settings_category_business", lang=lang) or "Business"), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    # Back should follow nav stack (settings -> categories -> ...)
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="panel"),)
    kb.adjust(1, 1, 1)
    return kb.as_markup()



def contacts_settings_kb(
    lang: str = "uk",
    *,
    phone: str | None = None,
    address: str | None = None,
    instagram: str | None = None,
    webapp_title: str | None = None,
) -> InlineKeyboardMarkup:
    """Contacts settings: phone, address, Instagram and WebApp title."""
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
    # WebApp title (display current value or add prompt)
    if webapp_title:
        kb.button(text=f"{tr('webapp_title_label', lang=lang) or 'WebApp title'}: {webapp_title}", callback_data=pack_cb(AdminEditSettingCB, setting_key="webapp_title"))
    else:
        kb.button(text=f"{tr('webapp_title_label', lang=lang) or 'WebApp title'} ➕", callback_data=pack_cb(AdminEditSettingCB, setting_key="webapp_title"))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))
    kb.adjust(1, 1, 1, 1, 1)
    return kb.as_markup()


def business_settings_kb(
    lang: str = "uk",
    *,
    telegram_provider_token: str | None = None,
    payments_enabled: bool | None = None,
    miniapp_enabled: bool | None = None,
    hold_min: int | None = None,
    cancel_min: int | None = None,
    reschedule_min: int | None = None,
    discount_percent: int | None = None,
    reminder_min: int | None = None,
    reminder_same_min: int | None = None,
    expire_sec: int | None = None,
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

    # Online payment discount quick-edit
    try:
        _disc = int(discount_percent) if discount_percent is not None else 0
    except Exception:
        _disc = 0
    disc_label = f"{tr('online_discount_label', lang=lang) or 'Online discount'}: {_disc}%"
    kb.button(text=disc_label, callback_data=pack_cb(AdminEditSettingCB, setting_key="online_payment_discount_percent"))

    # MiniApp toggle on business panel for parity with settings
    try:
        mini_enabled = bool(miniapp_enabled)
    except NameError:
        mini_enabled = False
    mini_state_txt = tr("miniapp_enabled_state", lang=lang) if mini_enabled else tr("miniapp_disabled_state", lang=lang)
    kb.button(text=mini_state_txt, callback_data=pack_cb(AdminMenuCB, act="toggle_telegram_miniapp"))

    _hold = _coerce_int(hold_min, 10)
    kb.button(text=tr("hold_label", lang=lang).format(minutes=_hold), callback_data=pack_cb(AdminMenuCB, act="hold_menu"))
    _cancel = _coerce_int(cancel_min, 180)
    kb.button(text=tr("cancel_lock_label", lang=lang).format(minutes=format_minutes_short(_cancel, lang)), callback_data=pack_cb(AdminMenuCB, act="cancel_menu"))

    _reschedule = _coerce_int(reschedule_min, 180)
    kb.button(text=tr("reschedule_lock_label", lang=lang).format(minutes=format_minutes_short(_reschedule, lang)), callback_data=pack_cb(AdminMenuCB, act="reschedule_menu"))

    # Add manage prices relocated from CRUD
    from bot.app.translations import tr as _tr
    kb.button(text=_tr("admin_menu_manage_prices", lang=lang), callback_data=pack_cb(AdminMenuCB, act="manage_prices"))
    # Reminder preview: "Нагадування: 24 год • 45 хв"
    _rem = _coerce_int(reminder_min, 60)
    _rem_same = _coerce_int(reminder_same_min, 60)
    lead_display = format_minutes_short(_rem, lang)
    if _rem_same is not None:
        same_display = format_minutes_short(_rem_same, lang)
        btn_label = f"{tr('reminder_summary_label', lang=lang) or 'Reminders'}: {lead_display} • {same_display}"
    else:
        btn_label = f"{tr('reminder_summary_label', lang=lang) or 'Reminders'}: {lead_display}"
    kb.button(text=btn_label, callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))

    # Expire-check frequency preview
    _exp = expire_sec if expire_sec is not None else 30
    if _exp >= 86400 and _exp % 86400 == 0:
        exp_lbl = f"{_exp // 86400} {t('day', lang=lang)}"
    elif _exp >= 3600 and _exp % 3600 == 0:
        exp_lbl = f"{_exp // 3600} {tr('hours_short', lang=lang) or 'h'}"
    elif _exp >= 60 and _exp % 60 == 0:
        exp_lbl = f"{_exp // 60} {tr('minutes_short', lang=lang) or 'min'}"
    else:
        exp_lbl = f"{_exp} s"
    kb.button(text=f"{tr('expire_check_frequency', lang=lang)}: {exp_lbl}", callback_data=pack_cb(AdminMenuCB, act="expire_menu"))
    # Timezone display removed from UI (configured via environment)
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))
    kb.adjust(1, 1, 1, 1, 1, 1, 1, 1)
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
    kb.button(text="⚠️ " + t("force_delete_label", lang=lang), callback_data=pack_cb(ConfirmForceDelMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(NavCB, act="back"))
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
    kb.button(text=t("confirm_force_delete", lang), callback_data=pack_cb(ExecForceDelMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(NavCB, act="back"))
    kb.adjust(1, 1)
    return kb.as_markup()


def confirm_cancel_all_master_kb(master_id: int, linked_count: int | None = None, lang: str = "uk") -> InlineKeyboardMarkup:
    """Keyboard to confirm cancelling all bookings for a master.

    Shows a Confirm and Cancel button; UI-only helper, no DB access.
    """
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import ExecCancelAllMasterCB
    kb.button(text=t("confirm", lang), callback_data=pack_cb(ExecCancelAllMasterCB, master_id=int(master_id)))
    kb.button(text=t("cancel", lang), callback_data=pack_cb(AdminMenuCB, act="manage_masters"))
    kb.adjust(2)
    return kb.as_markup()


def admin_settings_kb(
    lang: str = "uk",
    *,
    telegram_provider_token: str | None = None,
    payments_enabled: bool | None = None,
    miniapp_enabled: bool | None = None,
    hold_min: int | None = None,
    cancel_min: int | None = None,
    reschedule_min: int | None = None,
    hours_summary: str | None = None,
    reminder_min: int | None = None,
    reminder_same_min: int | None = None,
    expire_sec: int | None = None,
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
    _hold = _coerce_int(hold_min, 10)
    kb.button(text=tr("hold_label", lang=lang).format(minutes=_hold), callback_data=pack_cb(AdminMenuCB, act="hold_menu"))
    _cancel = _coerce_int(cancel_min, 180)
    kb.button(text=tr("cancel_lock_label", lang=lang).format(minutes=format_minutes_short(_cancel, lang)), callback_data=pack_cb(AdminMenuCB, act="cancel_menu"))

    _reschedule = _coerce_int(reschedule_min, 180)
    kb.button(text=tr("reschedule_lock_label", lang=lang).format(minutes=format_minutes_short(_reschedule, lang)), callback_data=pack_cb(AdminMenuCB, act="reschedule_menu"))

    # Working hours summary/edit
    if hours_summary:
        kb.button(text=hours_summary, callback_data=pack_cb(AdminMenuCB, act="settings_hours"))
    else:
        kb.button(text="Робочі години", callback_data=pack_cb(AdminMenuCB, act="settings_hours"))

    # Reminder preview: same as business menu
    _rem2 = _coerce_int(reminder_min, 60)
    # Telegram MiniApp booking toggle (UI mirrors Telegram Payments toggle)
    mini_enabled = bool(miniapp_enabled)
    mini_state_txt = tr("miniapp_enabled_state", lang=lang) if mini_enabled else tr("miniapp_disabled_state", lang=lang)
    kb.button(text=mini_state_txt, callback_data=pack_cb(AdminMenuCB, act="toggle_telegram_miniapp"))
    _rem_same2 = _coerce_int(reminder_same_min, 60)
    lead_display2 = format_minutes_short(_rem2, lang)
    if _rem_same2 is not None:
        same_display2 = format_minutes_short(_rem_same2, lang)
        rem_btn_label = f"{tr('reminder_summary_label', lang=lang) or 'Reminders'}: {lead_display2} • {same_display2}"
    else:
        rem_btn_label = f"{tr('reminder_summary_label', lang=lang) or 'Reminders'}: {lead_display2}"
    kb.button(text=rem_btn_label, callback_data=pack_cb(AdminMenuCB, act="settings_reminder"))

    # Expire-check frequency preview
    _exp2 = expire_sec if expire_sec is not None else 30
    if _exp2 >= 86400 and _exp2 % 86400 == 0:
        exp_lbl2 = f"{_exp2 // 86400} {t('day', lang=lang)}"
    elif _exp2 >= 3600 and _exp2 % 3600 == 0:
        exp_lbl2 = f"{_exp2 // 3600} {tr('hours_short', lang=lang) or 'h'}"
    elif _exp2 >= 60 and _exp2 % 60 == 0:
        exp_lbl2 = f"{_exp2 // 60} {tr('minutes_short', lang=lang) or 'min'}"
    else:
        exp_lbl2 = f"{_exp2} s"
    kb.button(text=f"{tr('expire_check_frequency', lang=lang)}: {exp_lbl2}", callback_data=pack_cb(AdminMenuCB, act="expire_menu"))

    # Expire-check frequency (background worker scan interval)
    kb.button(text=tr("expire_check_frequency", lang=lang), callback_data=pack_cb(AdminMenuCB, act="expire_menu"))

    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings"))

    # Keep all settings stacked in a single column to avoid wrapping
    # when toggle labels change length (one button per row).
    kb.adjust(1)

    logger.debug("Меню настроек админ-панели сгенерировано")
    return kb.as_markup()


def admin_hold_menu_kb(lang: str = "uk", current_min: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора времени удержания резерва."""
    kb = InlineKeyboardBuilder()
    options = [1, 5, 10, 15, 20, 30, 45, 60]
    from bot.app.telegram.common.callbacks import AdminSetHoldCB, AdminMenuCB
    for m in options:
        suffix = tr("minutes_short", lang=lang)
        label = f"{m} {suffix}" if suffix else f"{m}"
        if current_min is not None and int(current_min) == m:
            label = f"✅ {label}"
        kb.button(text=label, callback_data=pack_cb(AdminSetHoldCB, minutes=int(m)))
    kb.button(text=t("back", lang), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.adjust(4, 4, 1)
    logger.debug("Меню настройки удержания резерва сгенерировано")
    return kb.as_markup()


def admin_reminder_menu_kb(lang: str = "uk", lead_min: int | None = None, same_day_min: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора напоминаний: основное (за N минут/часов) и в день записи."""
    kb = InlineKeyboardBuilder()
    from bot.app.telegram.common.callbacks import AdminSetReminderCB, AdminSetReminderSameDayCB, NavCB

    lead_options = [1440, 2160, 2880, 4320]
    same_day_options = [60, 120, 180, 240]

    # Основное напоминание
    for m in lead_options:
        if m % 60 == 0:
            hrs = m // 60
            hr_label = tr('hours_short', lang=lang) or 'h'
            label = f"{hrs} {hr_label}"
        else:
            suffix = tr("minutes_short", lang=lang) or "min"
            label = f"{m} {suffix}"
        if lead_min is not None and int(lead_min) == m:
            label = f"✅ {label}"
        kb.button(text=label, callback_data=pack_cb(AdminSetReminderCB, minutes=int(m)))

    kb.button(text=t("disable_reminders", lang=lang), callback_data=pack_cb(AdminSetReminderCB, minutes=0))

    # Напоминание в день записи
    for m in same_day_options:
        if m % 60 == 0:
            hrs = m // 60
            hr_label = tr('hours_short', lang=lang) or 'h'
            label = f"{hrs} {hr_label}"
        else:
            suffix = tr("minutes_short", lang=lang) or "min"
            label = f"{m} {suffix}"
        if same_day_min is not None and int(same_day_min) == m:
            label = f"✅ {label}"
        kb.button(text=label, callback_data=pack_cb(AdminSetReminderSameDayCB, minutes=int(m)))
    kb.button(text=t("disable_same_day_reminders", lang=lang), callback_data=pack_cb(AdminSetReminderSameDayCB, minutes=0))

    kb.button(text=t("back", lang), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.adjust(4, 1, 4, 1)
    logger.debug("Меню настройки времени напоминания сгенерировано")
    return kb.as_markup()


def admin_expire_menu_kb(lang: str = "uk", current_expire: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора частоты проверки просроченных броней (в секундах)."""
    kb = InlineKeyboardBuilder()
    # options in seconds: 1m,5m,15m,30m,1h,2h,6h,1d
    options = [60, 5 * 60, 15 * 60, 30 * 60, 60 * 60, 2 * 60 * 60, 6 * 60 * 60, 24 * 60 * 60]
    from bot.app.telegram.common.callbacks import AdminSetExpireCB
    # Current value should be provided by the caller (handler). UI must not access DB here.

    for s in options:
        if s >= 86400 and s % 86400 == 0:
                lbl = f"{s // 86400} {t('day', lang=lang)}"
        elif s >= 3600 and s % 3600 == 0:
            lbl = f"{s // 3600} {tr('hours_short', lang=lang) or 'h'}"
        elif s >= 60 and s % 60 == 0:
            lbl = f"{s // 60} {tr('minutes_short', lang=lang) or 'min'}"
        else:
            lbl = f"{s} s"
        kb.button(text=(f"✅ {lbl}" if current_expire == s else lbl), callback_data=pack_cb(AdminSetExpireCB, seconds=int(s)))
    # Keep navigation consistent with other per-setting menus (back to Business settings)
    kb.button(text=t("back", lang), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.adjust(4, 4, 1)
    logger.debug("Меню настройки частоты проверки просроченных броней сгенерировано")
    return kb.as_markup()


def admin_cancel_menu_kb(lang: str = "uk", current_min: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора окна запрета отмены (в минутах)."""
    kb = InlineKeyboardBuilder()
    options = [30, 60, 120, 180, 240, 360, 720, 1440]
    from bot.app.telegram.common.callbacks import AdminSetCancelCB, AdminMenuCB
    for m in options:
        label = format_minutes_short(m, lang)
        if current_min is not None and int(current_min) == m:
            label = f"✅ {label}"
        kb.button(text=label, callback_data=pack_cb(AdminSetCancelCB, minutes=int(m)))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.adjust(4, 4, 1)
    logger.debug("Меню настройки окна отмены (минуты) сгенерировано")
    return kb.as_markup()


def admin_reschedule_menu_kb(lang: str = "uk", current_min: int | None = None) -> InlineKeyboardMarkup:
    """Меню выбора окна запрета переноса (в минутах)."""
    kb = InlineKeyboardBuilder()
    options = [30, 60, 120, 180, 240, 360, 720, 1440]
    from bot.app.telegram.common.callbacks import AdminSetRescheduleCB, AdminMenuCB
    for m in options:
        label = format_minutes_short(m, lang)
        if current_min is not None and int(current_min) == m:
            label = f"✅ {label}"
        kb.button(text=label, callback_data=pack_cb(AdminSetRescheduleCB, minutes=int(m)))
    kb.button(text=tr("back", lang=lang), callback_data=pack_cb(AdminMenuCB, act="settings_business"))
    kb.adjust(4, 4, 1)
    logger.debug("Меню настройки окна переноса (минуты) сгенерировано")
    return kb.as_markup()


# The old bookings filter keyboard has been removed: handlers now use the



def get_admin_bookings_dashboard_kb(
    lang: str = "uk",
    mode: str = "upcoming",
    page: int = 1,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    """Delegator: use shared build_bookings_dashboard_kb for admin dashboard."""
    try:
        from bot.app.telegram.client.client_keyboards import build_bookings_dashboard_kb

        meta = {
            "mode": mode,
            "page": int(page or 1),
            "total_pages": int(total_pages or 1),
        }
        return build_bookings_dashboard_kb("admin", meta, lang=lang)

    except Exception:
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
        from bot.app.telegram.common.callbacks import pack_cb, AdminMenuCB

        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=tr("back", lang=lang),
                        callback_data=pack_cb(AdminMenuCB, act="panel"),
                    )
                ]
            ]
        )


def stats_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню статистики.

    Returns:
        InlineKeyboardMarkup с кнопками статистики.
    """
    logger.debug("Меню статистики сгенерировано")
    return get_simple_kb(
        [
            (tr("stats_week", lang=lang), pack_cb(AdminMenuCB, act="stats_range_week")),
            (tr("stats_month", lang=lang), pack_cb(AdminMenuCB, act="stats_range_month")),
            (tr("stats_by_master", lang=lang), pack_cb(AdminMenuCB, act="stats_by_master")),
            (tr("stats_by_service", lang=lang), pack_cb(AdminMenuCB, act="stats_by_service")),
            (tr("back", lang=lang), pack_cb(AdminMenuCB, act="analytics")),
        ],
        cols=2,
    )


def biz_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    """Генерирует меню бизнес-аналитики.

    Returns:
        InlineKeyboardMarkup с кнопками аналитики.
    """
    logger.debug("Меню бизнес-аналитики сгенерировано")
    return get_simple_kb(
        [
            (tr("revenue", lang=lang), pack_cb(AdminMenuCB, act="biz_rev")),
            (tr("retention", lang=lang), pack_cb(AdminMenuCB, act="biz_ret")),
            (tr("no_show", lang=lang), pack_cb(AdminMenuCB, act="biz_ns")),
            (tr("top_ltv", lang=lang), pack_cb(AdminMenuCB, act="biz_ltv")),
            (tr("back", lang=lang), pack_cb(AdminMenuCB, act="analytics")),
        ],
        cols=2,
    )


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
    # Choose an appropriate Back target depending on prefix (delete flows go to CRUD)
    try:
        if isinstance(prefix, str) and prefix.startswith("del_master"):
            back_cb = pack_cb(AdminMenuCB, act="manage_masters")
        elif isinstance(prefix, str) and prefix.startswith("del_service"):
            back_cb = pack_cb(AdminMenuCB, act="manage_services")
        else:
            back_cb = pack_cb(NavCB, act="back")
    except Exception:
        back_cb = pack_cb(NavCB, act="back")
    builder.button(text=tr("back", lang=lang), callback_data=back_cb)
    builder.adjust(2, 1)
    logger.debug("Клавиатура пагинации сгенерирована: prefix=%s, page=%d, total_pages=%d", prefix, page, total_pages)
    return builder.as_markup()


__all__ = [
    "admin_menu_kb",
    "admin_settings_kb",
    "admin_hold_menu_kb",
    "services_list_kb",
    "services_prices_kb",
    "edit_price_kb",
    "stats_menu_kb",
    "biz_menu_kb",
    "pagination_kb",
]