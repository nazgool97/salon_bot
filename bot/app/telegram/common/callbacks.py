from __future__ import annotations
from typing import Any, get_origin, get_args, cast
import types
from aiogram.filters.callback_data import CallbackData

__all__ = [
    "create_callback_data",
    "pack_cb",
    "AdminEditSettingCB",
    "MasterMenuCB",
    "MasterBookingsCB",
    "ClientInfoCB",
    "MasterClientNoteCB",
    "ServiceSelectCB",
    "MasterSelectCB",
    "MasterProfileCB",  
    "MasterServicesCB",
    "MastersListCB",
    "GenericConfirmCB",
    "CalendarCB",
    "DateCB",
    "TimeCB",
    "BackToCalendarCB",
    "BackToMastersCB",
    "FindNearestDayCB",
    "MastersAltTomorrowCB",
    "BookingActionCB",
    "PayCB",
    "FeedbackCB",
    "PaymentCB",
    "BookingCB",
    "RescheduleCB",
    "BackCB",
    "MasterScheduleCB",
]

def create_callback_data(prefix: str, **fields: Any) -> type[CallbackData]:
    """
    Динамически создаёт класс CallbackData с указанным префиксом и полями.
    Aiogram 3.x требует, чтобы prefix передавался в __init_subclass__.
    Класс поддерживает инициализацию через keyword-аргументы.
    """
    # Let CallbackData / pydantic handle attribute initialization.
    # Only provide annotations; do not override __init__ which would bypass
    # pydantic internals and cause __pydantic_fields_set__ errors.
    # Build annotations and set default None for optional fields (e.g., int | None)
    annotations: dict[str, Any] = {}
    namespace: dict[str, Any] = {}
    for fname, ftype in fields.items():
        annotations[fname] = ftype
        # If the annotation allows None (Optional[...] or X | None), set a default of None
        origin = get_origin(ftype)
        args = get_args(ftype)
        allows_none = False
        if args and type(None) in args:
            allows_none = True
        # In some Python versions unions are represented using types.UnionType
        if getattr(ftype, "__args__", None) and type(None) in getattr(ftype, "__args__", ()): 
            allows_none = True
        if allows_none:
            namespace[fname] = None
    namespace["__annotations__"] = annotations
    return type(
        f"{prefix.capitalize()}CB",
        (CallbackData,),
        namespace,
        prefix=prefix,
    )


def pack_cb(cb_cls: type[CallbackData], **kwargs: Any) -> str:
    """Pack a CallbackData instance produced by the dynamic factory.

    The project uses a runtime factory to create CallbackData subclasses which
    works correctly at runtime but can confuse static analyzers/IDEs. Callers
    should use this helper instead of repeating `cast(Any, XCB)(...).pack()`.

    Example:
        payload = pack_cb(ClientMenuCB, act="booking_service")

    This centralizes the cast and makes intent clearer to future readers.
    """
    # Use typing.cast to quiet static analyzers about the runtime-created class
    # The runtime-created CallbackData subclass's .pack() returns str at runtime,
    # but static analyzers treat it as Any; cast to str for clarity.
    return cast(str, cast(Any, cb_cls)(**kwargs).pack())

# --- Определения callback'ов через фабрику ---

MasterMenuCB = create_callback_data("mm", act=str, page=int | None)
ClientInfoCB = create_callback_data("cinfo", user_id=int)
ServiceSelectCB = create_callback_data("svc", service_id=str)
MasterSelectCB = create_callback_data("ms", service_id=str, master_id=int)
MasterProfileCB = create_callback_data("mp", service_id=str, master_id=int)  # Новый callback для профиля
MasterServicesCB = create_callback_data("msvc", master_id=int)  # Список услуг мастера для клиентского флоу
MastersListCB = create_callback_data("mlist", page=int)
ServiceToggleCB = create_callback_data("svc_toggle", service_id=str)
MasterMultiCB = create_callback_data("master_multi", master_id=int)
MasterInfoCB = create_callback_data("master_info", master_id=int)
CalendarCB = create_callback_data("cal", service_id=str, master_id=int, year=int, month=int)
DateCB = create_callback_data("date", service_id=str, master_id=int, date=str)
TimeCB = create_callback_data("time", service_id=str, master_id=int, date=str, time=str)
BackToCalendarCB = create_callback_data("bcal", service_id=str, master_id=int)
BackToMastersCB = create_callback_data("mb", service_id=str)
FindNearestDayCB = create_callback_data("fnd", service_id=str, master_id=int)
MastersAltTomorrowCB = create_callback_data("mat", service_id=str)

# Новые callback'и
BookingActionCB = create_callback_data("ba", act=str, booking_id=int)
BookingCB = create_callback_data("booking", action=str, booking_id=int)
RescheduleCB = create_callback_data("reschedule", action=str, booking_id=int, master_id=int | None, date=str | None, time=str | None)
# Quick reschedule shortcuts (used by master UI quick-actions)
QuickRescheduleCB = create_callback_data("rq", offset_days=int, booking_id=int)
PayCB = create_callback_data("pay", action=str, booking_id=int)
CreateBookingCB = create_callback_data("create", service_id=str, master_id=int, date=str, time=str)
FeedbackCB = create_callback_data("fbk", action=str, booking_id=int | None)
# Rating callback: rating selected for a booking (1..5)
RatingCB = create_callback_data("rate", booking_id=int, rating=int)
BackCB = create_callback_data("back", action=str, target=str | None, booking_id=int | None)

# Master schedule editing callback (masters edit their weekly windows)
MasterScheduleCB = create_callback_data("msch", action=str, day=int | None, time=str | None, idx=int | None)

# Unified navigation callback for three canonical back actions:
# - act="root"       -> return to global root (client main menu)
# - act="back"       -> single-step back via nav stack
# - act="role_root"  -> return to role-specific root (admin/master/client)
NavCB = create_callback_data("nav", act=str)

MasterClientNoteCB = create_callback_data("mcn", action=str, user_id=int)

# Generic client menu actions (replaces literal string callback_data like "booking_service")
ClientMenuCB = create_callback_data("cmenu", act=str)

# Admin-specific callback data
AdminEditPriceCB = create_callback_data("admin_edit_price", service_id=str)
AdminSetPriceCB = create_callback_data("admin_set_price", service_id=str)
AdminPriceAdjCB = create_callback_data("admin_price_adj", service_id=str, delta=int)
AdminSetCurrencyCB = create_callback_data("admin_set_currency", service_id=str)
ConfirmDelServiceCB = create_callback_data("confirm_del_service", service_id=str)
ExecDelServiceCB = create_callback_data("exec_del_service", service_id=str)
# Paging and deletion/admin selections
BookingsPageCB = create_callback_data("bookings_page", page=int)
DelMasterPageCB = create_callback_data("del_master_page", page=int)
ConfirmDelMasterCB = create_callback_data("confirm_del_master", master_id=int)
ExecDelMasterCB = create_callback_data("exec_del_master", master_id=int)
DelServicePageCB = create_callback_data("del_service_page", page=int)
ConfirmCancelAllMasterCB = create_callback_data("confirm_cancel_all_master", master_id=int)
ExecCancelAllMasterCB = create_callback_data("exec_cancel_all_master", master_id=int)
MasterCancelReasonCB = create_callback_data("master_cancel_reason", booking_id=int, code=str)
MasterSetServiceDurationCB = create_callback_data("msdur_set", service_id=str, minutes=int)

# Admin bookings filter (used by admin bookings keyboard)
# Include optional `page` so pagination buttons can use the same callback class.
AdminBookingsCB = create_callback_data("admin_bookings", mode=str, page=int | None)
# Master bookings filter (mode: upcoming|done|no_show|all)
# Include optional `page` so pagination buttons can use the same callback class.
MasterBookingsCB = create_callback_data("master_bookings", mode=str, page=int | None)
# Client 'my bookings' callback (mode: upcoming|completed|all)
# Include `page` so pagination buttons carry the target page number.
MyBookingsCB = create_callback_data("my_bookings", mode=str | None, page=int | None)

# Admin top-level menu navigation
AdminMenuCB = create_callback_data("admin_menu", act=str)

# Admin master card callback: used when admin selects a specific master to manage
AdminMasterCardCB = create_callback_data("admin_master_card", master_id=int)

# Unified callback for editing atomic settings values via shared handler
AdminEditSettingCB = create_callback_data("admin_edit_setting", setting_key=str)

# Selection callbacks for linking/unlinking
SelectLinkMasterCB = create_callback_data("select_link_master", master_id=int)
SelectLinkServiceCB = create_callback_data("select_link_service", service_id=str)
SelectUnlinkMasterCB = create_callback_data("select_unlink_master", master_id=int)
SelectUnlinkServiceCB = create_callback_data("select_unlink_service", service_id=str)

# View links callbacks (admin): select master/service to view linked counterparts
SelectViewMasterCB = create_callback_data("select_view_master", master_id=int)
SelectViewServiceCB = create_callback_data("select_view_service", service_id=str)

# Admin management callbacks
ConfirmDelAdminCB = create_callback_data("confirm_del_admin", admin_id=int)
ExecDelAdminCB = create_callback_data("exec_del_admin", admin_id=int)

# Admin set options
AdminSetHoldCB = create_callback_data("admin_set_hold", minutes=int)
AdminSetCancelCB = create_callback_data("admin_set_cancel", hours=int)
# Admin expiration check frequency (seconds)
AdminSetExpireCB = create_callback_data("admin_set_expire", seconds=int)

# Admin reminder lead-time (minutes)
AdminSetReminderCB = create_callback_data("admin_set_reminder", minutes=int)

# Global currency setter (admin) — pick from a fixed whitelist
AdminSetGlobalCurrencyCB = create_callback_data("admin_set_currency_global", code=str)

# Working hours pickers: start hour, then end hour
AdminSetWorkStartCB = create_callback_data("admin_set_work_start", hour=int)
AdminSetWorkEndCB = create_callback_data("admin_set_work_end", start=int, hour=int)
AdminWorkHoursDayCB = create_callback_data("admin_work_hours_day", day=int)
AdminWorkHoursStartCB = create_callback_data("admin_work_hours_start", day=int, hour=int)
AdminWorkHoursEndCB = create_callback_data("admin_work_hours_end", day=int, start=int, hour=int)
AdminWorkHoursClosedCB = create_callback_data("admin_work_hours_closed", day=int)

# Service-level currency set (picker buttons)
AdminSetServiceCurrencyCB = create_callback_data("admin_set_service_currency", service_id=str, code=str)

# Алиас для обратной совместимости
PaymentCB = PayCB

# Convenience typed CallbackData classes for common actions used across handlers
# These are created via the factory above to ensure prefix consistency and avoid
# scattering magic strings across the codebase.
PaymentActionCB = PayCB  # alias
RescheduleActionCB = RescheduleCB  # alias

# Generic confirm callback for lightweight universal confirmation dialogs
GenericConfirmCB = create_callback_data("generic_confirm", model_type=str, model_id=str)

# Forwarded user quick-actions (admin forwards a user's message).
# Replaces legacy string callbacks like "__fast__:make_admin:123".
# Fields:
#   action: make_admin | make_master | view_master | view_client
#   user_id: telegram user id target
AdminLookupUserCB = create_callback_data("admin_lookup_user", action=str, user_id=int)