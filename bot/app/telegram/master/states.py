from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class MasterStates(StatesGroup):
    """States used by master flows (kept minimal)."""

    edit_note = State()
    cancel_reason_text = State()
    # Settings editing
    settings_menu = State()
    settings_edit_profile = State()
    settings_edit_hours = State()
    settings_edit_hours_start = State()
    settings_edit_hours_end = State()
    settings_edit_autorules = State()


from aiogram.fsm.state import State, StatesGroup


class AdminStates(StatesGroup):
    # Master management
    add_master_name = State()
    add_master_id = State()
    delete_master = State()

    # Service management
    add_service_id = State()
    add_service_name = State()
    delete_service = State()

    # Linking master <-> service
    link_master_service_select_master = State()
    link_master_service_select_service = State()
    unlink_master_service_select_master = State()
    unlink_master_service_select_service = State()

    # Onboarding flow for invited master
    invite_master_wait_id = State()
    invite_master_wait_name = State()
    invite_master_select_services = State()


class MasterRescheduleStates(StatesGroup):
    reschedule_select_date = State()
    reschedule_select_time = State()


class MasterScheduleStates(StatesGroup):
    schedule_menu = State()
    schedule_set_weekday = State()
    schedule_wait_hours = State()
    # New states for the enhanced schedule editor
    schedule_adding_window = State()
    schedule_adding_window_end = State()
    schedule_copy_src_selection = State()
    schedule_preview = State()
    # Exceptions handling (date-based overrides)
    schedule_add_exception = State()
    schedule_add_exception_receive = State()
