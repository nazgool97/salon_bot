from typing import ClassVar

from aiogram.fsm.state import State, StatesGroup


class AdminStates(StatesGroup):
    """FSM states used by admin flows.

    Each FSM state attribute is annotated as ClassVar[State] so static type
    checkers (mypy) treat them as class-level constants. Keep names stable
    because handlers import these attributes directly.
    """

    # --- Master management
    add_master_name: ClassVar[State] = State()
    add_master_id: ClassVar[State] = State()
    delete_master: ClassVar[State] = State()

    # --- Service management
    add_service_id: ClassVar[State] = State()
    add_service_name: ClassVar[State] = State()
    delete_service: ClassVar[State] = State()

    # --- Linking master <-> service
    link_master_service_select_master: ClassVar[State] = State()
    link_master_service_select_service: ClassVar[State] = State()
    unlink_master_service_select_master: ClassVar[State] = State()
    unlink_master_service_select_service: ClassVar[State] = State()

    # --- Onboarding flow for invited master
    invite_master_wait_id: ClassVar[State] = State()
    invite_master_wait_name: ClassVar[State] = State()
    invite_master_select_services: ClassVar[State] = State()

    # --- Confirmations / pagination helpers
    confirm_delete_master: ClassVar[State] = State()
    confirm_delete_service: ClassVar[State] = State()
    confirm_link_master_service: ClassVar[State] = State()
    confirm_unlink_master_service: ClassVar[State] = State()

    # --- Price / currency editing flow
    waiting_for_price: ClassVar[State] = State()
    set_price: ClassVar[State] = State()
    # Removed obsolete currency states (waiting_for_currency, set_currency) now handled via picker callbacks only.
    # Manual currency entry states
    enter_currency: ClassVar[State] = State()

    # Additional admin states
    add_master_extra: ClassVar[State] = State()
    admin_misc: ClassVar[State] = State()
    # Phone/settings editing state
    edit_setting_text: ClassVar[State] = State()
    # (wizard states removed)