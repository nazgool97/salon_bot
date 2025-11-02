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
