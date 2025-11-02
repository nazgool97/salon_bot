from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.app.telegram.common.callbacks import pack_cb, MasterMenuCB, MasterScheduleCB, NavCB
from bot.app.translations import t, tr
from typing import Any, cast
import logging

logger = logging.getLogger(__name__)


def get_master_main_menu() -> InlineKeyboardMarkup:
	"""Main menu for master: Schedule, My bookings, Statistics, Back/Exit."""
	logger.debug("Building master main menu keyboard")
	builder = InlineKeyboardBuilder()
	builder.button(text=t("master_schedule_button"), callback_data=pack_cb(MasterMenuCB, act="schedule"))
	builder.button(text=t("my_bookings_button"), callback_data=pack_cb(MasterMenuCB, act="bookings"))
	builder.button(text=t("master_stats_button"), callback_data=pack_cb(MasterMenuCB, act="stats"))
	# Use role-root back callback so role-specific root (master) is shown via NavCB
	builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="role_root"))
	builder.adjust(2)
	return builder.as_markup()


def get_weekly_schedule_kb(include_edit: bool = True) -> InlineKeyboardMarkup:
	"""Keyboard for the weekly schedule view with optional 'Edit schedule' button."""
	# Single builder: weekday buttons + big actions. include_edit adds the
	# "Edit schedule" action which triggers the `edit_schedule_flow` handler.
	builder = InlineKeyboardBuilder()
	weekdays = tr("weekday_short") or ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
	# weekday buttons: Mon..Sun
	for idx, wd in enumerate(weekdays):
		builder.button(text=wd, callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=idx))
	# Action buttons: Refresh, Clear all, Back
	builder.button(text=t("master_refresh_button"), callback_data=pack_cb(MasterScheduleCB, action="refresh"))
	builder.button(text=t("master_clear_all_button"), callback_data=pack_cb(MasterMenuCB, act="clear_all"))
	# Back from the weekly schedule should return to the master role root
	builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="role_root"))

	# Arrange rows: 4,3,1,1,1,1 to match the screenshot (weekday rows + actions)
	builder.adjust(4, 3, 1, 1, 1, 1)
	return builder.as_markup()



def get_schedule_day_preview_kb(day: int, windows: list | None) -> InlineKeyboardMarkup:
	"""Return a keyboard that previews existing windows and allows removing a specific window.

	Each existing window is shown as a button that triggers MasterScheduleCB(action="remove_window", day=..., idx=<i>). 
	There's also an 'Add window' button and a Back button.
	"""
	logger.debug("Building schedule day preview kb for day=%s windows=%s", day, windows)
	builder = InlineKeyboardBuilder()
	# Show each window as a removable row
	if windows:
		for idx, w in enumerate(windows):
			try:
				if isinstance(w, (list, tuple)) and len(w) >= 2:
					label = f"{w[0]}-{w[1]}"
				else:
					label = str(w)
			except Exception:
				label = str(w)
			# prefix with trash emoji to indicate removal
			builder.button(text=f"🗑 {label}", callback_data=pack_cb(MasterScheduleCB, action="remove_window", day=day, idx=idx))
	else:
		builder.button(text=t("master_no_windows"), callback_data=pack_cb(MasterScheduleCB, action="noop", day=day))

	# Add / actions row
	builder.button(text=t("master_add_window_button"), callback_data=pack_cb(MasterScheduleCB, action="add_time", day=day))
	# Back to weekly schedule overview (use role-root NavCB for unified behaviour)
	builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="back"))
	# Layout: each window its own row, then actions row
	# numbers: len(windows) rows + 1 actions row (or 2 if no windows)
	sizes = []
	if windows:
		sizes.extend([1] * len(windows))
	else:
		sizes.append(1)
	sizes.append(2)
	builder.adjust(*sizes)
	return builder.as_markup()


def get_bookings_menu_kb() -> InlineKeyboardMarkup:
	builder = InlineKeyboardBuilder()
	builder.button(text=t("master_today_button"), callback_data=pack_cb(MasterMenuCB, act="bookings_today"))
	builder.button(text=t("tomorrow"), callback_data=pack_cb(MasterMenuCB, act="bookings_tomorrow"))
	builder.button(text=t("this_week"), callback_data=pack_cb(MasterMenuCB, act="bookings_week"))
	builder.button(text=t("master_all_bookings_button"), callback_data=pack_cb(MasterMenuCB, act="bookings_all"))
	builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="role_root"))
	builder.adjust(2)
	return builder.as_markup()


def get_stats_kb() -> InlineKeyboardMarkup:
	builder = InlineKeyboardBuilder()
	builder.button(text=t("stats_week"), callback_data=pack_cb(MasterMenuCB, act="stats_week"))
	builder.button(text=t("stats_month"), callback_data=pack_cb(MasterMenuCB, act="stats_month"))
	builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="role_root"))
	builder.adjust(2)
	return builder.as_markup()


__all__ = [
	"get_master_main_menu",
	"get_weekly_schedule_kb",
	"get_bookings_menu_kb",
	"get_stats_kb",
	"get_time_start_kb",
	"get_time_end_kb",
]


def _make_time_list(start_hour: int = 6, end_hour: int = 22, step_min: int = 30) -> list[str]:
	"""Helper: produce list of hh:mm time strings from start_hour to end_hour inclusive with step_min increments."""
	times: list[str] = []
	for h in range(start_hour, end_hour + 1):
		for m in range(0, 60, step_min):
			# Stop if we've passed end_hour and minute > 0
			if h == end_hour and m > 30:
				continue
			times.append(f"{h:02d}:{m:02d}")
	return times


def get_time_start_kb(day: int, *, start_hour: int = 6, end_hour: int = 22, step_min: int = 30) -> InlineKeyboardMarkup:
	"""Keyboard for selecting the start time for a day's window."""
	logger.debug("Building time start kb for day=%s start_hour=%s end_hour=%s step=%s", day, start_hour, end_hour, step_min)
	builder = InlineKeyboardBuilder()
	times = _make_time_list(start_hour, end_hour, step_min)
	for tstr in times:
		# Pack time without ':' because ':' is used as separator by CallbackData.pack()
		token = tstr.replace(":", "")
		builder.button(text=tstr, callback_data=pack_cb(MasterScheduleCB, action="pick_start", day=day, time=token))
	# Back button returns to day actions (so master can continue editing the day)
	builder.button(text=f"{t('back')}", callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=day))
	# Also provide an explicit cancel action that clears FSM flows
	builder.button(text=f"{t('cancel')}", callback_data=pack_cb(MasterScheduleCB, action="cancel", day=day))

	# Arrange times into rows of 4, then the back button as a full-width row
	buttons_count = len(times) + 1
	full_rows = len(times) // 4
	rem = len(times) % 4
	sizes: list[int] = []
	sizes.extend([4] * full_rows)
	if rem:
		sizes.append(rem)
	sizes.append(1)  # back button row
	builder.adjust(*sizes)
	return builder.as_markup()


def get_time_end_kb(day: int, start_time: str, *, end_hour: int = 22, step_min: int = 30) -> InlineKeyboardMarkup:
	"""Keyboard for selecting the end time given a chosen start_time.

	End choices are strictly after start_time with the same step increments.
	"""
	logger.debug("Building time end kb for day=%s start_time=%s end_hour=%s step=%s", day, start_time, end_hour, step_min)
	builder = InlineKeyboardBuilder()
	# compute minutes from midnight
	h, m = map(int, start_time.split(":"))
	start_minutes = h * 60 + m
	times = _make_time_list(0, end_hour, step_min)
	# filter to times strictly greater than start_time
	end_choices = [ts for ts in times if (int(ts.split(":")[0]) * 60 + int(ts.split(":")[1])) > start_minutes]
	for tstr in end_choices:
		# Pack time without ':' to avoid separator conflicts
		token = tstr.replace(":", "")
		builder.button(text=tstr, callback_data=pack_cb(MasterScheduleCB, action="pick_end", day=day, time=token))
	# Back returns to day actions
	builder.button(text=f"{t('back')}", callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=day))
	# Cancel action clears FSM
	builder.button(text=f"{t('cancel')}", callback_data=pack_cb(MasterScheduleCB, action="cancel", day=day))

	# arrange into rows of 4 + final back row
	full_rows = len(end_choices) // 4
	rem = len(end_choices) % 4
	sizes: list[int] = []
	sizes.extend([4] * full_rows)
	if rem:
		sizes.append(rem)
	sizes.append(1)
	builder.adjust(*sizes)
	return builder.as_markup()

