from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.app.telegram.client.client_keyboards import get_simple_kb, build_bookings_dashboard_kb
from bot.app.telegram.common.callbacks import pack_cb, MasterMenuCB, MasterScheduleCB, NavCB
from bot.app.translations import t, tr
from typing import Any, cast
import logging

logger = logging.getLogger(__name__)
from bot.app.services.master_services import build_time_slot_list




def get_master_main_menu(lang: str = "uk") -> InlineKeyboardMarkup:
	"""Main menu for master: Schedule, My bookings, Statistics, Back/Exit.

	Accepts explicit `lang` so callers can localize keyboard labels.
	"""
	logger.debug("Building master main menu keyboard (lang=%s)", lang)
	builder = InlineKeyboardBuilder()
	builder.button(text=t("master_schedule_button", lang), callback_data=pack_cb(MasterMenuCB, act="schedule"))
	builder.button(text=t("my_bookings_button", lang), callback_data=pack_cb(MasterMenuCB, act="bookings"))
	builder.button(text=t("master_my_clients_button", lang), callback_data=pack_cb(MasterMenuCB, act="my_clients"))
	builder.button(text=t("master_stats_button", lang), callback_data=pack_cb(MasterMenuCB, act="stats"))
	# New: edit per-service durations
	builder.button(text=t("master_service_durations_button", lang), callback_data=pack_cb(MasterMenuCB, act="service_durations"))
	# Back from master main menu should return to role_root (client/main)
	# Use NavCB(role_root) so nav_role_root can decide the proper target.
	builder.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="role_root"))
	builder.adjust(2, 2, 1)
	return builder.as_markup()


def get_weekly_schedule_kb(include_edit: bool = True, lang: str = "uk") -> InlineKeyboardMarkup:
	"""Keyboard for the weekly schedule view with optional 'Edit schedule' button."""
	# Single builder: weekday buttons + big actions. include_edit adds the
	# "Edit schedule" action which triggers the `edit_schedule_flow` handler.
	builder = InlineKeyboardBuilder()
	weekdays = tr("weekday_short", lang=lang) or ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
	# weekday buttons: Mon..Sun
	for idx, wd in enumerate(weekdays):
		builder.button(text=wd, callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=idx))
	# Action buttons: Refresh, Clear all, Back
	builder.button(text=t("master_refresh_button", lang), callback_data=pack_cb(MasterScheduleCB, action="refresh"))
	builder.button(text=t("master_clear_all_button", lang), callback_data=pack_cb(MasterMenuCB, act="clear_all"))
	# Back from the weekly schedule should explicitly return to the master menu
	builder.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))

	# Arrange rows: 4,3,1,1,1,1 to match the screenshot (weekday rows + actions)
	builder.adjust(4, 3, 1, 1, 1, 1)
	return builder.as_markup()



def get_schedule_day_preview_kb(day: int, windows: list | None, lang: str = "uk") -> InlineKeyboardMarkup:
	"""Return a keyboard that previews existing windows and allows removing a specific window.

	Safety: removal is value-based, not index-based, to avoid race conditions. Buttons carry
	a packed time token HHMM-HHMM and the handler matches on values.
	"""
	logger.debug("Building schedule day preview kb for day=%s windows=%s", day, windows)
	builder = InlineKeyboardBuilder()
	# Show each window as a removable row
	if windows:
		for idx, w in enumerate(windows):
			try:
				if isinstance(w, (list, tuple)) and len(w) >= 2:
					label = f"{w[0]}-{w[1]}"
					# Provide both index and value tokens so removal can be value-based (safer under concurrency)
					start_token = str(w[0])
					end_token = str(w[1])
					packed_time = f"{start_token.replace(':','')}-{end_token.replace(':','')}"
				else:
					label = str(w)
					start_token = None
					end_token = None
					packed_time = None
			except Exception:
				label = str(w)
				start_token = None
				end_token = None
				packed_time = None
			# prefix with trash emoji to indicate removal
			if packed_time:
				# Value-based removal: only pass the time token; ignore indices.
				builder.button(text=f"🗑 {label}", callback_data=pack_cb(MasterScheduleCB, action="remove_window", day=day, time=packed_time))
			else:
				# Fallback: include only day; handler will show a refresh hint if value token is missing.
				builder.button(text=f"🗑 {label}", callback_data=pack_cb(MasterScheduleCB, action="remove_window", day=day))
	else:
		builder.button(text=t("master_no_windows", lang), callback_data=pack_cb(MasterScheduleCB, action="noop", day=day))

	# Add / actions row
	builder.button(text=t("master_add_window_button", lang), callback_data=pack_cb(MasterScheduleCB, action="add_time", day=day))
	# Back to weekly schedule overview (use role-root NavCB for unified behaviour)
	builder.button(text=t("back", lang), callback_data=pack_cb(NavCB, act="back"))
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


def get_bookings_menu_kb(lang: str = "uk") -> InlineKeyboardMarkup:
	items = [
		(t("master_today_button", lang), pack_cb(MasterMenuCB, act="bookings_today")),
		(t("tomorrow", lang), pack_cb(MasterMenuCB, act="bookings_tomorrow")),
		(t("this_week", lang), pack_cb(MasterMenuCB, act="bookings_week")),
		(t("master_all_bookings_button", lang), pack_cb(MasterMenuCB, act="bookings_all")),
		(t("back", lang), pack_cb(MasterMenuCB, act="menu")),
	]
	return get_simple_kb(items, cols=2)


def get_stats_kb(lang: str = "uk") -> InlineKeyboardMarkup:
	items = [
		(t("stats_week", lang), pack_cb(MasterMenuCB, act="stats_week")),
		(t("stats_month", lang), pack_cb(MasterMenuCB, act="stats_month")),
		(t("back", lang), pack_cb(MasterMenuCB, act="menu")),
	]
	return get_simple_kb(items, cols=2)


__all__ = [
	"get_master_main_menu",
	"get_weekly_schedule_kb",
	"get_bookings_menu_kb",
	"get_stats_kb",
	"get_time_start_kb",
	"get_time_end_kb",
]


def get_master_bookings_dashboard_kb(lang: str = "uk", mode: str = "upcoming", page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
	"""Delegator: use shared build_bookings_dashboard_kb for master dashboard."""
	meta = {"mode": mode, "page": int(page or 1), "total_pages": int(total_pages or 1)}
	return build_bookings_dashboard_kb("master", meta, lang=lang)


from bot.app.core.constants import (
	DEFAULT_DAY_END_HOUR,
	DEFAULT_TIME_STEP_MINUTES,
)


def get_time_start_kb(day: int, *, times: list[str] | None = None, lang: str = "uk") -> InlineKeyboardMarkup:
	"""Keyboard for selecting the start time for a day's window."""
	logger.debug("Building time start kb for day=%s times=%s", day, times)
	builder = InlineKeyboardBuilder()
	times = times or build_time_slot_list()
	for tstr in times:
		# Pack time without ':' because ':' is used as separator by CallbackData.pack()
		token = tstr.replace(":", "")
		builder.button(text=tstr, callback_data=pack_cb(MasterScheduleCB, action="pick_start", day=day, time=token))
	builder.adjust(4)
	# Back button returns to day actions (so master can continue editing the day)
	try:
		back_button = InlineKeyboardButton(text=f"{t('back', lang=lang)}", callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=day))
		builder.row(back_button)
	except Exception as e:
		logger.exception("get_time_start_kb: failed to append back_button: %s", e)
		raise
	return builder.as_markup()


def get_time_end_kb(day: int, start_time: str, *, items: list[tuple[str, str]], end_hour: int = DEFAULT_DAY_END_HOUR, step_min: int = DEFAULT_TIME_STEP_MINUTES, lang: str = "uk") -> InlineKeyboardMarkup:
	"""Keyboard for selecting the end time given a chosen start_time.

	End choices are strictly after start_time with the same step increments.
	"""
	logger.debug("Building time end kb for day=%s start_time=%s end_hour=%s step=%s", day, start_time, end_hour, step_min)

	# Allow callers to provide precomputed items (label, callback_data).
	# If items are not provided we delegate computation to master_services
	# so that all time logic lives in the service layer.
	def _build(items: list[tuple[str, str]] | None = None) -> InlineKeyboardMarkup:
		builder = InlineKeyboardBuilder()
		it = list(items or [])
		for label, cb in it:
			try:
				builder.button(text=label, callback_data=cb)
			except Exception:
				# skip problematic entry
				continue
		# Back returns to day actions
		try:
			builder.adjust(4)
		except Exception:
			pass
		try:
			back_button = InlineKeyboardButton(text=f"{t('back', lang=lang)}", callback_data=pack_cb(MasterScheduleCB, action="edit_day", day=day))
			builder.row(back_button)
		except Exception:
			pass
		return builder.as_markup()

	# Build from precomputed items (handlers compute via service layer)
	return _build(items)

