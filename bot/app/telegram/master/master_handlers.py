from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from sqlalchemy import select
from datetime import datetime, timezone
# FIX: Removed direct import for LOCAL_TZ - use cfg lookup later

from bot.app.telegram.common.roles import MasterRoleFilter, ensure_master
from bot.app.telegram.master.states import MasterScheduleStates, MasterStates

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import Any, cast
from bot.app.telegram.common.callbacks import pack_cb, MasterMenuCB, MasterScheduleCB, BookingActionCB, BookingsPageCB, NavCB
# FIX: Removed direct import for BOOKINGS_PAGE_SIZE
# from bot.config import BOOKINGS_PAGE_SIZE
import bot.config as cfg
# Local timezone fallback (use cfg.LOCAL_TZ when available)
LOCAL_TZ = getattr(cfg, "LOCAL_TZ", timezone.utc)
from bot.app.services.shared_services import build_booking_details, format_booking_details_text, get_service_name
from sqlalchemy.exc import SQLAlchemyError
from aiogram.exceptions import TelegramAPIError
from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.navigation import nav_back, nav_push, nav_replace, nav_reset
from bot.app.telegram.master.master_keyboards import (
	get_master_main_menu,
	get_weekly_schedule_kb,
	get_schedule_day_preview_kb,
	get_bookings_menu_kb,
	get_stats_kb,
	get_time_start_kb,
	get_time_end_kb,
)
from bot.app.core.db import get_session
from bot.app.domain.models import MasterProfile, Booking, User, BookingStatus, Service
import json
import re
from bot.app.translations import t, tr
 
# Router and logger expected by other modules and decorators in this file
master_router = Router(name="master")
logger = logging.getLogger(__name__)

# Apply role filter, locale middleware and error handlers so individual
# handlers don't need to call `ensure_master` or manage locale lookup.
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
from bot.app.telegram.common.errors import handle_telegram_error, handle_db_error
from bot.app.telegram.common.roles import MasterRoleFilter

# Применяем фильтр ролей (Шаг 1)
master_router.message.filter(MasterRoleFilter())
master_router.callback_query.filter(MasterRoleFilter())

# Подключаем LocaleMiddleware (Шаг 2)
master_router.message.middleware(LocaleMiddleware())
master_router.callback_query.middleware(LocaleMiddleware())

# Регистрируем обработчики ошибок (Шаг 3)
try:
	master_router.errors.register(handle_telegram_error)
	master_router.errors.register(handle_db_error)
except AttributeError:
	logger.debug("Master router: error handler registration skipped...")

# Centralized service layer used by many handlers
from bot.app.services import master_services

# Locale lookups are provided by LocaleMiddleware; handlers receive `locale: str`.
# The legacy _safe_get_locale wrapper was removed as part of migration.


@master_router.callback_query(MasterMenuCB.filter(F.act == "menu"))
async def show_master_menu(cb: CallbackQuery, state: FSMContext) -> None:
	"""Show top-level master menu and reset navigation stack."""
	kb = get_master_main_menu()
	await safe_edit(cb.message, text=t("master_menu_header"), reply_markup=kb)
	try:
		# reset nav stack: main menu is the root
		await nav_reset(state)
		await nav_replace(state, t("master_menu_header"), kb)
		# mark preferred role for later role-root navigation
		try:
			await state.update_data(preferred_role="master")
		except AttributeError:
			pass
	except AttributeError:
		pass

@master_router.callback_query(MasterMenuCB.filter(F.act == "schedule"))
async def show_schedule(cb: CallbackQuery, state: FSMContext) -> None:
	"""Show weekly schedule (placeholder)."""
	logger.debug("show_schedule invoked by user=%s data=%s", getattr(getattr(cb, 'from_user', None), 'id', None), getattr(cb, 'data', None))
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	try:
		if master_id is None:
			raise ValueError("missing master id")
		# 1. get canonical schedule dict
		sched = await master_services.get_master_schedule(int(master_id))
		# 2. render into human-readable table
		schedule_text = master_services.render_schedule_table(sched)
	except SQLAlchemyError:
		logger.exception("Failed to load/render schedule for master %s", master_id)
		schedule_text = t("error_loading_schedule")

	kb = get_weekly_schedule_kb()
	base_text = t("master_schedule_week_overview")
	full_text = f"{base_text}\n\n{schedule_text}"
	await safe_edit(cb.message, text=full_text, reply_markup=kb)
	try:
		await nav_push(state, full_text, kb)
	except AttributeError:
		pass
	try:
		# acknowledge the callback so the client shows immediate feedback
		await cb.answer()
	except TelegramAPIError:
		pass

@master_router.callback_query(MasterScheduleCB.filter(F.action == "edit_day"))
async def schedule_edit_day(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
	# Delegate rendering to shared helper so other places (picker completion)
	# can return the master to the day-actions screen without duplicating code.
	raw = cb.data or ""
	logger.debug("schedule_edit_day invoked raw=%s from=%s", raw, getattr(getattr(cb, 'from_user', None), 'id', None))

	# Ensure caller is a master


	# Extract day from parsed callback data (aiogram provides typed callback_data)
	day = None
	try:
		d = getattr(callback_data, "day", None)
		if d is not None:
			day = int(d)
	except (TypeError, ValueError):
		day = None

	if day is None:
		try:
			await cb.answer()
		except TelegramAPIError:
			pass
		return

	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	text, kb = await _show_day_actions(cb.message, int(master_id) if master_id is not None else 0, int(day))
	try:
		await nav_push(state, text, kb)
	except TelegramAPIError:
		# ignore UI/navigation failures
		pass


async def _show_day_actions(message, master_id: int, day: int):
	"""Helper to show the action screen for a specific day.

	Renders the current slots for `day` and shows the day-actions keyboard.
	"""
	# Use a preview keyboard that shows current windows with per-window remove buttons
	# and an Add button for convenience.
	sched = None
	try:
		sched = await master_services.get_master_schedule(int(master_id) if master_id is not None else 0)
		day_slots = sched.get(str(day)) if isinstance(sched, dict) else None
	except SQLAlchemyError:
		day_slots = None
	kb = get_schedule_day_preview_kb(day, day_slots)

	# Load the actual schedule for this master
	sched_text = "(немає даних)"
	try:
		sched = await master_services.get_master_schedule(int(master_id) if master_id is not None else 0)
		day_slots = sched.get(str(day)) if isinstance(sched, dict) else None
		if isinstance(day_slots, list) and len(day_slots) > 0:
			sched_text = ", ".join([f"{w[0]}-{w[1]}" for w in day_slots if isinstance(w, (list, tuple)) and len(w) >= 2])
		else:
			sched_text = t("master_no_windows")
	except SQLAlchemyError:
		sched_text = "(помилка читання розкладу)"

	day_names = tr("weekday_full") or ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
	text = f"{day_names[day]}: {sched_text}"
	await safe_edit(message, text=text, reply_markup=kb)
	return text, kb

@master_router.callback_query(MasterScheduleCB.filter())
async def schedule_actions(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
	"""Handle schedule actions like add_time/clear/make_off/back_to_choose.

	This handler is defensive: `CallbackData.parse` can fail in some
	runtime/static analyzer situations, so we fall back to a simple
	string parser for callback payloads like 'msch:action=add_time:day=2'.
	"""
	action = None
	day = None
	raw = cb.data or ""


	# If callback_data is not provided or empty, bail out early
	if not callback_data:
		try:
			await cb.answer()
		except TelegramAPIError:
			pass
		return

	# Use parsed callback_data provided by aiogram
	action = getattr(callback_data, "action", None)
	day = getattr(callback_data, "day", None)

	# Normalize day to int where possible (leave None if unparsable)
	try:
		if isinstance(day, str) and str(day).isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			# keep None for invalid values
			day = None
	except (TypeError, ValueError):
		day = None

	if action == "add_time":
		# Start interactive time-picker: show start-time keyboard
		try:
			# validate day
			if day is None:
				await cb.answer()
				return
			day_int = int(day)
			start_kb = get_time_start_kb(day_int)
			# set FSM state so we can keep track of the ephemeral flow
			await state.update_data(chosen_day=day_int)
			await state.set_state(MasterScheduleStates.schedule_adding_window)
			await safe_edit(cb.message, text=t("master_select_window_start"), reply_markup=start_kb)
		except (TelegramAPIError, AttributeError):
			logger.exception("Failed to start time picker for add_time")
			# fallback to textual prompt (guarded)
			try:
				if cb.message:
					await cb.message.answer(t("master_select_window_start"))
				else:
					# If no message, at least answer the callback
					await cb.answer(t("error_retry"), show_alert=True)
			except TelegramAPIError:
				pass
		return

	if action == "refresh":
		# Re-fetch and re-render the weekly schedule in-place.
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		try:
			if master_id is None:
				raise ValueError("missing master id")
			# 1. get canonical schedule dict
			sched = await master_services.get_master_schedule(int(master_id))
			# 2. render into human-readable table
			schedule_text = master_services.render_schedule_table(sched)
		except SQLAlchemyError:
			logger.exception("Failed to load/render schedule for master %s", master_id)
			schedule_text = t("error_loading_schedule")

		kb = get_weekly_schedule_kb()
		base_text = t("master_schedule_week_overview")
		full_text = f"{base_text}\n\n{schedule_text}"
		await safe_edit(cb.message, text=full_text, reply_markup=kb)
		try:
			# show a small toast confirming refresh
			await cb.answer("Обновлено")
		except TelegramAPIError:
			pass
		return

	# If callback was a positional/time pick (parsed by fallback) delegate to
	# the dedicated pick handlers so their own fallback parsing runs.
	if action in ("pick_start", "pick_end"):
		logger.debug("Delegating positional pick action '%s' to dedicated handler", action)
		try:
			if action == "pick_start":
				await pick_window_start(cb, callback_data, state)
			else:
				await pick_window_end(cb, callback_data, state)
		except (TypeError, ValueError) as e:
			logger.exception("Parsing error in delegated pick handler for action=%s: %s", action, e)
		except SQLAlchemyError as e:
			logger.exception("DB error in delegated pick handler for action=%s: %s", action, e)
		except TelegramAPIError as e:
			logger.exception("Telegram API error in delegated pick handler for action=%s: %s", action, e)
		await cb.answer()
		return
	if action == "clear_day":
		# Remove day's windows from MasterProfile
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		# FIX: Check for None master_id and day
		if master_id is None or day is None:
			await cb.answer(t("error_retry"), show_alert=True)
			return
		try:
			# Before clearing, check for future bookings that fall into any of the
			# configured windows for this weekday. This is stricter than matching by
			# weekday only and mirrors do_clear_all behaviour.
			# Use centralized service to check for conflicts for this specific day.
			conflicts = await master_services.check_future_booking_conflicts(
				int(master_id), day_to_clear=int(day), horizon_days=365
			)

			if conflicts:
				conflict_list = "\n".join(conflicts)
				await safe_edit(
					cb.message,
					text=tr("master_clear_blocked_existing_bookings", list=conflict_list),
				)
				return

			# Mark the day explicitly as off (empty windows) instead of removing the key.
			# Removing the key causes client code to fall back to default working hours.
			await master_services.set_master_schedule_day(int(master_id), int(day), [])
			# quick toast confirmation
			try:
				await cb.answer(t("toast_day_cleared"))
			except TelegramAPIError:
				# Ignore UI failures for the short toast
				pass
			await safe_edit(cb.message, text=t("master_cleared"))
		except SQLAlchemyError as e:
			# Database/service failures when marking the day
			logger.exception("Failed to clear day %s for master: %s", day, e)
			try:
				await safe_edit(cb.message, text=t("error_retry"))
			except TelegramAPIError:
				# Nothing more we can do if UI also fails
				pass
			return
		except TelegramAPIError as e:
			# Telegram UI failures when editing the message
			logger.exception("Telegram error while clearing day %s for master: %s", day, e)
			return
		return

	if action == "remove_window":
		# Remove specific window by index for the given day
		# Use typed callback_data instead of manual parsing
		idx = getattr(callback_data, "idx", None)
		try:
			if idx is not None:
				idx = int(idx)
		except (TypeError, ValueError):
			# invalid integer conversion
			idx = None

		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		# Ensure we have a valid day parsed earlier
		# FIX: Check for None master_id as well
		if day is None or master_id is None:
			await cb.answer()
			return
		if idx is None:
			await cb.answer()
			return
		try:
			sched = await master_services.get_master_schedule(int(master_id))
			day_slots = sched.get(str(day)) if isinstance(sched, dict) else []
			if not isinstance(day_slots, list) or idx < 0 or idx >= len(day_slots):
				await cb.answer()
				return
			# Before removing this specific window, check for future bookings that
			# fall into this exact interval. We query upcoming bookings for the
			# master and compare their start time to the window being removed.
			try:
				win = day_slots[int(idx)]
			except (IndexError, ValueError, TypeError):
				# invalid index or unexpected window format -> nothing to remove
				await cb.answer()
				return
			# determine a/b interval
			if isinstance(win, (list, tuple)) and len(win) >= 2:
				a, b = win[0], win[1]
			else:
				try:
					a, b = str(win).split("-")
				except (ValueError, TypeError):
					a, b = None, None
			conflicts = []
			if a and b:
				from datetime import timedelta
				now = datetime.now(timezone.utc)
				try:
					bookings = await master_services.get_master_bookings_for_period(int(master_id), start=now, days=365)
				except SQLAlchemyError:
					bookings = []
				for booking in (bookings or []):
					try:
						starts = getattr(booking, "starts_at", None)
						if not starts:
							continue
						if starts.weekday() != int(day):
							continue
						start_min = starts.hour * 60 + starts.minute
						a_h, a_m = map(int, a.split(":"))
						b_h, b_m = map(int, b.split(":"))
						a_min = a_h * 60 + a_m
						b_min = b_h * 60 + b_m
						if start_min >= a_min and start_min < b_min:
							try:
								client, _ = await master_services.enrich_booking_context(booking)
								user_name = getattr(client, "name", None) or f"id:{getattr(client, 'id', '?')}"
							except SQLAlchemyError:
								user_name = f"id:{getattr(booking, 'user_id', '?')}"
							try:
								iso = starts.isoformat()
							except AttributeError:
								iso = starts.strftime('%Y-%m-%d %H:%M') if hasattr(starts, 'strftime') else str(starts)
							conflicts.append(f"#{getattr(booking, 'id', '?')} {iso} — {user_name}")
					except (AttributeError, ValueError, TypeError, SQLAlchemyError):
						continue
				# If conflicts found, refuse deletion and show informative text
				if conflicts:
					conflict_list = "\n".join(conflicts)
					await safe_edit(
						cb.message,
						text=tr("master_clear_blocked_existing_bookings", list=conflict_list),
					)
					return
			# remove window
			day_slots.pop(int(idx))
			# persist
			await master_services.set_master_schedule(int(master_id), sched)
			# notify and refresh preview
			try:
				await cb.answer(t("toast_window_removed"))
			except TelegramAPIError:
				# ignore toast failures
				pass
			text, kb = await _show_day_actions(cb.message, int(master_id) if master_id is not None else 0, int(day))
			try:
				await nav_replace(state, text, kb)
			except TelegramAPIError:
				pass
			return
		except SQLAlchemyError as e:
			logger.exception("Failed to remove window (DB error): %s", e)
			try:
				await safe_edit(cb.message, text=t("error_retry"))
			except TelegramAPIError:
				pass
			return
		except TelegramAPIError as e:
			logger.exception("Telegram API error while removing window: %s", e)
			return

	if action == "cancel":
		# Cancel current FSM flow and return to day actions
		try:
			await state.clear()
		except AttributeError:
			# state API missing/changed — best-effort ignore
			pass
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		# FIX: Check if day is None. If so, can't show day actions.
		if day is None:
			# Fallback to the weekly schedule view
			await show_schedule(cb, state)
			return
		text, kb = await _show_day_actions(cb.message, int(master_id) if master_id is not None else 0, int(day))
		try:
			await nav_replace(state, text, kb)
		except TelegramAPIError:
			pass
		return
	if action == "make_off":
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		# FIX: Check for None master_id and day
		if master_id is None or day is None:
			await cb.answer(t("error_retry"), show_alert=True)
			return
		try:
			# Before marking day off, check for future bookings on that weekday.
			# Use centralized conflict check (bounded horizon)
			conflicts = await master_services.check_future_booking_conflicts(
				int(master_id), day_to_clear=int(day), horizon_days=365
			)
			if conflicts:
				conflict_list = "\n".join(conflicts)
				await safe_edit(
					cb.message,
					text=tr("master_clear_blocked_existing_bookings", list=conflict_list),
				)
				return

			# Use service to mark day as off (empty windows)
			await master_services.set_master_schedule_day(int(master_id), int(day), [])
			# quick toast confirmation
			try:
				await cb.answer(t("toast_day_off_marked"))
			except TelegramAPIError:
				pass
			await safe_edit(cb.message, text=t("master_day_marked_off"))
		except SQLAlchemyError as e:
			logger.exception("Failed to mark day off %s for master (DB error): %s", day, e)
			try:
				await safe_edit(cb.message, text=t("error_retry"))
			except TelegramAPIError:
				pass
			return
		except TelegramAPIError as e:
			logger.exception("Telegram API error while marking day off %s: %s", day, e)
			return
		return
	if action == "back_to_choose":
		# Return user to the weekly schedule overview (with edit entry)
		kb = get_weekly_schedule_kb(include_edit=True)
		await safe_edit(cb.message, text=t("master_schedule_week_overview"), reply_markup=kb)
		return

	# Fallback
	# Log raw callback data when we fall back to unknown — helps diagnose
	raw = getattr(cb, "data", None)
	uid = getattr(getattr(cb, "from_user", None), "id", None)
	logger.warning("Unhandled MasterSchedule action — raw callback=%s user=%s", raw, uid)
	await safe_edit(cb.message, text=t("unknown"))


# Note: master-specific keyboards now use MasterMenuCB(act="menu") for Back
# buttons. This avoids conflict with the client-wide "global_back" handler
# which is registered on the client router. The master menu is handled by
# `show_master_menu` (MasterMenuCB.act == "menu") which resets navigation.

@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings"))
async def show_bookings_menu(cb: CallbackQuery, state: FSMContext) -> None:
    kb = get_bookings_menu_kb()
    text = t("master_bookings_header")
    await safe_edit(cb.message, text=text, reply_markup=kb)
    await nav_replace(state, text, kb)  # Добавьте: обновляем state


@master_router.callback_query(MasterMenuCB.filter(F.act == "clear_all"))
async def confirm_clear_all(cb: CallbackQuery) -> None:
	"""Ask master to confirm clearing the whole weekly schedule."""
	# confirmation keyboard
	from aiogram.utils.keyboard import InlineKeyboardBuilder

	builder = InlineKeyboardBuilder()
	builder.button(text=t("confirm"), callback_data=pack_cb(MasterMenuCB, act="clear_all_confirm"))
	builder.button(text=t("cancel"), callback_data=pack_cb(MasterMenuCB, act="menu"))
	builder.adjust(2)
	await safe_edit(cb.message, text=tr("master_clear_all_confirm"), reply_markup=builder.as_markup())


@master_router.callback_query(MasterMenuCB.filter(F.act == "clear_all_confirm"))
async def do_clear_all(cb: CallbackQuery) -> None:
	"""Perform actual clearing of the schedule JSON in MasterProfile.bio."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	# FIX: Check for None master_id
	if master_id is None:
		await cb.answer(t("error_retry"), show_alert=True)
		return
	try:
		async with get_session() as session:
			prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == int(master_id)))
			if not prof:
				prof = MasterProfile(master_telegram_id=int(master_id), bio=None)
				session.add(prof)
			try:
				bio = json.loads(prof.bio or "{}")
			except (ValueError, TypeError, json.JSONDecodeError):
				bio = {}

			# If schedule exists, check for future bookings that fall into any of the windows
			sched = bio.get("schedule", {}) or {}
			if sched:
				# Use centralized conflict check across all weekdays (bounded horizon)
				conflicts = await master_services.check_future_booking_conflicts(
					int(master_id), clear_all=True, horizon_days=365
				)

				if conflicts:
					conflict_list = "\n".join(conflicts)
					await safe_edit(
						cb.message,
						text=tr("master_clear_blocked_existing_bookings", list=conflict_list),
					)
					return

			# Instead of removing the 'schedule' key (which causes clients to fall back
			# to the default 09:00-18:00), explicitly set all weekdays to empty lists
			# so they are treated as day-offs.
			try:
				empty_week = {str(d): [] for d in range(7)}
				await master_services.set_master_schedule(int(master_id), empty_week)
			except SQLAlchemyError:
				# fallback: try to update bio preserving other keys
				try:
					full_bio = await master_services.get_master_bio(int(master_id))
					full_bio["schedule"] = {str(d): [] for d in range(7)}
					await master_services.update_master_bio(int(master_id), full_bio)
				except SQLAlchemyError:
					# give up; leaving DB unchanged is safer than deleting the key
					logger.exception("Failed to set empty-week schedule for master %s", master_id)
		# quick toast confirmation
		try:
			await cb.answer(t("toast_schedule_cleared"))
		except TelegramAPIError:
			pass
		try:
			await safe_edit(cb.message, text=t("master_cleared"))
		except TelegramAPIError:
			pass
	except SQLAlchemyError as e:
		logger.exception("Failed to clear all schedule for master (DB error): %s", e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError as e:
		logger.exception("Telegram API error while clearing schedule for master: %s", e)
		raise


# Step 1: user picked a start time from the inline keyboard
@master_router.callback_query(MasterScheduleCB.filter(F.action == "pick_start"))
async def pick_window_start(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
	# Ensure caller is a master

	def _decode_time(tok: str | None) -> str | None:
		if not tok:
			return None
		if ":" in tok:
			return tok
		tok = tok.strip()
		if len(tok) == 4 and tok.isdigit():
			return f"{tok[:2]}:{tok[2:]}"
		if len(tok) == 3 and tok.isdigit():
			return f"{tok[0]}:{tok[1:]}"
		return tok

	# Use parsed callback_data provided by aiogram
	try:
		start_time = getattr(callback_data, "time", None)
		# day may be str/int — coerce safely
		day = int(getattr(callback_data, "day", 0) or 0)
	except (TypeError, ValueError, AttributeError):
		# malformed callback payload — best-effort acknowledge
		await cb.answer()
		return

	# normalize decoded time
	start_time = _decode_time(start_time)
	# Store chosen start in FSM and show end-time choices
	try:
		await state.update_data(chosen_day=day, chosen_start=start_time)
		await state.set_state(MasterScheduleStates.schedule_adding_window)
		# guard: start_time must be present for end keyboard builder
		if not start_time:
			try:
				await cb.answer()
			except TelegramAPIError:
				pass
			return
		end_kb = get_time_end_kb(day, start_time)
		await safe_edit(cb.message, text=tr("master_select_window_end", start=start_time), reply_markup=end_kb)
	except TelegramAPIError:
		# UI layer failed — nothing to do
		logger.exception("Telegram API error handling pick_start")
	except SQLAlchemyError as e:
		logger.exception("DB error while handling pick_start: %s", e)
		try:
			if cb.message:
				await cb.message.answer(t("error_retry"))
			else:
				await cb.answer(t("error_retry"))
		except TelegramAPIError:
			pass



# Step 2: user picked an end time — validate & persist
@master_router.callback_query(MasterScheduleCB.filter(F.action == "pick_end"))
async def pick_window_end(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
	# Ensure caller is a master

	def _decode_time(tok: str | None) -> str | None:
		if not tok:
			return None
		if ":" in tok:
			return tok
		tok = tok.strip()
		if len(tok) == 4 and tok.isdigit():
			return f"{tok[:2]}:{tok[2:]}"
		if len(tok) == 3 and tok.isdigit():
			return f"{tok[0]}:{tok[1:]}"
		return tok

	# Use parsed callback_data provided by aiogram
	try:
		end_time = getattr(callback_data, "time", None)
	except (AttributeError, TypeError):
		await cb.answer()
		return

	end_time = _decode_time(end_time)
	if not end_time:
		await cb.answer()
		return
	data = await state.get_data() or {}
	chosen_start = data.get("chosen_start")
	day = data.get("chosen_day")
	if not chosen_start or day is None:
		# missing state — ask to restart
		try:
			if cb.message:
				await cb.message.answer(t("error_retry"))
			else:
				await cb.answer(t("error_retry"))
		except TelegramAPIError:
			pass
		await state.clear()
		return

	# Validate interval ordering
	def to_minutes(tstr: str) -> int:
		h, m = map(int, tstr.split(":"))
		return h * 60 + m

	s_min = to_minutes(chosen_start)
	e_min = to_minutes(end_time)
	if e_min <= s_min:
		try:
			if cb.message:
				await cb.message.answer(t("master_invalid_interval"))
			else:
				await cb.answer(t("master_invalid_interval"))
		except TelegramAPIError:
			pass
		return

	# Persist using service insertion (the service will merge/normalize overlapping windows)
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	# FIX: Check for None master_id
	if master_id is None:
		raise ValueError("Master ID is missing")

	try:
		# Use service helpers to load and persist schedule
		sched = await master_services.get_master_schedule(int(master_id))
		new_sched = master_services.insert_window(sched, int(day), chosen_start, end_time)
		await master_services.set_master_schedule(int(master_id), new_sched)
	except SQLAlchemyError as e:
		logger.exception("Failed to save interval via picker (DB): %s", e)
		# best-effort clear ephemeral FSM state
		try:
			await state.clear()
		except AttributeError:
			pass
		# inform user of retryable failure
		try:
			if cb.message:
				await cb.message.answer(t("error_retry"))
			else:
				await cb.answer(t("error_retry"))
		except TelegramAPIError:
			pass
		return
	except TelegramAPIError as e:
		# If UI failed during persistence/ack, we can't continue reliably
		logger.exception("Telegram API error while saving interval via picker: %s", e)
		try:
			await state.clear()
		except AttributeError:
			pass
		return
	# Success path: acknowledge and return to day actions
	try:
		await cb.answer(t("toast_window_added"))
	except TelegramAPIError:
		# fallback to a simple confirmation via message or callback
		try:
			if cb.message:
				await cb.message.answer(tr("master_add_window_confirm", start=chosen_start, end=end_time))
			else:
				await cb.answer(tr("master_add_window_confirm", start=chosen_start, end=end_time))
		except TelegramAPIError:
			pass
	# Clear FSM and navigate back to day actions (best-effort)
	try:
		await state.clear()
	except AttributeError:
		pass
	text, kb = await _show_day_actions(cb.message, int(master_id), int(day))
	try:
		await nav_replace(state, text, kb)
	except TelegramAPIError:
		pass


# FSM handler: receive added time interval from master
@master_router.message(MasterScheduleStates.schedule_adding_window)
async def receive_time_window(msg: Message, state: FSMContext) -> None:
	text = (msg.text or "").strip()
	m = re.match(r"^(\d{2}:\d{2})-(\d{2}:\d{2})$", text)
	if not m:
		await msg.answer(t("invalid_time_format"))
		return
	interval = f"{m.group(1)}-{m.group(2)}"
	data = await state.get_data()
	day = data.get("chosen_day")
	master_id_raw = getattr(getattr(msg, "from_user", None), "id", None) # FIX: Get raw
	if day is None or master_id_raw is None: # FIX: Check raw
		await msg.answer(t("error_retry"))
		# keep FSM so master can try again; clear ephemeral chosen_start
		try:
			await state.update_data(chosen_start=None)
		except AttributeError:
			pass
		return

	master_id = int(master_id_raw) # FIX: Cast once
	try:
		# Use service helpers: insert and persist
		sched = await master_services.get_master_schedule(master_id)
		# interval is 'HH:MM-HH:MM'
		start, end = m.group(1), m.group(2)
		new_sched = master_services.insert_window(sched, int(day), start, end)
		await master_services.set_master_schedule(master_id, new_sched)
	except SQLAlchemyError as e:
		logger.exception("Failed to save interval for master (DB): %s", e)
		try:
			await msg.answer(t("error_retry"))
		except TelegramAPIError:
			pass
	else:
		# quick toast confirmation, then show day actions so master can add more
		try:
			await msg.answer(t("toast_window_added"))
			text, kb = await _show_day_actions(msg, master_id, int(day))
			try:
				await nav_replace(state, text, kb)
			except TelegramAPIError:
				pass
		except TelegramAPIError:
			# fallback to simple confirmation
			start, end = m.group(1), m.group(2)
			try:
				await msg.answer(tr("master_add_window_confirm", start=start, end=end))
			except TelegramAPIError:
				pass
	finally:
		# Keep FSM active; clear ephemeral chosen_start only
		try:
			await state.update_data(chosen_start=None)
		except AttributeError:
			pass


# Receive client note text when master is in edit_note state
@master_router.message(MasterStates.edit_note)
async def receive_client_note(msg: Message, state: FSMContext) -> None:
	text = (msg.text or "").strip()
	data = await state.get_data() or {}
	booking_id_raw = data.get("client_note_booking_id") # FIX: Get raw
	if not booking_id_raw: # FIX: Check raw
		await msg.answer(t("error_retry"))
		await state.clear()
		return
	
	booking_id = int(booking_id_raw) # FIX: Cast once
	try:
		ok = await master_services.upsert_client_note(booking_id, text)
		if ok:
			await msg.answer(t("master_note_saved"))
		else:
			await msg.answer(t("error_retry"))
	except SQLAlchemyError:
		logger.exception("Failed to upsert client note for booking %s (DB)", booking_id)
		await msg.answer(t("error_retry"))
	except TelegramAPIError:
		logger.exception("Telegram API error while notifying about note upsert for booking %s", booking_id)
	finally:
		await state.clear()

@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings_week"))
async def bookings_week(cb: CallbackQuery, state: FSMContext) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None: # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		# render first page (page=0) via helper to avoid duplicating logic
		text, kb = await _render_bookings_page(cb.message, int(master_id), page=0)
		# update navigation state (push bookings overview)
		try:
			if text and kb:
				await nav_push(state, text, kb)
		except TelegramAPIError:
			# non-critical UI failure
			pass
	except SQLAlchemyError as e:
		logger.exception("Failed to fetch bookings for week (DB): %s", e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError as e:
		logger.exception("Telegram API error while fetching bookings for week: %s", e)


@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings_today"))
async def bookings_today(cb: CallbackQuery, state: FSMContext) -> None:
	"""Show today's bookings for the master (single-day view)."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None: # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		text, kb = await _render_bookings_page(cb.message, int(master_id), page=0, days=1)
		try:
			if text and kb:
				await nav_push(state, text, kb)
		except TelegramAPIError:
			pass
	except SQLAlchemyError as e:
		logger.exception("Failed to fetch bookings for today (DB): %s", e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError as e:
		logger.exception("Telegram API error while fetching bookings for today: %s", e)


@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings_tomorrow"))
async def bookings_tomorrow(cb: CallbackQuery, state: FSMContext) -> None:
	"""Show tomorrow's bookings for the master (single-day view)."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None: # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		# Fetch a 2-day window and present items (the helper paginates client-side)
		text, kb = await _render_bookings_page(cb.message, int(master_id), page=0, days=2)
		try:
			if text and kb:
				await nav_push(state, text, kb)
		except TelegramAPIError:
			pass
	except SQLAlchemyError as e:
		logger.exception("Failed to fetch bookings for tomorrow (DB): %s", e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError as e:
		logger.exception("Telegram API error while fetching bookings for tomorrow: %s", e)


	@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings_all"))
	async def bookings_all(cb: CallbackQuery, state: FSMContext) -> None:
		"""Show all upcoming bookings for the master (no time horizon)"""
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		if master_id is None: # FIX: Check for None
			await safe_edit(cb.message, text=t("error_retry"))
			return
		try:
			# Request an open-ended bookings list (days=None -> all future)
			text, kb = await _render_bookings_page(cb.message, int(master_id), page=0, days=None)
			try:
				if text and kb:
					await nav_push(state, text, kb)
			except TelegramAPIError:
				pass
		except SQLAlchemyError as e:
			logger.exception("Failed to fetch all upcoming bookings for master (DB): %s", e)
			try:
				await safe_edit(cb.message, text=t("error_retry"))
			except TelegramAPIError:
				pass
		except TelegramAPIError as e:
			logger.exception("Telegram API error while fetching all upcoming bookings: %s", e)


@master_router.callback_query(BookingsPageCB.filter())
async def bookings_page(cb: CallbackQuery, callback_data, state: FSMContext) -> None:
	"""Render a specific page of master bookings (page indexed from 0)."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None: # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		# parse requested page from typed callback_data
		page = 0
		try:
			p = getattr(callback_data, "page", None)
			if p is not None:
				page = int(p)
		except (TypeError, ValueError):
			page = 0
		text, kb = await _render_bookings_page(cb.message, int(master_id), page=page)
		try:
			if text and kb:
				# pagination replaces current bookings screen
				await nav_replace(state, text, kb)
		except TelegramAPIError:
			# navigation/editing UI failure — ignore where non-critical
			pass
	except SQLAlchemyError as e:
		logger.exception("DB error while rendering bookings page for master %s: %s", master_id, e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError as e:
		logger.exception("Telegram API error while rendering bookings page for master %s: %s", master_id, e)


# Booking action handler: show detail card and accept actions from master
@master_router.callback_query(BookingActionCB.filter(F.act.in_([
	"master_detail",
	"mark_done",
	"mark_noshow",
	"confirm_mark_done",
	"cancel_confirm",
	"cancel",
	"confirm_mark_noshow",
	"client_history",
	"add_note",
	"cancel_note",
])))
async def booking_action(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	# Debug: log incoming booking-action callback data and caller
	logger.debug("booking_action invoked: user=%s data=%s", getattr(getattr(cb, 'from_user', None), 'id', None), getattr(cb, 'data', None))
	
	# Use typed callback_data provided by aiogram
	act = getattr(callback_data, "act", None)
	booking_id_raw = getattr(callback_data, "booking_id", None) # FIX: Get raw

	# FIX: Check raw value
	if not act or not booking_id_raw:
		try:
			await cb.answer()
		except TelegramAPIError:
			pass
		return
	
	try:
		booking_id = int(booking_id_raw) # FIX: Cast to int *after* check
	except (TypeError, ValueError):
		await cb.answer() # Failed to cast
		return

	# Show booking detail card
	if act == "master_detail":
		# Build canonical booking details and render master-facing card using BookingDetails
		try:
			bd = await build_booking_details(booking_id, user_id=None)
		except SQLAlchemyError:
			# fallback to building without explicit user context
			bd = await build_booking_details(booking_id)

		# Role-aware formatting: use the shared formatter with role='master'
		lang = locale
		try:
			text = format_booking_details_text(bd, lang, role='master')
		except Exception:
			# Fallback to non-role formatter when anything goes wrong
			text = format_booking_details_text(bd, lang)

		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		# action buttons
		# Show reschedule/cancel only when builder flags allow it (computed by shared_services)
		from bot.app.telegram.common.callbacks import RescheduleCB
		if bd.can_reschedule:
			# Use typed callback packing for reschedule
			try:
				cb_payload = pack_cb(RescheduleCB, action="start", booking_id=int(booking_id))
				kb.button(text=t("reschedule"), callback_data=cb_payload)
			except (TypeError, ValueError, AttributeError):
				# if something unexpected happens, skip the reschedule button
				pass

		# Cancel (master-initiated) — show confirm flow trigger when allowed
		if bd.can_cancel:
			kb.button(text=t("cancel"), callback_data=pack_cb(BookingActionCB, act="cancel_confirm", booking_id=booking_id))

		# existing master actions
		kb.button(text=t("booking_mark_done_button"), callback_data=pack_cb(BookingActionCB, act="mark_done", booking_id=booking_id))
		kb.button(text=t("booking_mark_noshow_button"), callback_data=pack_cb(BookingActionCB, act="mark_noshow", booking_id=booking_id))
		kb.button(text=t("booking_client_history_button"), callback_data=pack_cb(BookingActionCB, act="client_history", booking_id=booking_id))
		kb.button(text=t("booking_add_note_button"), callback_data=pack_cb(BookingActionCB, act="add_note", booking_id=booking_id))
		# Back button to return to previous screen (nav stack)
		try:
			kb.button(text=t("back"), callback_data=pack_cb(NavCB, act="back"))
		except (TypeError, AttributeError):
			# best-effort: if NavCB not available or packing fails, skip back button
			pass
		kb.adjust(2)
		markup = kb.as_markup()
		# Use HTML parse mode so tg://user link becomes clickable
		await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
		try:
			await nav_push(state, text, markup)
		except TelegramAPIError:
			# ignore UI/navigation failures at this point
			pass
		return

		# Mark booking as done
	if act == "mark_done":
		# ask for confirmation first
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		kb.button(text="✅", callback_data=pack_cb(BookingActionCB, act="confirm_mark_done", booking_id=booking_id))
		kb.button(text="❌", callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
		kb.adjust(2)
		await cb.answer()
		await safe_edit(cb.message, text=t("confirm_mark_done_prompt"), reply_markup=kb.as_markup())
		return

	# Mark as no-show
	if act == "mark_noshow":
		# ask for confirmation first
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		kb.button(text="✅", callback_data=pack_cb(BookingActionCB, act="confirm_mark_noshow", booking_id=booking_id))
		kb.button(text="❌", callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
		kb.adjust(2)
		await cb.answer()
		await safe_edit(cb.message, text=t("confirm_mark_noshow_prompt"), reply_markup=kb.as_markup())
		return

	# Confirmations handlers
	if act == "confirm_mark_done":
		ok = await master_services.update_booking_status(booking_id, BookingStatus.DONE)
		if ok:
			try:
				await cb.answer(t("master_checkin_success"))
			except TelegramAPIError:
				pass
			# refresh booking card
			try:
				# Use role-aware formatter for master view
				try:
					bd = await build_booking_details(booking_id)
				except SQLAlchemyError:
					bd = await build_booking_details(booking_id)
				lang = locale
				try:
					text = format_booking_details_text(bd, lang, role='master')
				except Exception:
					text = format_booking_details_text(bd, lang)
				try:
					await safe_edit(cb.message, text=text, reply_markup=None, parse_mode="HTML", disable_web_page_preview=True)
				except TelegramAPIError:
					pass
			except SQLAlchemyError:
				# If refreshing booking details fails, ignore and continue
				pass
		else:
			await cb.answer(t("error_retry"))
		return

	# Ask master to confirm cancellation (shows Yes / No)
	if act == "cancel_confirm":
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		# Confirm -> perform cancel (handled below as act == "cancel")
		kb.button(text=t("confirm"), callback_data=pack_cb(BookingActionCB, act="cancel", booking_id=booking_id))
		kb.button(text="❌", callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
		kb.adjust(2)
		await cb.answer()
		await safe_edit(cb.message, t("cancel_confirm_question"), reply_markup=kb.as_markup())
		return

	# Perform master cancellation
	if act == "cancel":
		try:
			ok = await master_services.update_booking_status(booking_id, BookingStatus.CANCELLED)
			if ok:
				# notify master and refresh card
				try:
					await cb.answer(t("booking_cancelled_success"))
				except TelegramAPIError:
					pass
				try:
					from bot.app.services import shared_services
					bd = await shared_services.build_booking_details(booking_id)
				except SQLAlchemyError:
					bd = await shared_services.build_booking_details(booking_id)
				lang = locale
				try:
					text = shared_services.format_booking_details_text(bd, lang, role='master')
				except Exception:
					text = shared_services.format_booking_details_text(bd, lang)
				try:
					await safe_edit(cb.message, text=text, reply_markup=None, parse_mode="HTML", disable_web_page_preview=True)
				except TelegramAPIError:
					pass
				except SQLAlchemyError:
					pass
			else:
				try:
					await cb.answer(t("error_retry"))
				except TelegramAPIError:
					pass
		except SQLAlchemyError:
			try:
				await cb.answer(t("error_retry"))
			except TelegramAPIError:
				pass
		return

	if act == "confirm_mark_noshow":
		ok = await master_services.update_booking_status(booking_id, BookingStatus.NO_SHOW)
		if ok:
			try:
				await cb.answer(t("master_noshow_success"))
			except TelegramAPIError:
				pass
			try:
				bd = await build_booking_details(booking_id)
			except SQLAlchemyError:
				bd = await build_booking_details(booking_id)
			lang = locale
			try:
				text = format_booking_details_text(bd, lang, role='master')
			except Exception:
				text = format_booking_details_text(bd, lang)
			try:
				await safe_edit(cb.message, text=text, reply_markup=None, parse_mode="HTML", disable_web_page_preview=True)
			except TelegramAPIError:
				pass
			except SQLAlchemyError:
				pass
		else:
			await cb.answer(t("error_retry"))
			try:
				bd = await build_booking_details(booking_id)
			except SQLAlchemyError:
				bd = await build_booking_details(booking_id)
			lang = locale
			try:
				text = format_booking_details_text(bd, lang, role='master')
			except Exception:
				text = format_booking_details_text(bd, lang)
			try:
				await safe_edit(cb.message, text=text, reply_markup=None, parse_mode="HTML", disable_web_page_preview=True)
			except TelegramAPIError:
				pass
			except SQLAlchemyError:
				pass

	# Add/edit client note: switch to FSM to receive note text
	if act == "add_note":
		# store booking id in state and ask for note text
		await state.update_data(client_note_booking_id=booking_id)
		await state.set_state(MasterStates.edit_note)
		# Edit the card in-place to prompt for the note (master will type the note in chat)
		# Provide a Cancel button to avoid FSM leaks if master changes their mind.
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		kb.button(text=t("cancel"), callback_data=pack_cb(BookingActionCB, act="cancel_note", booking_id=booking_id))
		kb.adjust(1)
		await safe_edit(cb.message, text=t("master_enter_note"), reply_markup=kb.as_markup())
		return

	# Cancel editing note: clear FSM and re-render booking card
	if act == "cancel_note":
		try:
			await state.clear()
		except AttributeError:
			# state API missing/changed — best-effort ignore
			pass
		# Re-render booking detail card to restore previous view using canonical formatter
		try:
			bd = await build_booking_details(booking_id)
		except SQLAlchemyError as e:
			logger.exception("DB error while building booking details for cancel_note: %s", e)
			# Try a fallback build without DB-specific context
			bd = await build_booking_details(booking_id)

			lang = locale
		try:
			text = format_booking_details_text(bd, lang, role='master')
		except Exception:
			text = format_booking_details_text(bd, lang)

		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb2 = InlineKeyboardBuilder()
		kb2.button(text=t("booking_mark_done_button"), callback_data=pack_cb(BookingActionCB, act="mark_done", booking_id=booking_id))
		kb2.button(text=t("booking_mark_noshow_button"), callback_data=pack_cb(BookingActionCB, act="mark_noshow", booking_id=booking_id))
		kb2.button(text=t("booking_client_history_button"), callback_data=pack_cb(BookingActionCB, act="client_history", booking_id=booking_id))
		kb2.button(text=t("booking_add_note_button"), callback_data=pack_cb(BookingActionCB, act="add_note", booking_id=booking_id))
		kb2.adjust(2)
		await safe_edit(cb.message, text=text, reply_markup=kb2.as_markup(), parse_mode="HTML", disable_web_page_preview=True)
		return
		return

	# fallback
	await cb.answer()

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats"))
async def show_stats(cb: CallbackQuery, state: FSMContext) -> None:  # Добавьте state
	kb = get_stats_kb()
	text = t("master_stats_header")
	await safe_edit(cb.message, text=text, reply_markup=kb)
	try:
		await nav_replace(state, text, kb)
	except TelegramAPIError:
		# ignore navigation UI failures
		pass

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats_week"))
async def stats_week(cb: CallbackQuery, state: FSMContext) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:  # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		stats = await master_services.get_master_stats_summary(int(master_id), days=7)
		text = (
			f"{t('stats_week')}:\n"
			f"{t('next_booking')}: {stats.get('next_booking_time')}\n"
			f"{t('total_bookings')}: {stats.get('total_bookings')}\n"
			f"{t('completed_bookings')}: {stats.get('completed_bookings')}\n"
			f"{t('pending_payment')}: {stats.get('pending_payment')}\n"
			f"{t('no_shows')}: {stats.get('no_shows')}"
		)
		kb = get_stats_kb()
		await safe_edit(cb.message, text=text, reply_markup=kb)
		await state.update_data(current_screen="stats_week")
	except SQLAlchemyError:
		logger.exception("Failed to fetch weekly stats (DB error) for master %s", master_id)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError:
		logger.exception("Telegram API error while showing weekly stats for master %s", master_id)

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats_month"))
async def stats_month(cb: CallbackQuery, state: FSMContext) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:  # FIX: Check for None
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		stats = await master_services.get_master_stats_summary(int(master_id), days=30)
		text = (
			f"{t('stats_month')}:\n"
			f"{t('next_booking')}: {stats.get('next_booking_time')}\n"
			f"{t('total_bookings')}: {stats.get('total_bookings')}\n"
			f"{t('completed_bookings')}: {stats.get('completed_bookings')}\n"
			f"{t('pending_payment')}: {stats.get('pending_payment')}\n"
			f"{t('no_shows')}: {stats.get('no_shows')}"
		)
		kb = get_stats_kb()
		await safe_edit(cb.message, text=text, reply_markup=kb)
	except SQLAlchemyError:
		logger.exception("Failed to fetch monthly stats (DB error) for master %s", master_id)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except TelegramAPIError:
			pass
	except TelegramAPIError:
		logger.exception("Telegram API error while showing monthly stats for master %s", master_id)

__all__ = ["master_router"]



async def _render_bookings_page(
	message,
	master_id: int,
	page: int = 0,
	page_size: int = getattr(cfg, "BOOKINGS_PAGE_SIZE", 5), # FIX: Use cfg.BOOKINGS_PAGE_SIZE
	days: int | None = 7
) -> tuple[Any, Any]:
	"""Helper: render a page of bookings for master `master_id` into `message`."""

	try:
		bookings = await master_services.get_master_bookings_for_period(int(master_id), days=days)

		# Фильтруем только будущие и активные записи
		now = datetime.now(timezone.utc)
		_terminal_statuses = {
			BookingStatus.CANCELLED,
			BookingStatus.DONE,
			BookingStatus.NO_SHOW,
		}
		try:
			_terminal_statuses.add(BookingStatus.EXPIRED)
		except AttributeError:
			# EXPIRED may not exist in older enums
			pass

		bookings = [
			b for b in (bookings or [])
			if getattr(b, "starts_at", None) and getattr(b, "starts_at") >= now
			and getattr(b, "status", None) not in _terminal_statuses
		]

		# Если записей нет — показываем сообщение и клавиатуру с "Назад"
		if not bookings:
			nav_builder = InlineKeyboardBuilder()
			nav_builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="back"))
			nav_kb = nav_builder.as_markup()

			header = f"{t('upcoming_bookings_title')}\n\n{t('no_bookings')}"
			await safe_edit(message, text=header, reply_markup=nav_kb)
			return header, nav_kb

		# Пагинация
		total = len(bookings)
		start_idx = page * page_size
		end_idx = start_idx + page_size
		page_items = bookings[start_idx:end_idx]

		# Bulk-prefetch related rows to avoid N+1 queries (clients, services)
		client_ids = {int(getattr(b, "user_id", 0) or 0) for b in page_items if getattr(b, "user_id", None) is not None}
		service_ids = {getattr(b, "service_id") for b in page_items if getattr(b, "service_id", None) is not None}
		master_tids = {int(getattr(b, "master_id", 0) or 0) for b in page_items if getattr(b, "master_id", None) is not None}

		clients_map: dict[int, User] = {}
		services_map: dict[object, str] = {}
		masters_map: dict[int, User] = {}
		async with get_session() as session:
			if client_ids:
				c_res = await session.execute(select(User).where(User.id.in_(client_ids)))
				clients_map = {u.id: u for u in c_res.scalars().all()}
			if master_tids:
				try:
					m_res = await session.execute(select(User).where(User.telegram_id.in_(master_tids)))
					masters_map = {u.telegram_id: u for u in m_res.scalars().all()}
				except SQLAlchemyError:
					masters_map = {}
			if service_ids:
				try:
					s_res = await session.execute(select(Service.id, Service.name).where(Service.id.in_(service_ids)))
					services_map = {sid: sname for sid, sname in s_res.all()}
				except SQLAlchemyError:
					# best-effort: leave services_map empty so we fall back to shared lookup
					services_map = {}

		builder = InlineKeyboardBuilder()
		for b in page_items:
			try:
				booking_id = getattr(b, "id", None)
				if booking_id is None:
					continue

				# Localized time
				starts = getattr(b, "starts_at", None)
				try:
					txt_time = starts.astimezone(LOCAL_TZ).strftime('%H:%M') if starts else "?"
				except (AttributeError, ValueError, TypeError):
					try:
						txt_time = starts.strftime('%H:%M') if starts else "?"
					except (AttributeError, ValueError, TypeError):
						txt_time = "?"

				# Client name (prefer cached client)
				client_obj = clients_map.get(int(getattr(b, "user_id", 0) or 0))
				client_name = None
				client_username = None
				if client_obj:
					client_name = getattr(client_obj, "name", None)
					client_username = getattr(client_obj, "username", None)
				if client_name:
					client_line = f"{client_name} (@{client_username})" if client_username else client_name
				else:
					client_line = f"id:{getattr(b, 'user_id', '?')}"

				# Service name: prefer prefetch, fallback to shared_services.get_service_name
				sid = getattr(b, "service_id", None)
				if sid in services_map:
					svc_name = services_map.get(sid) or str(sid)
				else:
					# best-effort single lookup via shared_services (centralized cache)
					try:
						svc_name = await get_service_name(str(sid))
					except Exception:
						# fallback to string representation
						svc_name = str(sid)

				label = f"{txt_time} — {client_line} ({svc_name})"
				builder.button(text=label, callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
			except (AttributeError, ValueError, TypeError, SQLAlchemyError):
				# Skip problematic entries (malformed data or DB enrichment failures)
				continue

		# Навигация
		nav_builder = InlineKeyboardBuilder()
		prev_page = page - 1
		next_page = page + 1
		if prev_page >= 0:
			nav_builder.button(text=tr("page_prev"), callback_data=pack_cb(BookingsPageCB, page=prev_page))
		if end_idx < total:
			nav_builder.button(text=tr("page_next"), callback_data=pack_cb(BookingsPageCB, page=next_page))
		nav_builder.button(text=t("back"), callback_data=pack_cb(NavCB, act="back"))
		nav_builder.adjust(2)

		# Комбинируем клавиатуры
		main_kb = builder.as_markup()
		nav_kb = nav_builder.as_markup()
		combined = InlineKeyboardMarkup(inline_keyboard=[])
		for row in getattr(main_kb, "inline_keyboard", []):
			combined.inline_keyboard.append(row)
		for row in getattr(nav_kb, "inline_keyboard", []):
			combined.inline_keyboard.append(row)

		# Заголовок с индикатором страницы
		total_pages = (total + page_size - 1) // page_size if page_size and total else 1
		header = f"{t('upcoming_bookings_title')} ({page + 1}/{max(1, total_pages)})"
		await safe_edit(message, text=header, reply_markup=combined)
		return header, combined

	except SQLAlchemyError as e:
		logger.exception("DB error while building bookings page for master %s: %s", master_id, e)
		try:
			await safe_edit(message, text=t("error_retry"))
		except TelegramAPIError:
			pass
		return None, None
	except TelegramAPIError as e:
		logger.exception("Telegram API error while building bookings page for master %s: %s", master_id, e)
		return None, None
	# Let unexpected exceptions bubble to the centralized router handlers