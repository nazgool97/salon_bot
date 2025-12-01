from __future__ import annotations

import logging

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from datetime import datetime, timezone
from bot.app.telegram.common.roles import MasterRoleFilter, ensure_master
from bot.app.telegram.master.states import MasterScheduleStates, MasterStates

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import Any, cast, Protocol
from bot.app.telegram.common.callbacks import (
	pack_cb,
	MasterMenuCB,
	MasterScheduleCB,
	BookingActionCB,
	BookingsPageCB,
	NavCB,
	MasterBookingsCB,
	ClientInfoCB,
	MasterClientNoteCB,
	MasterCancelReasonCB,
	MasterSetServiceDurationCB,
)
from bot.app.services.shared_services import _decode_time, get_admin_ids
from bot.app.services.client_services import BookingRepo, build_booking_details
from bot.app.services.shared_services import format_booking_details_text
from bot.app.telegram.client.client_keyboards import build_booking_card_kb
from sqlalchemy.exc import SQLAlchemyError
from aiogram.exceptions import TelegramAPIError
from bot.app.telegram.common.ui_fail_safe import safe_edit
from bot.app.telegram.common.navigation import nav_back, nav_push, nav_replace, nav_reset, nav_get_lang
from bot.app.core.constants import DEFAULT_PAGE_SIZE
from bot.app.telegram.master.master_keyboards import (
	get_master_main_menu,
	get_weekly_schedule_kb,
	get_schedule_day_preview_kb,
	get_time_start_kb,
	get_time_end_kb,
)

# Removed direct DB/domain imports; handlers should rely on services only
import json
import re
from bot.app.translations import t, tr
from bot.app.services.shared_services import default_language
from bot.app.services import master_services
import bot.app.telegram.common.callbacks as callbacks_mod

logger = logging.getLogger(__name__)
master_router = Router(name="master")
# Apply master role filter and locale middleware so handlers receive `locale: str` from middleware
from bot.app.telegram.common.locale_middleware import LocaleMiddleware
from bot.app.telegram.common.errors import handle_telegram_error, handle_db_error

# 1. Ensure only masters reach these handlers (redundant if applied elsewhere)
master_router.message.filter(MasterRoleFilter())
master_router.callback_query.filter(MasterRoleFilter())

# 2. Attach LocaleMiddleware so callbacks/messages get `locale: str` injected
master_router.message.middleware(LocaleMiddleware())
master_router.callback_query.middleware(LocaleMiddleware())

# Per DRY: handler-level guard removed; rely on master_router.errors handlers.


# Error handlers centralized in run_bot.py; local registration removed.


# Normalize any keyboard-like object into InlineKeyboardMarkup for strict call sites
def _as_markup(kb: Any) -> InlineKeyboardMarkup | None:
	try:
		if isinstance(kb, InlineKeyboardMarkup):
			return kb
		if isinstance(kb, InlineKeyboardBuilder):
			return kb.as_markup()
	except Exception:
		logger.exception("_as_markup: failed to normalize keyboard object")
	return None


async def _show_master_menu(obj, state: FSMContext, locale: str | None = None) -> None:
	"""
	Legacy helper: builds and shows the master menu with a dashboard summary.
	New code should compute text in the handler and call `show_master_menu_ui`.
	"""
	# Reset navigation stack and mark preferred role for NavCB
	try:
		await nav_reset(state)
	except Exception:
		pass
	try:
		await state.update_data(preferred_role="master")
	except Exception:
		logger.exception("_show_master_menu: failed to set preferred_role in state")

	# Resolve locale: prefer provided, then nav stack, then environment-driven default
	# Prefer middleware-injected `locale`; fall back to environment default when missing
	locale = locale or default_language()

	# Build a small "today" dashboard summary, then delegate to UI-only renderer
	lang = locale
	header = tr("master_menu_header", lang=lang)
	kb = get_master_main_menu(lang)
	text = header
	try:
		master_id = getattr(getattr(obj, 'from_user', None), 'id', None)
		if master_id is not None:
			try:
				summary = await master_services.get_master_dashboard_summary(int(master_id), lang=lang)
				text = f"{summary}\n\n{header}"
			except Exception as e:
				logger.error("ОШИБКА СБОРКИ ДАШБОРДА МАСТЕРА: %s", e)
	except Exception:
		logger.exception("_show_master_menu: failed to read master_id or dashboard summary")
	await show_master_menu_ui(obj, state, lang or default_language(), text, kb)

# Public alias expected by other modules
show_master_menu = _show_master_menu


async def show_master_menu_ui(obj, state: FSMContext, lang: str, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
	"""UI-only renderer for the master menu. Does not fetch or compute data.

	Handlers should build `text` and `kb` and call this function to update UI.
	"""
	try:
		await nav_reset(state)
	except Exception:
		logger.exception("show_master_menu_ui: nav_reset failed")
	try:
		await state.update_data(preferred_role="master")
	except Exception:
		logger.exception("show_master_menu_ui: failed to set preferred_role in state")
	if kb is None:
		kb = get_master_main_menu(lang)
	try:
		await safe_edit(obj, text=text, reply_markup=kb)
	except Exception as e:
		logger.exception("safe_edit failed in show_master_menu_ui: %s", e)
		try:
			if hasattr(obj, "answer"):
				await obj.answer(t("error", lang), show_alert=True)
		except Exception:
			pass
	try:
		header_title = tr("master_menu_header", lang=lang)
		await nav_replace(state, header_title, kb, lang=lang)
	except Exception:
		try:
			header_title = tr("master_menu_header", lang=lang)
			await nav_replace(state, header_title, kb)
		except Exception:
			logger.exception("show_master_menu_ui: failed to nav_replace state without lang")


async def _cancel_and_notify_bookings(bot, booking_ids: list[int] | None, master_id: int) -> int:
	"""Cancel bookings by id, notify clients and admins. Returns number cancelled.

	Best-effort: continues on failures and logs exceptions.
	"""
	# Delegate to shared master service implementation to avoid duplication.
	try:
		return await master_services.cancel_bookings_and_notify(bot, booking_ids)
	except Exception:
		logger.exception("_cancel_and_notify_bookings failed for master %s", master_id)
		return 0



@master_router.message(MasterStates.edit_note)
async def master_edit_note_fallback(msg: Message, state: FSMContext, locale: str) -> None:
	"""Fallback handler moved from client_handlers: when a master is in the
	MasterStates.edit_note FSM state and sends a message, save it as the
	client-facing note attached to the booking.

	The master router is already filtered to master users, so no additional
	master check is necessary here.
	"""
	text = (msg.text or "").strip()

	# Prefer middleware-injected `locale` as the canonical source of truth
	lang = locale or default_language()

	# Cancellation keywords (may be missing from translations)
	raw_cancel_keywords = tr("cancel_keywords", lang=lang)
	cancel_keywords = set()
	if isinstance(raw_cancel_keywords, list):
		cancel_keywords.update(kw.strip().lower() for kw in raw_cancel_keywords if kw)
	elif raw_cancel_keywords:
		cancel_keywords.add(str(raw_cancel_keywords).strip().lower())
	if not cancel_keywords:
		cancel_keywords = {"cancel"}

	normalized_text = text.lower().strip()
	if normalized_text in cancel_keywords:
		data = await state.get_data() or {}
		await state.clear()
		# Inform master that the action was cancelled
		try:
			await msg.answer(t("action_cancelled", lang))
		except Exception:
			logger.exception("master_edit_note_fallback: failed to send action_cancelled to master")

		# Try to restore appropriate UI where possible (booking card or client history)
		booking_id_raw = data.get("client_note_booking_id")
		client_user_raw = data.get("client_note_user_id")
		master_id = getattr(getattr(msg, 'from_user', None), 'id', None)
		try:
			if booking_id_raw:
				booking_id = int(booking_id_raw)
				res = await master_services.handle_cancel_note(booking_id, lang)
				if res:
					text_card, markup = res
					await msg.answer(text=text_card, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
			elif client_user_raw and master_id:
				user_id = int(client_user_raw)
				hist = await master_services.MasterRepo.get_client_history_for_master_by_user(int(master_id), int(user_id))
				text_card = master_services.format_client_history(hist or {}, int(user_id), lang=lang) if hist else t("master_no_client_history", lang)
				from aiogram.utils.keyboard import InlineKeyboardBuilder
				from bot.app.telegram.common.callbacks import pack_cb, MasterClientNoteCB, MasterMenuCB
				builder = InlineKeyboardBuilder()
				builder.button(text=t("edit_note_button", lang), callback_data=pack_cb(MasterClientNoteCB, action="edit", user_id=int(user_id)))
				builder.button(text=t("back", lang), callback_data=pack_cb(callbacks_mod.MasterMenuCB, act="my_clients"))
				builder.adjust(2)
				kb = builder.as_markup()
				await msg.answer(text_card, reply_markup=kb)
		except Exception:
			logger.exception("Error during note cancellation fallback UI restore")
		return

	# --- End cancellation branch ---

	data = await state.get_data() or {}
	# Support two flows: booking-scoped note or user-scoped note
	booking_id_raw = data.get("client_note_booking_id")
	client_user_raw = data.get("client_note_user_id")

	booking_id = None
	user_id = None
	if booking_id_raw:
		try:
			booking_id = int(booking_id_raw)
		except Exception:
			booking_id = None
	if client_user_raw:
		try:
			user_id = int(client_user_raw)
		except Exception:
			user_id = None

	try:
		ok = False
		if booking_id:
			ok = await master_services.MasterRepo.upsert_client_note(booking_id, text)
		elif user_id:
			master_id = getattr(getattr(msg, 'from_user', None), 'id', None)
			if master_id:
				ok = await master_services.MasterRepo.upsert_client_note_for_user(int(master_id), int(user_id), text)
			else:
				ok = False
		else:
			ok = False

		if ok:
			# Send confirmation and show updated client card when possible
			try:
				await msg.answer(t("master_note_saved", lang))
			except Exception:
				pass
			# try to render updated card if we have user-scoped flow
			if user_id:
				try:
					master_id = getattr(getattr(msg, 'from_user', None), 'id', None)
					if master_id:
						hist = await master_services.MasterRepo.get_client_history_for_master_by_user(int(master_id), int(user_id))
						text_card = master_services.format_client_history(hist or {}, int(user_id), lang=lang) if hist else t("master_no_client_history", lang)
						from aiogram.utils.keyboard import InlineKeyboardBuilder
						from bot.app.telegram.common.callbacks import pack_cb, MasterClientNoteCB
						builder = InlineKeyboardBuilder()
						builder.button(text=t("edit_note_button", lang), callback_data=pack_cb(MasterClientNoteCB, action="edit", user_id=int(user_id)))
						builder.button(text=t("back", lang), callback_data=pack_cb(callbacks_mod.MasterMenuCB, act="my_clients"))
						builder.adjust(2)
						kb = builder.as_markup()
						await msg.answer(text_card, reply_markup=kb)
				except Exception:
					pass
			return
	except Exception:
		try:
			await msg.answer(t("error_retry", lang))
		except Exception:
			pass
	finally:
		try:
			await state.clear()
		except Exception:
			pass



@master_router.callback_query(MasterMenuCB.filter(F.act == "menu"))
async def handle_master_menu_entry(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	"""
	ВХОДНАЯ ТОЧКА: Ловит нажатие кнопки 'Меню мастера' из главного меню.
	"""
	# Prefer middleware-provided `locale`; fall back to default when missing
	lang = locale or default_language()
	# Build text in handler and call UI-only renderer
	header = tr("master_menu_header", lang=lang)
	try:
		master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	except Exception:
		master_id = None
	text = header
	if master_id is not None:
		try:
			summary = await master_services.get_master_dashboard_summary(int(master_id), lang=lang)
			text = f"{summary}\n\n{header}"
		except Exception:
			text = header
	kb = get_master_main_menu(lang)
	await show_master_menu_ui(cb, state, lang, text, kb)
	try:
		await cb.answer()
	except Exception:
		pass



@master_router.callback_query(MasterMenuCB.filter(F.act == "my_clients"))
async def master_my_clients(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	"""Show paginated list of clients for the master.

	Supports MasterMenuCB(act="my_clients", page=<n>) where page is 1-indexed.
	"""
	# Prefer middleware-provided `locale`; fall back to default when missing
	lang = locale or default_language()
	master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	if master_id is None:
		try:
			await cb.answer()
		except Exception:
			pass
		return

	try:
		clients = await master_services.MasterRepo.get_clients_for_master(int(master_id))
	except Exception:
		clients = []

	from aiogram.utils.keyboard import InlineKeyboardBuilder
	from bot.app.telegram.common.callbacks import pack_cb, ClientInfoCB

	builder = InlineKeyboardBuilder()

	# Pagination params
	PAGE_SIZE = 5
	page = int(getattr(callback_data, 'page', 1) or 1)
	if page < 1:
		page = 1

	if not clients:
		try:
			await safe_edit(cb.message, text=t("master_no_clients", lang), reply_markup=get_master_main_menu(lang))
			await nav_push(state, t("master_no_clients", lang), get_master_main_menu(lang), lang=lang)
		except Exception:
			pass
		try:
			await cb.answer()
		except Exception:
			pass
		return

	total = len(clients)
	total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
	if page > total_pages:
		page = total_pages or 1

	start = (page - 1) * PAGE_SIZE
	end = start + PAGE_SIZE
	page_clients = clients[start:end]

	# One button per client on the page
	for uid, name, username in page_clients:
		label = name or f"#{uid}"
		if username:
			label = f"{label} (@{username})"
		builder.button(text=label, callback_data=pack_cb(ClientInfoCB, user_id=int(uid)))

	# Navigation row: Prev / Back / Next
	if total_pages > 1:
		if page > 1:
			builder.button(text=t("page_prev", lang), callback_data=pack_cb(MasterMenuCB, act="my_clients", page=page - 1))
		else:
			# filler disabled button (Telegram doesn't support disabled; omit)
			pass
			builder.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))
		if page < total_pages:
			builder.button(text=t("page_next", lang), callback_data=pack_cb(MasterMenuCB, act="my_clients", page=page + 1))
	else:
		builder.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))

	# Layout: each client its own row, nav row at the end (3 buttons if present)
	sizes = [1] * len(page_clients)
	sizes.append(3 if total_pages > 1 else 1)
	builder.adjust(*sizes)
	kb = builder.as_markup()
	title = t("master_my_clients_header", lang)
	try:
		await safe_edit(cb.message, text=title, reply_markup=kb)
		await nav_push(state, title, kb, lang=lang)
	except Exception:
		pass
	try:
		await cb.answer()
	except Exception:
		pass


@master_router.callback_query(ClientInfoCB.filter())
async def master_client_info(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	"""Show client card & actions for selected client (by user_id)."""
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	if not callback_data:
		await cb.answer()
		return
	user_id = int(getattr(callback_data, 'user_id', 0) or 0)
	if not user_id or master_id is None:
		await cb.answer()
		return
	try:
		hist = await master_services.MasterRepo.get_client_history_for_master_by_user(int(master_id), int(user_id))
	except Exception:
		hist = None
	# format_client_history exists in this module (service-level formatter)
	try:
		text = master_services.format_client_history(hist or {}, int(user_id), lang=lang) if hist else t("master_no_client_history", lang)
	except Exception:
		text = t("master_no_client_history", lang)

	from aiogram.utils.keyboard import InlineKeyboardBuilder
	from bot.app.telegram.common.callbacks import pack_cb, MasterClientNoteCB
	builder = InlineKeyboardBuilder()
	builder.button(text=t("edit_note_button", lang), callback_data=pack_cb(MasterClientNoteCB, action="edit", user_id=int(user_id)))
	builder.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="my_clients"))
	builder.adjust(2)
	kb = builder.as_markup()
	try:
		await safe_edit(cb.message, text=text, reply_markup=kb)
		await nav_push(state, text, kb, lang=lang)
	except Exception:
		pass
	try:
		await cb.answer()
	except Exception:
		pass


@master_router.callback_query(MasterClientNoteCB.filter(F.action == "edit"))
async def master_client_note_edit(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	"""Start edit-note flow for a selected client (store user_id in state and await message)."""
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())

	if not callback_data:
		await cb.answer()
		return
	user_id = int(getattr(callback_data, 'user_id', 0) or 0)
	if not user_id:
		await cb.answer()
		return

	# store target user id in state and set FSM for message input
	try:
		await state.update_data(client_note_user_id=int(user_id))
		await state.set_state(MasterStates.edit_note)
		# fetch existing note for prompt
		existing = ""
		try:
						master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
						if master_id:
							hist = await master_services.MasterRepo.get_client_history_for_master_by_user(int(master_id), int(user_id))
							existing = hist.get("note", "") if hist else ""
		except Exception:
			existing = ""

		if existing:
			prompt = f"{t('master_enter_note', lang)}\n\n{t('master_current_note_prefix', lang)}: {existing}"
		else:
			prompt = t('master_enter_note', lang)

		try:
			if cb.message:
				await cb.message.answer(prompt)
			else:
				await cb.answer()
		except Exception:
			try:
				await cb.answer()
			except Exception:
				pass
	except Exception:
		try:
			await cb.answer()
		except Exception:
			pass



@master_router.callback_query(MasterMenuCB.filter(F.act == "schedule"))
async def show_schedule(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	"""Show weekly schedule (placeholder)."""
	logger.debug("show_schedule invoked by user=%s data=%s", getattr(getattr(cb, 'from_user', None), 'id', None), getattr(cb, 'data', None))
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:
		raise ValueError("missing master id")
	# Resolve lang preferring nav stack, then middleware locale
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())

	# 1. get canonical schedule dict
	sched = await master_services.MasterRepo.get_schedule(int(master_id))
	# 2. render into human-readable table
	schedule_text = master_services.render_schedule_table(sched)

	kb = get_weekly_schedule_kb(lang=lang)
	base_text = t("master_schedule_week_overview", lang)
	full_text = f"{base_text}\n\n{schedule_text}"
	await safe_edit(cb.message, text=full_text, reply_markup=kb)
	try:
		await nav_push(state, full_text, kb, lang=lang)
	except AttributeError:
		pass
	# show a small toast confirming refresh (let Telegram errors bubble to centralized handler)
	await cb.answer(t("master_schedule_refreshed", lang))


@master_router.callback_query(MasterScheduleCB.filter(F.action == "pick_start"))
async def schedule_pick_start(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	try:
		await pick_window_start(cb, callback_data, state, locale)
	except (TypeError, ValueError) as e:
		logger.exception("Parsing error in pick_start: %s", e)
	except SQLAlchemyError as e:
		logger.exception("DB error in pick_start: %s", e)
	finally:
		try:
			await cb.answer()
		except Exception:
			pass


async def pick_window_end(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	"""Handle selection of end time after start was picked.

	This will read the previously stored `chosen_start` from FSM state,
	insert the new window via `master_services.insert_window` and persist
	it with `master_services.set_master_schedule`, then refresh UI.
	"""
	try:
		end_time_raw = getattr(callback_data, "time", None)
		day_raw = getattr(callback_data, "day", None)
		try:
			day = int(day_raw) if day_raw is not None else 0
		except Exception:
			day = 0
	except Exception:
		await cb.answer()
		return

	try:
		lang = locale
	except Exception:
		lang = locale

	end_time = _decode_time(end_time_raw)

	# retrieve previously picked start from FSM
	data = await state.get_data()
	start_time = data.get("chosen_start")
	if not start_time:
		await cb.answer()
		return

	if not end_time:
		await cb.answer()
		return

	master_id_raw = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id_raw is None:
		await cb.answer(t("error_retry", lang))
		return
	master_id = int(master_id_raw)

	try:
		sched = await master_services.get_master_schedule(master_id)
		new_sched = master_services.insert_window(sched, int(day), start_time, end_time)
		await master_services.set_master_schedule(master_id, new_sched)
	except Exception as e:
		logger.exception("pick_window_end failed: %s", e)
		try:
			await cb.answer(t("error_retry", lang))
		except Exception:
			pass
		return

	# success: clear ephemeral state and refresh day UI
	try:
		await state.update_data(chosen_start=None)
	except Exception:
		pass

	try:
		await cb.answer(t("toast_window_added", lang))
	except Exception:
		pass

	try:
		text, kb = await _show_day_actions(cb.message, master_id, int(day), lang=lang)
		try:
			await nav_replace(state, text, kb)
		except TelegramAPIError:
			pass
		try:
			await safe_edit(cb.message, text=text, reply_markup=kb)
		except TelegramAPIError:
			pass
	except Exception:
		# ignore UI refresh failures
		pass


@master_router.callback_query(MasterScheduleCB.filter(F.action == "pick_end"))
async def schedule_pick_end(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	try:
		await pick_window_end(cb, callback_data, state, locale)
	except (TypeError, ValueError) as e:
		logger.exception("Parsing error in pick_end: %s", e)
	except SQLAlchemyError as e:
		logger.exception("DB error in pick_end: %s", e)
	finally:
		try:
			await cb.answer()
		except Exception:
			pass


@master_router.callback_query(MasterScheduleCB.filter(F.action == "clear_day"))
async def schedule_clear_day(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	await _check_and_confirm_day_clear(cb, int(day), clear_mode=True)
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "remove_window"))
async def schedule_remove_window(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	# Always use value-based removal (start/end encoded into "time" token) to avoid index races.
	time_token = getattr(callback_data, "time", None)
	start_val: str | None = None
	end_val: str | None = None
	# Parse time_token of form HHMM-HHMM only for action='remove_window'
	if isinstance(time_token, str) and "-" in time_token and len(time_token) >= 7:
		try:
			parts = time_token.split("-", 1)
			if len(parts) == 2 and all(2 <= len(p) <= 4 for p in parts):
				# Accept HMM or HHMM, normalize
				def _fmt(p: str) -> str:
					p = p.strip()
					p = p.zfill(4)  # 900 -> 0900
					return f"{p[:2]}:{p[2:]}"
				start_val = _fmt(parts[0])
				end_val = _fmt(parts[1])
		except Exception:
			start_val = None
			end_val = None
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if day is None or master_id is None:
		try:
			await cb.answer(t("error_retry"), show_alert=False)
		except Exception:
			pass
		return
	try:
		# Require value tokens; if missing, instruct user to refresh the day view.
		if not (start_val and end_val):
			try:
				await cb.answer(tr("stale_schedule_refresh"), show_alert=True)
			except Exception:
				pass
			return
		# Value-based removal path only
		success, conflicts = await master_services.remove_schedule_window_by_value(int(master_id), int(day), start_val, end_val)
		if not success:
			if conflicts:
				try:
					ids = await master_services.check_future_booking_conflicts(int(master_id), clear_all=True, horizon_days=365, return_ids=True)
				except Exception:
					ids = []
				count = len(ids or [])
				from aiogram.utils.keyboard import InlineKeyboardBuilder
				kb = InlineKeyboardBuilder()
				kb.button(text=tr("confirm"), callback_data=pack_cb(MasterMenuCB, act="confirm_clear_all_exec"))
				kb.button(text=tr("cancel"), callback_data=pack_cb(MasterMenuCB, act="menu"))
				kb.adjust(2)
				try:
					await cb.answer()
				except Exception:
					pass
				await safe_edit(cb.message, text=tr("master_clear_all_confirm_with_conflicts", count=count), reply_markup=kb.as_markup())
				return
			try:
				await cb.answer()
			except Exception:
				pass
			return
		try:
			await cb.answer(t("toast_window_removed"))
		except Exception:
			pass
		lang = locale or default_language()
		text, kb = await _show_day_actions(cb.message, int(master_id) if master_id is not None else 0, int(day), lang=lang)
		try:
			await nav_replace(state, text, kb)
		except Exception:
			pass
		try:
			await safe_edit(cb.message, text=text, reply_markup=kb)
		except TelegramAPIError:
			pass
	except SQLAlchemyError as e:
		logger.exception("Failed to remove window (DB error): %s", e)
		await safe_edit(cb.message, text=t("error_retry"))
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "cancel"))
async def schedule_cancel(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	try:
		await state.clear()
	except AttributeError:
		pass
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if day is None:
		await show_schedule(cb, state, locale)
		return
	lang = locale or default_language()
	text, kb = await _show_day_actions(cb.message, int(master_id) if master_id is not None else 0, int(day), lang=lang)
	await nav_replace(state, text, kb)
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "make_off"))
async def schedule_make_off(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	await _check_and_confirm_day_clear(cb, int(day), off_mode=True)
	return


async def _check_and_confirm_day_clear(cb: CallbackQuery, day: int, clear_mode: bool = False, off_mode: bool = False) -> None:
	"""Shared conflict-check + apply logic for clearing or marking a day off.

	Only one of clear_mode/off_mode should be True to determine messages.
	Extracts master_id from callback; shows confirmation if conflicts exist.
	"""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	try:
		# Single conflict query (request only IDs and derive count)
		ids = await master_services.check_future_booking_conflicts(int(master_id), day_to_clear=int(day), horizon_days=365, return_ids=True)
		if ids:
			count = len(ids)
			from aiogram.utils.keyboard import InlineKeyboardBuilder
			kb = InlineKeyboardBuilder()
			kb.button(text=tr("confirm"), callback_data=pack_cb(MasterScheduleCB, action="confirm_clear_day", day=day))
			kb.button(text=tr("cancel"), callback_data=pack_cb(MasterScheduleCB, action="cancel", day=day))
			kb.adjust(2)
			try:
				await cb.answer()
			except Exception:
				pass
			await safe_edit(cb.message, text=tr("master_clear_confirm_with_conflicts", count=count), reply_markup=kb.as_markup())
			return

		await master_services.set_master_schedule_day(int(master_id), int(day), [])
		try:
			if off_mode:
				await cb.answer(t("toast_day_off_marked"))
			else:
				await cb.answer(t("toast_day_cleared"))
		except Exception:
			pass
		try:
			if off_mode:
				await safe_edit(cb.message, text=t("master_day_marked_off"))
			else:
				await safe_edit(cb.message, text=t("master_cleared"))
		except TelegramAPIError:
			pass
	except SQLAlchemyError as e:
		if off_mode:
			logger.exception("Failed to mark day off %s for master (DB error): %s", day, e)
		else:
			logger.exception("Failed to clear day %s for master: %s", day, e)
		try:
			await safe_edit(cb.message, text=t("error_retry"))
		except Exception:
			pass
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "back_to_choose"))
async def schedule_back_to_choose(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	# prefer middleware-provided locale when rendering UI text
	lang = locale or default_language()
	kb = get_weekly_schedule_kb(include_edit=True)
	await safe_edit(cb.message, text=t("master_schedule_week_overview", lang), reply_markup=kb)
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "edit_day"))
async def schedule_edit_day(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	try:
		lang = locale or default_language()
		text, kb = await _show_day_actions(cb.message, int(master_id), int(day), lang=lang)
		try:
			await nav_push(state, text, kb)
		except Exception:
			pass
		await safe_edit(cb.message, text=text, reply_markup=kb)
	except SQLAlchemyError as e:
		logger.exception("Failed to show day actions for master %s day=%s: %s", master_id, day, e)
		await safe_edit(cb.message, text=t("error_retry"))
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "noop"))
async def schedule_noop(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer()
		except Exception:
			pass
		return
	lang = locale or default_language()
	text, kb = await _show_day_actions(cb.message, int(master_id), int(day), lang=lang)
	await safe_edit(cb.message, text=text, reply_markup=kb)
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "add_time"))
async def schedule_add_time(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	try:
		await state.set_state(MasterScheduleStates.schedule_adding_window)
		times = master_services.build_time_slot_list()
		start_kb = get_time_start_kb(int(day), times=times)
		await safe_edit(cb.message, text=tr("master_select_window_start"), reply_markup=start_kb)
	except TelegramAPIError:
		logger.exception("Telegram API error showing start-time kb for master %s day=%s", master_id, day)
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "refresh"))
async def schedule_refresh(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	try:
		await show_schedule(cb, state, locale)
	except Exception:
		logger.exception("Failed to refresh weekly schedule")
	return


@master_router.callback_query(MasterScheduleCB.filter(F.action == "confirm_clear_day"))
async def schedule_confirm_clear_day(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	day = getattr(callback_data, "day", None)
	try:
		if isinstance(day, str) and day.isdigit():
			day = int(day)
		elif isinstance(day, int):
			pass
		else:
			day = None
	except Exception:
		day = None
	if master_id is None or day is None:
		try:
			await cb.answer(t("error_retry"), show_alert=True)
		except Exception:
			pass
		return
	try:
		ids = await master_services.check_future_booking_conflicts(int(master_id), day_to_clear=int(day), horizon_days=365, return_ids=True)
	except Exception:
		ids = []
	cancelled = 0
	bot = getattr(cb, "bot", None)
	_msg = getattr(cb, "message", None)
	if not bot and _msg is not None:
		bot = getattr(_msg, "bot", None)
	try:
		from bot.app.services.client_services import send_booking_notification
	except Exception:
		send_booking_notification = None
	for bid in (ids or []):
		try:
			bd = await master_services.MasterRepo.get_booking_display_data(int(bid))
			client_tid = bd.get("client_telegram_id") if bd else None
			ok = await BookingRepo.set_cancelled(int(bid))
			if ok:
				cancelled += 1
				if bot and client_tid:
					recipients = [int(client_tid)]
					admins = get_admin_ids()
					if admins:
						recipients.extend(admins)
				recipients = list(dict.fromkeys(recipients))
				try:
					if send_booking_notification:
						await send_booking_notification(cast(Bot, bot), int(bid), "cancelled", recipients)
					else:
						logger.warning("send_booking_notification helper unavailable, skipping notification for %s", bid)
				except Exception:
					logger.exception("Failed to notify recipients for cancelled booking %s", bid)
		except Exception:
			continue
	try:
		await master_services.set_master_schedule_day(int(master_id), int(day), [])
	except SQLAlchemyError:
		logger.exception("Failed to mark day off after cancelling bookings for master %s day=%s", master_id, day)
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		await cb.answer(t("toast_day_off_marked"))
	except Exception:
		pass
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	msg = t("master_day_marked_off", lang)
	if cancelled:
		msg = f"{msg}\n\n{t('cancelled_count', lang).format(count=cancelled)}"
	await safe_edit(cb.message, text=msg)
	return


@master_router.callback_query(MasterScheduleCB.filter())
async def schedule_fallback(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	raw = getattr(cb, "data", None)
	uid = getattr(getattr(cb, "from_user", None), "id", None)
	logger.warning("Unhandled MasterSchedule action — raw callback=%s user=%s", raw, uid)
	await safe_edit(cb.message, text=t("unknown"))


async def _show_day_actions(msg_obj, master_id: int, day: int, *, lang: str | None = None) -> tuple[str, InlineKeyboardMarkup]:
	"""Return (text, keyboard) for a given master's weekday preview.

	This central helper is used by several flows: when a master selects a
	weekday button, after adding/removing windows, and when cancelling
	intermediate FSM flows.
	"""
	try:
		sched = await master_services.get_master_schedule(int(master_id))
	except Exception:
		sched = {}

	from typing import Any
	windows: list[Any] = []
	try:
		if isinstance(sched, dict):
			windows = sched.get(str(day), []) or []
	except Exception:
		windows = []

	# Build textual preview: header + per-window lines
	try:
		wd_raw = tr("weekday_short", lang=lang)
		if isinstance(wd_raw, list):
			weekdays = wd_raw
		elif isinstance(wd_raw, str):
			weekdays = [w.strip() for w in wd_raw.split(",") if w.strip()]
		else:
			weekdays = []
		if not weekdays:
			weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
	except Exception:
		weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

	day_name = weekdays[int(day)] if 0 <= int(day) < len(weekdays) else f"Day {day}"
	header = f"{tr('master_schedule_day_header', lang=lang, day=day_name)}\n"
	if not windows:
		body = t("master_no_windows")
	else:
		try:
			lines = []
			for w in windows:
				if isinstance(w, (list, tuple)) and len(w) >= 2:
					lines.append(f"{w[0]}-{w[1]}")
				else:
					lines.append(str(w))
			body = "\n".join(lines)
		except Exception:
			body = t("master_no_windows")

	text = f"{header}\n{body}"
	kb = get_schedule_day_preview_kb(int(day), windows)
	return text, kb



# Note: master-specific keyboards now use MasterMenuCB(act="menu") for Back
# buttons. This avoids conflict with the client-wide "global_back" handler
# which is registered on the client router. The master menu is handled by
# `show_master_menu` (MasterMenuCB.act == "menu") which resets navigation.

@master_router.callback_query(MasterMenuCB.filter(F.act == "bookings"))
async def show_bookings_menu(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	# Dashboard-first: render upcoming bookings page and present dashboard KB.
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:
		await safe_edit(cb.message, text=t("error_retry"))
		return
	try:
		lang = locale
	except Exception:
		lang = locale
	try:
		# Fetch booking rows via service layer, format them and build UI-only keyboard
		from bot.app.services.master_services import get_master_bookings
		from bot.app.services.shared_services import format_booking_list_item
		# master_services is imported from bot.app.services above

		rows, meta = await get_master_bookings(master_id=int(master_id), mode='upcoming', page=1, page_size=DEFAULT_PAGE_SIZE)
		formatted_rows: list[tuple[str,int]] = []
		for r in rows:
			try:
				txt, bid = format_booking_list_item(r, role="master", lang=lang)
				formatted_rows.append((txt, bid))
			except Exception:
				continue
		# Build keyboard using UI-only builder
		from bot.app.telegram.client.client_keyboards import build_my_bookings_keyboard
		kb = await build_my_bookings_keyboard(
			formatted_rows,
			meta.get('upcoming_count', meta.get('total', 0)),
			(meta.get('done_count', 0) + meta.get('cancelled_count', 0) + meta.get('noshow_count', 0)),
			'upcoming',
			meta.get('page', 1),
			lang,
			items_per_page=DEFAULT_PAGE_SIZE,
			cancelled_count=meta.get('cancelled_count', 0),
			noshow_count=meta.get('noshow_count', 0),
			total_pages=meta.get('total_pages', 1),
			current_page=meta.get('page', 1),
			role="master",
		)
		# persist current mode/page in state
		try:
			await state.update_data(bookings_mode="upcoming", bookings_page=1)
		except Exception:
			pass
		# Ensure role hint is set so NavCB(role_root) returns to master menu
		try:
			await state.update_data(preferred_role="master")
		except Exception:
			pass
		# Use nav_replace with the dashboard text + keyboard. The keyboard builder is UI-only
		# For master users prefer concise per-mode titles (no generic "master bookings" prefix).
		# Use explicit per-mode title for the initial view (upcoming).
		try:
			# Map the canonical visible title to the current mode. Default to the generic header
			# only if a mode-specific title is unavailable.
			base_title = t("upcoming_bookings_title", lang) or t("master_bookings_header", lang)
		except Exception:
			base_title = t("master_bookings_header", lang)
		# build dynamic header: for masters show the per-mode base title and optional page info
		try:
			meta = meta or {}
			current_mode = "upcoming"
			if current_mode == "upcoming":
				title_for_mode = t("upcoming_bookings_title", lang)
				tab_count = int(meta.get('upcoming_count', 0) or 0)
			elif current_mode == "done":
				title_for_mode = t("completed_bookings_title", lang)
				tab_count = int(meta.get('done_count', 0) or 0)
			elif current_mode == "cancelled":
				title_for_mode = t("cancelled_bookings_title", lang)
				tab_count = int(meta.get('cancelled_count', 0) or 0)
			elif current_mode == "no_show":
				title_for_mode = t("no_show_bookings_title", lang)
				tab_count = int(meta.get('noshow_count', 0) or 0)
			else:
				title_for_mode = base_title
				tab_count = int(meta.get('total', 0) or 0)
			page = int(meta.get('page', 1) or 1)
			total_pages = int(meta.get('total_pages', 1) or 1)
			# Visible header: use a concise per-mode title without counts or the
			# generic word "Bookings". Keep it short: Upcoming / Done / Cancelled / No-show.
			try:
				if current_mode == "upcoming":
					title_for_mode = tr("upcoming", lang=lang) or "Upcoming"
				elif current_mode == "done":
					title_for_mode = tr("master_completed", lang=lang) or "Done"
				elif current_mode == "cancelled":
					title_for_mode = tr("cancelled", lang=lang) or "Cancelled"
				elif current_mode == "no_show":
					title_for_mode = tr("no_show", lang=lang) or "No-show"
				else:
					title_for_mode = base_title
			except Exception:
				title_for_mode = base_title
			dynamic_header = title_for_mode
		except Exception:
			dynamic_header = base_title
			title_for_mode = base_title
		# ensure `title` exists for fallback message sends
		title = title_for_mode if 'title_for_mode' in locals() else base_title
		try:
			await nav_replace(state, dynamic_header, _as_markup(kb))
		except Exception:
			try:
				await nav_replace(state, dynamic_header, _as_markup(kb))
			except Exception:
				pass
		# Try to edit the existing message; fallback to sending a new one when edit fails
		try:
			ok = await safe_edit(cb.message, text=dynamic_header, reply_markup=_as_markup(kb))
			if not ok:
				msg_obj = getattr(cb, 'message', None)
				if msg_obj is not None and hasattr(msg_obj, 'answer'):
					try:
						new_msg = await msg_obj.answer(title, reply_markup=_as_markup(kb))
						try:
							bot_instance = getattr(msg_obj, 'bot', None)
							if bot_instance is not None:
								await bot_instance.delete_message(chat_id=msg_obj.chat.id, message_id=msg_obj.message_id)
						except Exception:
							pass
					except Exception:
						pass
		except Exception:
			logger.exception("force redraw failed in show_bookings_menu")
	except SQLAlchemyError as e:
		logger.exception("Failed to render bookings dashboard for master %s: %s", master_id, e)
		await safe_edit(cb.message, text=t("error_retry"))
	await cb.answer()


@master_router.callback_query(MasterMenuCB.filter(F.act == "clear_all"))
async def confirm_clear_all(cb: CallbackQuery, locale: str) -> None:
	"""Ask master to confirm clearing the whole weekly schedule."""
	# confirmation keyboard
	from aiogram.utils.keyboard import InlineKeyboardBuilder

	# No FSM state available here — use middleware-provided locale
	lang = locale

	builder = InlineKeyboardBuilder()
	builder.button(text=t("confirm", lang), callback_data=pack_cb(MasterMenuCB, act="clear_all_confirm"))
	builder.button(text=t("cancel", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))
	builder.adjust(2)
	await safe_edit(cb.message, text=tr("master_clear_all_confirm"), reply_markup=builder.as_markup())


@master_router.message(Command("start"))
async def master_cmd_start(message: Message, state: FSMContext, locale: str) -> None:
	"""Handle /start for masters: clear FSM state and show master menu."""
	try:
		await state.clear()
	except Exception:
		pass
	try:
		await _show_master_menu(message, state, locale)
	except Exception:
		try:
			await message.answer(t("master_menu_header", locale))
		except Exception:
			pass


@master_router.message(F.text.regexp(r"^/start(?:@[A-Za-z0-9_]+)?(?:\s|$)"))
async def master_cmd_start_fallback(message: Message, state: FSMContext, locale: str) -> None:
	await master_cmd_start(message, state, locale)


@master_router.message(F.text.regexp(r"(?i)^(start|старт)(\s|$)"))
async def master_cmd_start_plaintext(message: Message, state: FSMContext, locale: str) -> None:
	await master_cmd_start(message, state, locale)


@master_router.callback_query(MasterMenuCB.filter(F.act == "clear_all_confirm"))
async def do_clear_all(cb: CallbackQuery, locale: str) -> None:
	"""Perform clearing of the weekly schedule using service layer only."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:
		await cb.answer(t("error_retry"), show_alert=True)
		return
	try:
		# Check for future booking conflicts across all weekdays (bounded horizon)
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

		# Explicitly set all weekdays to empty lists to mark days off
		empty_week: dict[str, list[Any]] = {str(d): [] for d in range(7)}
		try:
			await master_services.set_master_schedule(int(master_id), empty_week)
		except SQLAlchemyError:
			# fallback: update bio preserving other keys
			try:
				full_bio = await master_services.MasterRepo.get_master_bio(int(master_id))
				full_bio["schedule"] = {str(d): [] for d in range(7)}
				await master_services.MasterRepo.update_master_bio(int(master_id), full_bio)
			except SQLAlchemyError:
				logger.exception("Failed to set empty-week schedule for master %s", master_id)

		await cb.answer(t("toast_schedule_cleared"))
		await safe_edit(cb.message, text=t("master_cleared", locale))
	except SQLAlchemyError:
		await safe_edit(cb.message, text=t("error_retry", locale))


@master_router.callback_query(MasterMenuCB.filter(F.act == "confirm_clear_all_exec"))
async def exec_clear_all_with_conflicts(cb: CallbackQuery, locale: str) -> None:
	"""User confirmed clearing all: cancel conflicting bookings (notify clients) and clear the weekly schedule."""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:
		await cb.answer(t("error_retry"), show_alert=True)
		return

	try:
		ids = await master_services.check_future_booking_conflicts(int(master_id), clear_all=True, horizon_days=365, return_ids=True)
	except Exception:
		ids = []

	cancelled = 0
	# Acquire bot instance for notifications
	bot = getattr(cb, "bot", None)
	_msg = getattr(cb, "message", None)
	if not bot and _msg is not None:
		bot = getattr(_msg, "bot", None)
	try:
		from bot.app.services.client_services import send_booking_notification
	except Exception:
		send_booking_notification = None

	for bid in (ids or []):
		try:
			bd = await master_services.MasterRepo.get_booking_display_data(int(bid))
			client_tid = bd.get("client_telegram_id") if bd else None
			ok = await BookingRepo.set_cancelled(int(bid))
			if ok:
				cancelled += 1
				if bot and client_tid:
					recipients = [int(client_tid)]
					admins = get_admin_ids()
					if admins:
						recipients.extend(admins)
					recipients = list(dict.fromkeys(recipients))
					try:
						if send_booking_notification:
							await send_booking_notification(cast(Bot, bot), int(bid), "cancelled", recipients)
						else:
							logger.warning("send_booking_notification helper unavailable, skipping notification for %s", bid)
					except Exception:
						logger.exception("Failed to notify recipients for cancelled booking %s", bid)
		except Exception:
			continue

	# Now clear the weekly schedule
		empty_week: dict[str, list[Any]] = {str(d): [] for d in range(7)}
		try:
			await master_services.set_master_schedule(int(master_id), empty_week)
		except SQLAlchemyError:
			try:
				full_bio = await master_services.MasterRepo.get_master_bio(int(master_id))
				full_bio["schedule"] = {str(d): [] for d in range(7)}
				await master_services.MasterRepo.update_master_bio(int(master_id), full_bio)
			except SQLAlchemyError:
				logger.exception("Failed to set empty-week schedule for master %s", master_id)

	try:
		await cb.answer(t("toast_schedule_cleared"))
	except Exception:
		pass

	# Localize post-operation summary
	try:
		from bot.app.services.shared_services import safe_get_locale
		lang = await safe_get_locale(int(master_id))
	except Exception:
		lang = "uk"
	msg = t("master_cleared", lang)
	if cancelled:
		msg = f"{msg}\n\n{t('cancelled_count', lang).format(count=cancelled)}"
	await safe_edit(cb.message, text=msg)


# Step 1: user picked a start time from the inline keyboard
@master_router.callback_query(MasterScheduleCB.filter(F.action == "pick_start"))
async def pick_window_start(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	# Ensure caller is a master

	# use module-level _decode_time

	# Use parsed callback_data provided by aiogram
	try:
		start_time = getattr(callback_data, "time", None)
		# day may be str/int — coerce safely
		day = int(getattr(callback_data, "day", 0) or 0)
	except (TypeError, ValueError, AttributeError):
		# malformed callback payload — best-effort acknowledge
		await cb.answer()
		return

	# Resolve language preference for UI builders (nav lang preferred)
	try:
		lang = locale
	except Exception:
		lang = locale

	# normalize decoded time
	start_time = _decode_time(start_time)
	# Store chosen start in FSM and show end-time choices
	try:
		await state.update_data(chosen_day=day, chosen_start=start_time)
		await state.set_state(MasterScheduleStates.schedule_adding_window)
		# guard: start_time must be present for end keyboard builder
		if not start_time:
			# let Telegram errors bubble to centralized handler
			await cb.answer()
			return
		# Compute end-time items via service layer and pass to UI-only keyboard builder
		items = master_services.compute_time_end_items(day, start_time)
		# Build end-time keyboard and ask master to pick end
		end_kb = get_time_end_kb(day, start_time, items=items)
		await safe_edit(cb.message, text=tr("master_select_window_end", start=start_time), reply_markup=end_kb)
	except Exception as e:
			# Catch-all: log and inform user that DB/UI action failed
			logger.exception("Error handling pick_start: %s", e)
			try:
				if cb.message:
					await cb.message.answer(t("error_retry"))
				else:
					await cb.answer(t("error_retry"))
			except Exception:
				pass
			return

# ---------------- Service durations editing (new feature) -----------------
@master_router.callback_query(MasterMenuCB.filter(F.act == "service_durations"))
async def master_service_durations_menu(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	lang = locale or default_language()
	try:
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		if not master_id:
			await cb.answer()
			return


		rows = await master_services.MasterRepo.get_services_with_durations_for_master(int(master_id))
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		if not rows:
			kb.button(text=t("master_no_services", lang) if t("master_no_services", lang) != "master_no_services" else "Нет услуг", callback_data="noop")
		else:
			for sid, name, dur in rows:
				label = f"{name} • {dur}m" if dur else name
				kb.button(text=label, callback_data=pack_cb(MasterSetServiceDurationCB, service_id=sid, minutes=dur or 0))
		kb.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))
		kb.adjust(*([1] * max(1,len(rows))),1)
		header = t("master_service_durations_header", lang) if t("master_service_durations_header", lang) != "master_service_durations_header" else "Измените длительность услуг:" 
		await safe_edit(cb.message, text=header, reply_markup=kb.as_markup())
		await cb.answer()
	except Exception as e:
		logger.exception("master_service_durations_menu failed: %s", e)
		try:
			await cb.answer(t("error_retry", lang))
		except Exception:
			pass
	return


@master_router.callback_query(MasterSetServiceDurationCB.filter())
async def master_set_service_duration(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	lang = locale or default_language()
	service_id = getattr(callback_data, "service_id", None)
	current = getattr(callback_data, "minutes", None)
	if not service_id:
		await cb.answer()
		return

	# Persist selected service and (optionally) minutes into FSM state so Save can use them
	try:
		await state.update_data(last_service_id=str(service_id))
		if current is not None and isinstance(current, int) and int(current) > 0:
			await state.update_data(last_service_minutes=int(current))
	except Exception:
		# non-fatal: state backend may be unavailable; continue without persisting
		pass
	# Build selection of common durations
	options = [15, 30, 45, 60, 75, 90, 105, 120]
	try:
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		if not master_id:
			await cb.answer()
			return
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		for m in options:
			prefix = "✅ " if current and int(current) == m else ""
			kb.button(text=f"{prefix}{m}m", callback_data=pack_cb(MasterSetServiceDurationCB, service_id=str(service_id), minutes=m))
		# Save button uses the currently selected minutes (if any)
		if current and int(current) > 0:
			kb.button(text=t("save", lang) if t("save", lang) != "save" else "Сохранить", callback_data=pack_cb(MasterMenuCB, act="save_service_duration", page=int(current) if isinstance(current,int) else None))
		kb.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="service_durations"))
		kb.adjust(4,4,2)
		header = t("master_pick_duration_header", lang) if t("master_pick_duration_header", lang) != "master_pick_duration_header" else "Выберите длительность:" 
		await safe_edit(cb.message, text=header, reply_markup=kb.as_markup())
		await cb.answer()
	except Exception:
		await cb.answer()
	return


@master_router.callback_query(MasterMenuCB.filter(F.act == "save_service_duration"))
async def master_save_service_duration(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	lang = locale or default_language()
	# We re-use state to remember last selected service+duration
	data = await state.get_data()
	service_id = data.get("last_service_id")
	minutes = data.get("last_service_minutes")
	if not service_id or not minutes:
		await cb.answer(t("error_retry", lang))
		return
	try:
		master_id = getattr(getattr(cb, "from_user", None), "id", None)
		if master_id is None:
			await cb.answer()
			return
		ok = await master_services.MasterRepo.set_master_service_duration(int(master_id), str(service_id), int(minutes))
		await cb.answer(t("master_service_duration_set_success", lang) if t("master_service_duration_set_success", lang) != "master_service_duration_set_success" else "Готово")
		# Re-render list
		rows = await master_services.MasterRepo.get_services_with_durations_for_master(int(master_id))
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		for sid, name, dur in rows:
			label = f"{name} • {dur}m" if dur else name
			kb.button(text=label, callback_data=pack_cb(MasterSetServiceDurationCB, service_id=sid, minutes=dur or 0))
		kb.button(text=t("back", lang), callback_data=pack_cb(MasterMenuCB, act="menu"))
		kb.adjust(*([1] * max(1,len(rows))),1)
		header = t("master_service_durations_header", lang) if t("master_service_durations_header", lang) != "master_service_durations_header" else "Измените длительность услуг:" 
		await safe_edit(cb.message, text=header, reply_markup=kb.as_markup())
	except Exception as e:
		logger.exception("master_save_service_duration failed: %s", e)
		try:
			await cb.answer(t("error_retry", lang))
		except Exception:
			pass
	return


# FSM handler: receive added time interval from master
@master_router.message(MasterScheduleStates.schedule_adding_window)
async def receive_time_window(msg: Message, state: FSMContext, locale: str) -> None:
	# Ensure locale is injected by middleware and used for translations
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	text = (msg.text or "").strip()
	m = re.match(r"^(\d{2}:\d{2})-(\d{2}:\d{2})$", text)
	if not m:
		await msg.answer(t("invalid_time_format", lang))
		return
	interval = f"{m.group(1)}-{m.group(2)}"
	data = await state.get_data()
	day = data.get("chosen_day")
	# get raw master id and validate
	master_id_raw = getattr(getattr(msg, "from_user", None), "id", None)
	if day is None or master_id_raw is None:
		await msg.answer(t("error_retry", lang))
		# keep FSM so master can try again; clear ephemeral chosen_start
		try:
			await state.update_data(chosen_start=None)
		except AttributeError:
			pass
		return

	master_id = int(master_id_raw)
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
			await msg.answer(t("toast_window_added", lang))
			text, kb = await _show_day_actions(msg, master_id, int(day), lang=lang)
			try:
				await nav_replace(state, text, kb)
			except TelegramAPIError:
				pass
		except TelegramAPIError:
			# fallback to simple confirmation
			start, end = m.group(1), m.group(2)
			try:
				await msg.answer(tr("master_add_window_confirm", lang=lang, start=start, end=end))
			except TelegramAPIError:
				pass
	finally:
		# Keep FSM active; clear ephemeral chosen_start only
		try:
			await state.update_data(chosen_start=None)
		except AttributeError:
			pass


# Duplicate handler `receive_client_note` removed — keep `master_edit_note_fallback`
# which provides the canonical logic for saving a client's note when a master
# is in the `MasterStates.edit_note` state. This avoids routing conflicts.


class _HasModePage(Protocol):
	mode: str | None
	page: int | None

@master_router.callback_query(MasterBookingsCB.filter())
@master_router.callback_query(BookingsPageCB.filter())
async def master_bookings_navigate(cb: CallbackQuery, callback_data: _HasModePage, state: FSMContext, locale: str | None = None) -> None:
	"""Handle master bookings tab changes and pagination (combined).

	Accepts MasterBookingsCB (mode switch) and BookingsPageCB (pagination).
	"""
	master_id = getattr(getattr(cb, "from_user", None), "id", None)
	if master_id is None:
		await safe_edit(cb.message, text=t("error_retry", locale or default_language()))
		return
	# Prefer explicit navigation language, fall back to middleware-provided locale, then 'uk'
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	try:
		# RoleCB now carries both `mode` and optional `page`.
		# If `page` is present -> this is a pagination event; if only `mode` -> tab switch.
		mode = getattr(callback_data, "mode", None)
		page = getattr(callback_data, "page", None)
		if page is not None:
			# Pagination event: preserve current mode from state
			data = await state.get_data() or {}
			mode = data.get("bookings_mode", mode or "upcoming")
			try:
				page = int(page)
			except Exception:
				page = int(data.get("bookings_page", 1) or 1)
			await state.update_data(bookings_page=page)
		elif mode is not None:
			# Tab switch -> reset to first page
			page = 1
			await state.update_data(bookings_mode=mode, bookings_page=1)
		else:
			# Neither provided: fallback to state
			data = await state.get_data() or {}
			mode = data.get("bookings_mode", "upcoming")
			try:
				page = int(data.get("bookings_page", 1) or 1)
			except Exception:
				page = 1

		# Use service-driven flow: fetch rows, prefetch maps, format and build UI-only keyboard
		from bot.app.services.master_services import get_master_bookings
		from bot.app.services.shared_services import format_booking_list_item

		rows, meta = await get_master_bookings(master_id=int(master_id), mode=mode, page=int(page or 1), page_size=DEFAULT_PAGE_SIZE)
		formatted_rows: list[tuple[str,int]] = []
		for r in rows:
			try:
				txt, bid = format_booking_list_item(r, role="master", lang=lang)
				formatted_rows.append((txt, bid))
			except Exception:
				continue
		from bot.app.telegram.client.client_keyboards import build_my_bookings_keyboard
		kb = await build_my_bookings_keyboard(
			formatted_rows,
			meta.get('upcoming_count', meta.get('total', 0)),
			(meta.get('done_count', 0) + meta.get('cancelled_count', 0) + meta.get('noshow_count', 0)),
			mode or 'upcoming',
			meta.get('page', 1),
			lang,
			items_per_page=DEFAULT_PAGE_SIZE,
			cancelled_count=meta.get('cancelled_count', 0),
			noshow_count=meta.get('noshow_count', 0),
			total_pages=meta.get('total_pages', 1),
			current_page=meta.get('page', 1),
			role="master",
		)
		if cb.message:
			# dynamic header for master bookings navigation
			try:
				meta = meta or {}
				mode_for_header = mode or 'upcoming'
				mode_map = {
					"upcoming": (t("upcoming", lang), int(meta.get('upcoming_count', 0) or 0)),
					"done": (t("done_bookings", lang), int(meta.get('done_count', 0) or 0)),
					"cancelled": (t("cancelled_bookings", lang), int(meta.get('cancelled_count', 0) or 0)),
					"no_show": (t("no_show_bookings", lang), int(meta.get('noshow_count', 0) or 0)),
				}
				tab_name, tab_count = mode_map.get(mode_for_header, mode_map["upcoming"])
				page = int(meta.get('page', 1) or 1)
				total_pages = int(meta.get('total_pages', 1) or 1)
				# For masters prefer concise per-mode titles instead of a generic header
				if mode_for_header == "upcoming":
					title_for_mode = t("upcoming_bookings_title", lang)
				elif mode_for_header == "done":
					title_for_mode = t("completed_bookings_title", lang)
				elif mode_for_header == "cancelled":
					title_for_mode = t("cancelled_bookings_title", lang)
				elif mode_for_header == "no_show":
					title_for_mode = t("no_show_bookings_title", lang)
				else:
					title_for_mode = t('master_bookings_header', lang)
				dynamic_header = f"{title_for_mode} ({tab_count})"
				if total_pages > 1:
					dynamic_header += f" ({t('page_short', lang)} {page}/{total_pages})"
			except Exception:
				dynamic_header = t('master_bookings_header', lang)
			await safe_edit(cb.message, text=dynamic_header, reply_markup=_as_markup(kb))
		logger.info("Master bookings navigate: user=%s mode=%s page=%s", master_id, mode, page)
	except Exception as e:
		# Catch-all for DB/UI/other failures during bookings navigation
		logger.exception("Error while fetching master bookings: %s", e)
		try:
			await safe_edit(cb.message, text=t("error_retry", lang))
		except Exception:
			pass
	await cb.answer()




# Split booking actions into focused handlers to improve readability and testability.


def _resolve_lang(state: FSMContext, locale: str) -> str:
	# helper: prefer nav stack lang when handlers need it; individual handlers will await nav_get_lang
	return locale or default_language()


@master_router.callback_query(BookingActionCB.filter(F.act == "master_detail"))
async def booking_master_detail(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	try:
		booking_id_raw = getattr(callback_data, "booking_id", None)
		if not booking_id_raw:
			await cb.answer()
			return
		try:
			booking_id = int(booking_id_raw)
		except Exception:
			await cb.answer()
			return
		lang = locale or default_language()
		try:
			bd = await build_booking_details(booking_id, user_id=None, lang=lang)
			text = format_booking_details_text(bd, lang, role="master")
			markup = build_booking_card_kb(bd, booking_id, role="master", lang=lang)
			if cb.message:
				await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
				try:
					await nav_push(state, text, markup)
				except Exception:
					pass
		except Exception as _e:
			logger.exception("Failed to render master_detail booking card: %s", _e)
			try:
				await cb.answer(t("error_retry", lang), show_alert=True)
			except Exception:
				pass
	finally:
		return


@master_router.callback_query(BookingActionCB.filter(F.act == "mark_done"))
async def booking_mark_done(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	ok, text, markup = await master_services.handle_mark_done(booking_id, lang)
	try:
		if ok:
			await cb.answer(t("master_checkin_success"))
		else:
			await cb.answer(t("error_retry"))
	except TelegramAPIError:
		pass
	try:
		if cb.message:
			await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
	except TelegramAPIError:
		pass
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "mark_noshow"))
async def booking_mark_noshow(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	ok, text, markup = await master_services.handle_mark_noshow(booking_id, lang)
	try:
		if ok:
			await cb.answer(t("master_noshow_success"))
		else:
			await cb.answer(t("error_retry"))
	except TelegramAPIError:
		pass
	try:
		if cb.message:
			await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
	except TelegramAPIError:
		pass
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "confirm_mark_done"))
async def booking_confirm_mark_done(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	return await booking_mark_done(cb, callback_data, state, locale)


@master_router.callback_query(BookingActionCB.filter(F.act == "confirm_mark_noshow"))
async def booking_confirm_mark_noshow(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	return await booking_mark_noshow(cb, callback_data, state, locale)


@master_router.callback_query(BookingActionCB.filter(F.act == "show_full_note"))
async def booking_show_full_note(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		bd = await build_booking_details(booking_id)
		lang = locale
		note = None
		if bd:
			if isinstance(bd, dict):
				note = bd.get("client_note")
			else:
				note = getattr(bd.raw, 'get', lambda k, d=None: None)('client_note') if getattr(bd, 'raw', None) else None
		text = t("client_note_label", lang) + ":\n\n" + (note or t("no_notes", lang) if t("no_notes", lang) != "no_notes" else (note or "—"))
		from aiogram.utils.keyboard import InlineKeyboardBuilder
		kb = InlineKeyboardBuilder()
		kb.button(text=t("back", lang), callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
		kb.adjust(1)
		await safe_edit(cb.message, text=text, reply_markup=kb.as_markup())
		await cb.answer()
	except Exception:
		try:
			await cb.answer()
		except Exception:
			pass
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "client_history"))
async def booking_client_history(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		lang = locale or default_language()
	except Exception:
		lang = (locale or default_language())
	res = await master_services.handle_client_history(booking_id, lang)
	if not res:
		try:
			await safe_edit(cb.message, text=t("master_no_client_history", lang), reply_markup=None)
			await cb.answer()
		except Exception:
			try:
				await cb.answer()
			except Exception:
				pass
		return
	view_text, kb = res
	try:
		await safe_edit(cb.message, text=view_text, reply_markup=kb)
		await cb.answer()
	except Exception:
		try:
			await cb.answer()
		except Exception:
			pass
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "cancel_confirm"))
async def booking_cancel_confirm(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	from aiogram.utils.keyboard import InlineKeyboardBuilder
	from bot.app.telegram.common.callbacks import MasterCancelReasonCB
	reason_kb = InlineKeyboardBuilder()
	reason_kb.button(text=t("master_cancel_reason_ill", locale), callback_data=pack_cb(MasterCancelReasonCB, booking_id=booking_id, code="ill"))
	reason_kb.button(text=t("master_cancel_reason_emergency", locale), callback_data=pack_cb(MasterCancelReasonCB, booking_id=booking_id, code="emergency"))
	reason_kb.button(text=t("master_cancel_reason_error", locale), callback_data=pack_cb(MasterCancelReasonCB, booking_id=booking_id, code="error"))
	reason_kb.button(text=t("master_cancel_reason_other", locale), callback_data=pack_cb(MasterCancelReasonCB, booking_id=booking_id, code="other"))
	reason_kb.button(text="❌", callback_data=pack_cb(MasterMenuCB, act="my_clients"))
	reason_kb.adjust(2,2,1)
	await cb.answer()
	await safe_edit(cb.message, t("master_cancel_reason_prompt", locale), reply_markup=reason_kb.as_markup())
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "cancel"))
async def booking_cancel(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		ok = await master_services.cancel_booking(booking_id)
		if ok:
			await cb.answer(t("booking_cancelled_success", locale))
			try:
				bd = await build_booking_details(booking_id)
			except Exception:
				bd = await build_booking_details(booking_id)
			lang = locale
			try:
				text = format_booking_details_text(bd, lang, role='master')
			except Exception:
				text = format_booking_details_text(bd, lang)
			try:
				markup = build_booking_card_kb(bd, booking_id, role='master', lang=lang)
				await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
			except Exception:
				pass
		else:
			await cb.answer(t("error_retry", locale))
	except Exception:
		await cb.answer(t("error_retry", locale))
	return

@master_router.callback_query(MasterCancelReasonCB.filter())
async def master_cancel_reason(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id = getattr(callback_data, "booking_id", None)
	code = getattr(callback_data, "code", None)
	if not booking_id:
		await cb.answer()
		return
	try:
		bid = int(booking_id)
	except Exception:
		await cb.answer()
		return
	lang = locale
	if code == "other":
		await state.update_data(cancel_booking_id=bid)
		from bot.app.telegram.master.states import MasterStates
		await state.set_state(MasterStates.cancel_reason_text)
		await cb.answer()
		await safe_edit(cb.message, t("master_cancel_reason_enter", lang))
		return
	reason_map = {
		"ill": t("master_cancel_reason_ill", lang),
		"emergency": t("master_cancel_reason_emergency", lang),
		"error": t("master_cancel_reason_error", lang),
	}
	code_str = str(code) if code is not None else ""
	reason_label = reason_map.get(code_str, code_str)
	try:
		ok = await master_services.cancel_booking(bid)
		if ok:
			await cb.answer(t("booking_cancelled_success", lang))
			try:
				bd = await build_booking_details(bid)
			except Exception:
				bd = None
			if bd and cb.message:
				try:
					text = format_booking_details_text(bd, lang, role='master')
				except Exception:
					text = format_booking_details_text(bd, lang)
				text += f"\nПричина: {reason_label}" if reason_label else ""
				try:
					markup = build_booking_card_kb(bd, bid, role='master', lang=lang)
					await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML")
				except Exception:
					pass
		else:
			await cb.answer(t("error_retry", lang))
	except Exception:
		await cb.answer(t("error_retry", lang))
	return

from aiogram.types import Message as _MasterMsgType
@master_router.message(MasterStates.cancel_reason_text)
async def master_cancel_reason_text_input(msg: _MasterMsgType, state: FSMContext, locale: str) -> None:
	lang = locale
	content = (msg.text or "").strip()
	if content.lower() == "/cancel":
		await state.clear()
		await msg.answer(t("action_cancelled", lang))
		return
	data = await state.get_data()
	bid = data.get("cancel_booking_id")
	if not bid:
		await msg.answer(t("error_retry", lang))
		return
	try:
		ok = await master_services.cancel_booking(int(bid))
	except Exception:
		ok = False
	if ok:
		await msg.answer(t("booking_cancelled_success", lang))
	else:
		await msg.answer(t("error_retry", lang))
	try:
		await state.clear()
	except Exception:
		pass


@master_router.callback_query(BookingActionCB.filter(F.act == "add_note"))
async def booking_add_note(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	await state.update_data(client_note_booking_id=booking_id)
	await state.set_state(MasterStates.edit_note)
	try:
		res = await master_services.handle_add_note(booking_id, locale)
		if res:
			prompt, kb = res
			await safe_edit(cb.message, text=prompt, reply_markup=kb)
		else:
			await safe_edit(cb.message, text=t('master_enter_note', locale), reply_markup=None)
	except Exception:
		try:
			await safe_edit(cb.message, text=t('master_enter_note', locale), reply_markup=None)
		except Exception:
			pass
	return


@master_router.callback_query(BookingActionCB.filter(F.act == "cancel_note"))
async def booking_cancel_note(cb: CallbackQuery, callback_data, state: FSMContext, locale: str) -> None:
	booking_id_raw = getattr(callback_data, "booking_id", None)
	if not booking_id_raw:
		await cb.answer()
		return
	try:
		booking_id = int(booking_id_raw)
	except Exception:
		await cb.answer()
		return
	try:
		await state.clear()
	except AttributeError:
		pass
	try:
		res = await master_services.handle_cancel_note(booking_id, locale)
		if res:
			text, markup = res
			await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
		else:
			try:
				bd = await build_booking_details(booking_id, user_id=None, lang=locale)
				text = format_booking_details_text(bd, locale, role="master")
				markup = build_booking_card_kb(bd, booking_id, role="master", lang=locale)
				await safe_edit(cb.message, text=text, reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
			except Exception:
				try:
					text = format_booking_details_text(bd, locale, role='master')
				except Exception:
					text = format_booking_details_text(bd, locale)
				from aiogram.utils.keyboard import InlineKeyboardBuilder
				kb2 = InlineKeyboardBuilder()
				kb2.button(text=t("booking_mark_done_button"), callback_data=pack_cb(BookingActionCB, act="mark_done", booking_id=booking_id))
				kb2.button(text=t("booking_mark_noshow_button"), callback_data=pack_cb(BookingActionCB, act="mark_noshow", booking_id=booking_id))
				kb2.button(text=t("booking_client_history_button"), callback_data=pack_cb(BookingActionCB, act="client_history", booking_id=booking_id))
				kb2.button(text=t("booking_add_note_button"), callback_data=pack_cb(BookingActionCB, act="add_note", booking_id=booking_id))
				try:
					kb2.button(text=t("to_list", locale) if t("to_list", locale) != "to_list" else t("menu", locale), callback_data=pack_cb(MasterMenuCB, act="my_clients"))
				except Exception:
					kb2.button(text=t("menu", locale), callback_data=pack_cb(MasterMenuCB, act="my_clients"))
				kb2.adjust(2)
				await safe_edit(cb.message, text=text, reply_markup=kb2.as_markup(), parse_mode="HTML", disable_web_page_preview=True)
	except Exception as e:
		logger.exception("Failed to cancel_note/render booking card for %s: %s", booking_id, e)
		try:
			await cb.answer()
		except Exception:
			pass
	return

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats"))
async def show_stats(cb: CallbackQuery, state: FSMContext, locale: str) -> None:  # Добавьте locale
	# Prefer middleware-provided `locale`; fall back to default when missing
	lang = locale or default_language()

	# Build a compact summary for both week and month, then show main menu keyboard
	master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	if master_id is None:
		await safe_edit(cb.message, text=t("error_retry", lang))
		return
	try:
		stats_week = await master_services.get_master_stats_summary(int(master_id), days=7)
		stats_month = await master_services.get_master_stats_summary(int(master_id), days=30)
	except Exception:
		stats_week = {}
		stats_month = {}

	text_parts: list[str] = []
	text_parts.append(t("master_stats_header", lang))
	# Week section
	text_parts.append(
		f"\n{t('stats_week', lang)}:\n"
		f"{t('next_booking', lang)}: {stats_week.get('next_booking_time')}\n"
		f"{t('total_bookings', lang)}: {stats_week.get('total_bookings')}\n"
		f"{t('completed_bookings', lang)}: {stats_week.get('completed_bookings')}\n"
		f"{t('no_shows', lang)}: {stats_week.get('no_shows')}"
	)
	# Month section
	text_parts.append(
		f"\n{t('stats_month', lang)}:\n"
		f"{t('next_booking', lang)}: {stats_month.get('next_booking_time')}\n"
		f"{t('total_bookings', lang)}: {stats_month.get('total_bookings')}\n"
		f"{t('completed_bookings', lang)}: {stats_month.get('completed_bookings')}\n"
		f"{t('no_shows', lang)}: {stats_month.get('no_shows')}"
	)
	text = "\n".join(text_parts)
	kb = get_master_main_menu(lang)
	await safe_edit(cb.message, text=text, reply_markup=kb)
	try:
		await nav_replace(state, text, kb, lang=lang)
	except TelegramAPIError:
		# ignore navigation UI failures
		pass

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats_week"))
async def stats_week(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	# Prefer middleware-provided `locale`; fall back to default when missing
	lang = locale or default_language()
	master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	if master_id is None:
		await safe_edit(cb.message, text=t("error_retry", lang))
		return
	stats = await master_services.get_master_stats_summary(int(master_id), days=7)
	text = (
		f"{t('stats_week', lang)}:\n"
		f"{t('next_booking', lang)}: {stats.get('next_booking_time')}\n"
		f"{t('total_bookings', lang)}: {stats.get('total_bookings')}\n"
		f"{t('completed_bookings', lang)}: {stats.get('completed_bookings')}\n"
		f"{t('pending_payment', lang)}: {stats.get('pending_payment')}\n"
		f"{t('no_shows', lang)}: {stats.get('no_shows')}"
	)
	kb = get_master_main_menu(lang)
	await safe_edit(cb.message, text=text, reply_markup=kb)
	await state.update_data(current_screen="stats_week")

@master_router.callback_query(MasterMenuCB.filter(F.act == "stats_month"))
async def stats_month(cb: CallbackQuery, state: FSMContext, locale: str) -> None:
	# Prefer middleware-provided `locale`; fall back to default when missing
	lang = locale or default_language()
	master_id = getattr(getattr(cb, 'from_user', None), 'id', None)
	if master_id is None:
		await safe_edit(cb.message, text=t("error_retry", lang))
		return
	stats = await master_services.get_master_stats_summary(int(master_id), days=30)
	text = (
		f"{t('stats_month', lang)}:\n"
		f"{t('next_booking', lang)}: {stats.get('next_booking_time')}\n"
		f"{t('total_bookings', lang)}: {stats.get('total_bookings')}\n"
		f"{t('completed_bookings', lang)}: {stats.get('completed_bookings')}\n"
		f"{t('pending_payment', lang)}: {stats.get('pending_payment')}\n"
		f"{t('no_shows', lang)}: {stats.get('no_shows')}"
	)
	kb = get_master_main_menu(lang)
	await safe_edit(cb.message, text=text, reply_markup=kb)
	await state.update_data(current_screen="stats_month")


# master-specific bookings rendering moved to `bot.app.services.master_services.render_bookings_page`.
# The legacy inline helper was removed to avoid duplication; the service should be the single source
# of truth for bookings rendering logic.