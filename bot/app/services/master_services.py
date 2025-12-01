from __future__ import annotations

import logging
from datetime import UTC, datetime, date as _date, time as _time, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Iterable, cast
import re

from sqlalchemy import select, and_, func
from sqlalchemy.exc import SQLAlchemyError

from bot.app.domain.models import User # Добавьте User
from bot.app.core.constants import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_DAY_END_HOUR,
    DEFAULT_DAY_START_HOUR,
    DEFAULT_SERVICE_FALLBACK_DURATION,
    DEFAULT_TIME_STEP_MINUTES,
)

from bot.app.core.db import get_session
from bot.app.domain.models import Booking, BookingStatus, MasterClientNote, User, Service, TERMINAL_STATUSES
from bot.app.services.admin_services import ServiceRepo, SettingsRepo
from bot.app.services.shared_services import (
    format_money_cents,
    _parse_hm_to_minutes,
    _minutes_to_hm,
    default_language,
    get_admin_ids,
    format_booking_list_item,
    format_booking_details_text,
    get_local_tz,
    BookingInfo,
    format_user_display_name,
)
from bot.app.translations import tr, t
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from bot.app.telegram.common.callbacks import pack_cb, BookingActionCB, BookingsPageCB, NavCB
from datetime import timezone

logger = logging.getLogger(__name__)


def compute_time_end_items(day: int, start_time: str, *, end_hour: int = DEFAULT_DAY_END_HOUR, step_min: int = DEFAULT_TIME_STEP_MINUTES) -> list[tuple[str, str]]:
    """Compute end-time button items for a given day and chosen start_time.

    Returns a list of tuples (label_text, callback_data) suitable for keyboard builders.

    This extracts the time computation logic out of UI and places it in the
    master services layer so handlers can precompute items and keyboards remain
    pure rendering helpers.
    """
    try:
        from bot.app.telegram.common.callbacks import MasterScheduleCB, pack_cb
    except Exception:
        # If callbacks can't be imported (tests/static analysis), return an
        # empty list so callers can fall back to conservative UI.
        return []

    try:
        start_minutes = _parse_hm_to_minutes(start_time)
    except Exception:
        start_minutes = 0

    items: list[tuple[str, str]] = []
    # generate times from 0:00 to end_hour with step_min increments
    try:
        step = int(step_min or 30)
        for minutes in range(0, (end_hour * 60) + 1, step):
            if minutes <= start_minutes:
                continue
            # format as HH:MM
            h = minutes // 60
            m = minutes % 60
            tstr = f"{h:02d}:{m:02d}"
            token = tstr.replace(":", "")
            try:
                cb = pack_cb(MasterScheduleCB, action="pick_end", day=day, time=token)
            except Exception:
                cb = ""
            items.append((tstr, cb))
    except Exception:
        return []

    return items


def build_time_slot_list(
    *,
    start_hour: int = DEFAULT_DAY_START_HOUR,
    end_hour: int = DEFAULT_DAY_END_HOUR,
    step_min: int = DEFAULT_TIME_STEP_MINUTES,
) -> list[str]:
    """Return time labels from start to end (inclusive) using the configured step."""
    times: list[str] = []
    for hour in range(start_hour, end_hour + 1):
        for minute in range(0, 60, step_min):
            if hour == end_hour and minute > 30:
                continue
            times.append(f"{hour:02d}:{minute:02d}")
    return times


def format_master_profile_text(data: dict | None, lang: str, *, with_title: bool = True) -> str:
    """Pure formatter: build profile text from pre-fetched `data`.

    This was previously defined in the master UI module. Move it here so
    formatting logic lives in the service layer and keyboards receive ready
    text or DTOs.
    """
    try:
        if not data:
            return tr("master_not_found", lang=lang)

        master = data.get("master")
        services = data.get("services") or []
        durations_map = data.get("durations_map") or {}
        about_text = data.get("about_text")
        reviews = data.get("reviews") or []

        lines: list[str] = []
        if with_title:
            title = tr("profile_title", lang=lang)
            lines.append(title)
        uname = getattr(master, "username", None)
        master_name = getattr(master, "name", "")
        master_tid = getattr(master, "telegram_id", "")
        lines.append(f"👤 {master_name} (@{uname})" if uname else f"👤 {master_name}")
        lines.append(f"🆔 {master_tid}")
        if getattr(master, "phone", None):
            lines.append(f"📞 {getattr(master, 'phone', '')}")
        if getattr(master, "email", None):
            lines.append(f"✉️ {getattr(master, 'email', '')}")

        if services:
            lines.append("")
            lines.append(tr("services_list_title", lang=lang))
            for sid, sname, category, price_cents, currency in services:
                dur = durations_map.get(str(sid))
                dur_txt = f"{dur} {tr('minutes_short', lang=lang)}" if isinstance(dur, int) and dur > 0 else None
                price_txt = format_money_cents(price_cents or 0, currency or "UAH")
                tail = []
                if dur_txt:
                    tail.append(f"({dur_txt})")
                if price_txt:
                    tail.append(f"— {price_txt}")
                head = f"• {sname}" if not category else f"• {category} → {sname}"
                lines.append(" ".join([head] + tail) if tail else head)
        else:
            lines.append("")
            lines.append("❌ " + tr("no_services_for_master", lang=lang))

        if getattr(master, "rating", None):
            lines.append("")
            rating_label = tr("rating_label", lang=lang)
            orders_word = tr("orders", lang=lang)
            lines.append(
                f"⭐ {rating_label}: {getattr(master, 'rating', 0):.1f}/5 ({int(getattr(master, 'completed_orders', 0) or 0)} {orders_word})"
            )

        if about_text:
            lines.append("")
            about_title = tr("about_title", lang=lang)
            lines.append(about_title)
            lines.append(str(about_text))

        reviews = data.get("reviews") or []
        if reviews:
            lines.append("")
            rv_title = tr("reviews_title", lang=lang)
            lines.append(rv_title)
            for rating, comment in reviews:
                if comment:
                    lines.append(f"• \"{comment}\"")
                else:
                    lines.append(f"• ⭐ {rating}/5")

        sched = data.get("schedule") or {}
        if isinstance(sched, dict):
            try:
                lines.append("")
                sched_title = tr("schedule_title", lang=lang)
                lines.append(f"{sched_title}:")
                wd_full = tr("weekday_full", lang=lang)
                for i in range(7):
                    # Support both string and integer keys in stored schedule dicts.
                    windows = sched.get(str(i)) if isinstance(sched, dict) else None
                    if not windows:
                        windows = sched.get(i) if isinstance(sched, dict) else None
                    if not windows:
                        windows = []
                    if not windows:
                        lines.append(f"• {wd_full[i]}: —")
                        continue
                    parts: list[str] = []
                    for w in windows:
                        try:
                            if isinstance(w, (list, tuple)) and len(w) >= 2:
                                parts.append(f"{w[0]}–{w[1]}")
                            else:
                                s = str(w)
                                if "-" in s:
                                    a,b = s.split("-",1)
                                    parts.append(f"{a.strip()}–{b.strip()}")
                        except Exception:
                            continue
                    lines.append(f"• {wd_full[i]}: {', '.join(parts) if parts else '—'}")
            except Exception:
                pass
        return "\n".join(lines)
    except Exception:
        return tr("error", lang=lang)


# ---------------- Masters cache (moved here from shared_services) ----------------
_masters_cache_store: dict[int, str] | None = None

# ---------------- Master schedule caching (removed) ----------------
# DB-only strategy: previous in-memory read-through cache removed to avoid
# multi-process divergence. All schedule reads now query the DB directly.

async def masters_cache() -> dict[int, str]:
    """Return cached masters mapping {telegram_id: name} loaded from the DB.

    This was moved from shared_services into master_services to keep master
    repository helpers together with MasterRepo.
    """
    global _masters_cache_store
    if _masters_cache_store is not None:
        return _masters_cache_store
    try:
        async with get_session() as session:
            from bot.app.domain.models import Master

            res = await session.execute(
                select(
                    Master.telegram_id,
                    Master.username,
                    Master.first_name,
                    Master.last_name,
                    Master.name,
                )
            )
            rows = res.all()
            if rows:
                cache: dict[int, str] = {}
                for r in rows:
                    try:
                        tid = int(r[0])
                    except Exception:
                        continue
                    username = r[1]
                    first_name = r[2]
                    last_name = r[3]
                    fallback_name = r[4]
                    formatted = format_user_display_name(username, first_name, last_name)
                    if not formatted:
                        formatted = str(fallback_name) if fallback_name is not None else str(tid)
                    cache[tid] = formatted
                _masters_cache_store = cache
            else:
                logger.info("masters_cache: DB empty; returning empty masters mapping")
                _masters_cache_store = {}
    except Exception as e:
        logger.exception("masters_cache unexpected error: %s", e)
        _masters_cache_store = {}
    return _masters_cache_store or {}


_MASTER_TEXT_DEFAULTS: dict[str, str] = {
    "unknown_client": "unknown",
    "no_visits": "Нет",
    "no_notes": "Нет",
}



# Role-based booking formatting now uses shared `format_booking_list_item(..., role="master")`.


async def get_master_bookings(
    *,
    master_id: int,
    mode: str = "upcoming",
    page: int = 1,
    page_size: int | None = 5,
    start: datetime | None = None,
    end: datetime | None = None,
) -> tuple[Sequence[Any], dict[str, Any]]:
    """
    # Master-specific pagination: delegates directly to BookingRepo.get_paginated_list.
    """
    # Delegate to canonical BookingRepo provider to centralize booking queries
    try:
        from bot.app.services.client_services import BookingRepo
        return await BookingRepo.get_paginated_list(master_id=master_id, mode=mode, page=page, page_size=page_size, start=start, end=end)
    except Exception:
        # Fallback: return empty page
        return [], {"total": 0, "total_pages": 1, "page": 1, "done_count": 0, "cancelled_count": 0, "noshow_count": 0, "upcoming_count": 0}


async def get_master_dashboard_summary(master_id: int, *, lang: str | None = None) -> str:
    """Build a small "today" dashboard summary string for a master.

    Returns a localized text block ready to be prepended to the master menu.
    """
    try:
        l = lang or await SettingsRepo.get_setting("language", default_language())

        # compute local day bounds and convert to UTC
        local_tz = get_local_tz() or UTC
        try:
            now_utc = datetime.now(UTC)
            local_now = now_utc.astimezone(local_tz)
            local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            local_day_end = local_day_start + timedelta(days=1)
            day_start_utc = local_day_start.astimezone(UTC)
            day_end_utc = local_day_end.astimezone(UTC)
        except Exception:
            now_utc = datetime.now(UTC)
            day_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end_utc = day_start_utc + timedelta(days=1)
            local_day_start = day_start_utc.astimezone(local_tz)

        # Optimized: fetch top N upcoming bookings for today (for list display)
        rows, _meta = await get_master_bookings(
            master_id=int(master_id),
            start=day_start_utc,
            end=day_end_utc,
            mode="upcoming",
            page=1,
            page_size=DEFAULT_PAGE_SIZE,
        )
        # Format rows inline using shared formatter (role-aware).
        formatted_rows: list[tuple[str, int]] = []
        for r in rows:
            try:
                txt, bid = format_booking_list_item(r, role="master", lang=l)
                formatted_rows.append((txt, bid))
            except Exception:
                continue

        # Separate aggregate query for full-day counts to avoid incorrect
        # totals caused by limiting page_size above (previous bug: totals
        # reflected only first 5 bookings).
        try:
            from sqlalchemy import select, func, case
            from bot.app.domain.models import Booking, BookingStatus
            async with get_session() as session:
                counts_stmt = (
                    select(
                        func.count(Booking.id).label("total"),
                        func.sum(case((Booking.status == BookingStatus.DONE, 1), else_=0)).label("done"),
                        func.sum(case((Booking.status == BookingStatus.CANCELLED, 1), else_=0)).label("cancelled"),
                    )
                    .where(
                        Booking.master_id == int(master_id),
                        Booking.starts_at >= day_start_utc,
                        Booking.starts_at < day_end_utc,
                    )
                )
                res = await session.execute(counts_stmt)
                c_row = res.first()
                if c_row:
                    total = int(c_row[0] or 0)
                    done = int(c_row[1] or 0)
                    cancelled = int(c_row[2] or 0)
                else:
                    total = done = cancelled = 0
                awaiting = max(0, total - done - cancelled)
        except Exception:
            total = done = cancelled = awaiting = 0

        if formatted_rows:
            # Helper: remove emoji and other pictographic symbols from translations


            date_label = local_day_start.strftime("%d.%m.%Y") if 'local_day_start' in locals() else ""
            header_raw = tr("master_schedule_today_header", lang=l).format(date=date_label) if date_label else tr("master_schedule_today_header", lang=l)
            header = (header_raw)

            # Build vertical stats (one stat per line), using translations when available
            today_lbl = tr("dashboard_today_label", lang=l).replace("{count}", "").strip() or "Today"
            done_lbl = tr("dashboard_done_label", lang=l).replace("{count}", "").strip() or "Done"
            cancelled_lbl = tr("dashboard_cancelled_label", lang=l).replace("{count}", "").strip() or "Cancelled"
            pending_lbl = tr("dashboard_pending_label", lang=l).replace("{count}", "").strip() or "Pending"

            lines = [header]
            lines.append(f"{today_lbl}: {total}")
            lines.append(f"{done_lbl}: {done}")
            lines.append(f"{cancelled_lbl}: {cancelled}")
            lines.append(f"{pending_lbl}: {awaiting}")

            # Append up to 5 compact booking rows, one per line (no leading dash)
            for txt, _id in formatted_rows[:5]:
                lines.append(txt)
            summary = "\n".join(lines)
        else:
            summary = tr("master_no_bookings_today", lang=l)

        # also fetch a 7-day stats summary and append to dashboard
        try:
            stats = await get_master_stats_summary(int(master_id), days=7)
        except Exception:
            stats = {"total_bookings": 0, "completed_bookings": 0, "no_shows": 0, "next_booking_time": None}
        try:
            # Build 7-day stats as vertical lines without emojis
            total7_lbl = t('master_stats_7d_total', l).split(':')[0] if t('master_stats_7d_total', l) else 'Total'
            done7_lbl = t('master_stats_7d_done', l).split(':')[0] if t('master_stats_7d_done', l) else 'Done'
            noshow7_lbl = t('master_stats_7d_noshow', l).split(':')[0] if t('master_stats_7d_noshow', l) else 'No-shows'
            seven_lines = ["", (t('last_7_days', l) or 'Last 7 days:')]
            seven_lines.append(f"{total7_lbl}: {stats.get('total_bookings', 0)}")
            seven_lines.append(f"{done7_lbl}: {stats.get('completed_bookings', 0)}")
            seven_lines.append(f"{noshow7_lbl}: {stats.get('no_shows', 0)}")
            # Revenue (format cents to human-friendly string)
            try:
                rev_cents = int(stats.get('revenue_cents', 0) or 0)
                from bot.app.services.shared_services import format_money_cents

                rev_txt = format_money_cents(rev_cents, 'UAH')
            except Exception:
                rev_txt = str(int(stats.get('revenue_cents', 0) or 0) / 100.0)
            seven_lines.append(f"Revenue: {rev_txt}")
            # Avg per day
            try:
                avgd = float(stats.get('avg_per_day', 0.0) or 0.0)
                seven_lines.append(f"Avg/day: {avgd:.1f}")
            except Exception:
                pass
            # No-show rate
            try:
                nsr = float(stats.get('no_show_rate', 0.0) or 0.0)
                seven_lines.append(f"No-show rate: {nsr:.1f}%")
            except Exception:
                pass
            # Next booking time
            try:
                if stats.get('next_booking_time'):
                    seven_lines.append(f"Next: {stats.get('next_booking_time')}")
            except Exception:
                pass
            seven_line = "\n" + "\n".join(seven_lines)
        except Exception:
            seven_line = ""

        return f"{summary}{seven_line}"
    except Exception:
        return tr("master_menu_header", lang=lang or default_language())


async def handle_mark_done(booking_id: int, lang: str | None = None) -> tuple[bool, str, InlineKeyboardMarkup]:
    """Service handler that marks booking done and returns updated card text+kbd for master view.

    Encapsulates DB update + data fetching + formatting so handlers remain thin.
    """
    try:
        from bot.app.services.client_services import BookingRepo, build_booking_details

        ok = await BookingRepo.update_status(booking_id, BookingStatus.DONE)
        from bot.app.telegram.client.client_keyboards import build_booking_card_kb
        bd = await build_booking_details(booking_id)
        txt = format_booking_details_text(bd, lang or default_language(), role="master")
        kb = build_booking_card_kb(bd, booking_id, role="master", lang=lang)
        return bool(ok), txt, kb
    except Exception as e:
        logger.exception("handle_mark_done failed for %s: %s", booking_id, e)
        # Fallback: return a generic retry message and empty keyboard
        from aiogram.types import InlineKeyboardMarkup
        return False, t("error_retry", lang or default_language()), InlineKeyboardMarkup(inline_keyboard=[])


async def handle_mark_noshow(booking_id: int, lang: str | None = None) -> tuple[bool, str, InlineKeyboardMarkup]:
    """Service handler that marks booking as no-show and returns updated master card text+kbd."""
    try:
        from bot.app.services.client_services import BookingRepo, build_booking_details

        ok = await BookingRepo.update_status(booking_id, BookingStatus.NO_SHOW)
        from bot.app.telegram.client.client_keyboards import build_booking_card_kb
        bd = await build_booking_details(booking_id)
        txt = format_booking_details_text(bd, lang or default_language(), role="master")
        kb = build_booking_card_kb(bd, booking_id, role="master", lang=lang)
        return bool(ok), txt, kb
    except Exception as e:
        logger.exception("handle_mark_noshow failed for %s: %s", booking_id, e)
        from aiogram.types import InlineKeyboardMarkup
        return False, t("error_retry", lang or default_language()), InlineKeyboardMarkup(inline_keyboard=[])


async def handle_client_history(booking_id: int, lang: str | None = None) -> tuple[str, InlineKeyboardMarkup] | None:
    """Return (view_text, kb) for client history; None indicates no history available."""
    try:
        view = await build_client_history_view(booking_id)
        if not view:
            return None
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        kb = InlineKeyboardBuilder()
        from bot.app.telegram.common.callbacks import pack_cb, BookingActionCB
        kb.button(text=t("back", lang or default_language()), callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id))
        kb.adjust(1)
        return view, kb.as_markup()
    except Exception as e:
        logger.exception("handle_client_history failed for %s: %s", booking_id, e)
        return None


async def handle_add_note(booking_id: int, lang: str | None = None) -> tuple[str, InlineKeyboardMarkup]:
    """Return prompt text and keyboard to ask master to enter/edit client note."""
    try:
        # Try to load existing client note from booking display data
        bd = await MasterRepo.get_booking_display_data(booking_id)
        existing_note = None
        if bd and isinstance(bd, dict):
            existing_note = bd.get("client_note")

        from aiogram.utils.keyboard import InlineKeyboardBuilder
        from bot.app.telegram.common.callbacks import pack_cb, BookingActionCB
        kb = InlineKeyboardBuilder()
        kb.button(text=t("cancel" , lang or default_language()), callback_data=pack_cb(BookingActionCB, act="cancel_note", booking_id=booking_id))
        kb.adjust(1)

        if existing_note and isinstance(existing_note, str) and existing_note.strip():
            prompt = f"{t('master_enter_note', lang or default_language())}\n\n{t('master_current_note_prefix', lang or default_language())}: {existing_note}"
        else:
            prompt = t('master_enter_note', lang or default_language())

        return prompt, kb.as_markup()
    except Exception as e:
        logger.exception("handle_add_note failed for %s: %s", booking_id, e)
        from aiogram.types import InlineKeyboardMarkup
        return (t('master_enter_note', lang or default_language()), InlineKeyboardMarkup(inline_keyboard=[]))


async def handle_cancel_note(booking_id: int, lang: str | None = None) -> tuple[str, InlineKeyboardMarkup] | None:
    """Return booking card text and markup to restore master booking view after cancelling note edit."""
    try:
        # Reuse client_services to build canonical booking details and card
        from bot.app.services.client_services import build_booking_details
        from bot.app.telegram.client.client_keyboards import build_booking_card_kb

        bd = await build_booking_details(booking_id)
        text = format_booking_details_text(bd, lang or 'uk', role='master')
        kb = build_booking_card_kb(bd, booking_id, role='master', lang=lang)
        return text, kb
    except Exception as e:
        logger.exception("handle_cancel_note failed for %s: %s", booking_id, e)
        return None


def invalidate_masters_cache() -> None:
    """Invalidate masters cache (useful after CRUD)."""
    global _masters_cache_store
    _masters_cache_store = None



# ---------------- MasterRepo (merged from shared_services) -----------------
class MasterRepo:
    """Repository for Master-related persistence (profiles, schedules, bio).

    This implementation contains the canonical methods previously defined
    in `shared_services.MasterRepo`. It consolidates master-related DB
    access in one place so callers can import `MasterRepo` from
    `bot.app.services.master_services` without indirection.
    """

    @staticmethod
    async def get_schedule(master_telegram_id: int) -> dict:
        """Return normalized schedule dict (DB-only).

        Reads from `master_schedules` table; returns {} if none defined.
        Legacy JSON fallback removed.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterProfile, MasterSchedule
                prof = await session.scalar(select(MasterProfile.id).where(MasterProfile.master_telegram_id == master_telegram_id))
                if prof is None:
                    return {}
                ms_stmt = select(MasterSchedule.day_of_week, MasterSchedule.start_time, MasterSchedule.end_time).where(
                    MasterSchedule.master_profile_id == prof
                ).order_by(MasterSchedule.day_of_week, MasterSchedule.start_time)
                ms_res = await session.execute(ms_stmt)
                rows = ms_res.all()
                if not rows:
                    return {}
                sched: dict[str, list[list[str]]] = {}
                for dow, st, et in rows:
                    try:
                        s = st.strftime('%H:%M') if hasattr(st, 'strftime') else str(st)
                        e = et.strftime('%H:%M') if hasattr(et, 'strftime') else str(et)
                    except Exception:
                        s = str(st)
                        e = str(et)
                    sched.setdefault(str(int(dow)), []).append([s, e])
                from bot.app.services.master_services import _normalize_schedule
                return _normalize_schedule(sched)
        except Exception as e:
            logger.warning("MasterRepo.get_schedule failed for %s: %s", master_telegram_id, e)
            return {}

    @staticmethod
    async def set_schedule(master_telegram_id: int, schedule: dict[str, Any]) -> bool:
        """Persist canonical schedule into MasterProfile.bio.schedule (legacy store).

        After full migration, this should write into `master_schedules` rows instead
        of the JSON bio; retained here for transitional compatibility.
        """
        try:
            if not isinstance(schedule, dict):
                logger.warning("MasterRepo.set_schedule: rejecting non-dict schedule for %s", master_telegram_id)
                return False
            import json
            async with get_session() as session:
                from bot.app.domain.models import MasterProfile
                from sqlalchemy import select
                prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
                canonical = schedule or {}
                if not prof:
                    prof = MasterProfile(master_telegram_id=master_telegram_id, bio=json.dumps({"schedule": canonical}))
                    session.add(prof)
                else:
                    try:
                        bio_obj = json.loads(prof.bio or "{}") or {}
                    except Exception:
                        bio_obj = {}
                    bio_obj["schedule"] = {str(k): v for k, v in canonical.items()}
                    prof.bio = json.dumps(bio_obj)
                await session.commit()
            logger.info("MasterRepo.set_schedule: schedule set for %s", master_telegram_id)
            return True
        except Exception as e:
            logger.exception("MasterRepo.set_schedule failed for %s: %s", master_telegram_id, e)
            return False

    @staticmethod
    async def get_bookings_for_period(
        master_telegram_id: int,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        days: int | None = 7,
    ) -> list[Any]:
        """Return list of Booking objects for the master in given period.

        Mirrors previous service behaviour; centralizes DB access.
        """
        try:
            if start is None:
                base = datetime.now(UTC)
                if days is None:
                    start = base
                    end = None
                else:
                    start = base
                    end = base + timedelta(days=days)
            else:
                if end is None:
                    if days is not None:
                        end = start + timedelta(days=days)
                    else:
                        end = None

            async with get_session() as session:
                from bot.app.domain.models import Booking, BookingStatus
                from sqlalchemy import select

                stmt = select(Booking).where(Booking.master_id == master_telegram_id)
                if end is not None:
                    stmt = stmt.where(Booking.starts_at.between(start, end))
                else:
                    stmt = stmt.where(Booking.starts_at >= start)

                stmt = stmt.where(
                    Booking.status.notin_(tuple(TERMINAL_STATUSES))
                ).order_by(Booking.starts_at)

                result = await session.execute(stmt)
                bookings = list(result.scalars().all())
                logger.info(
                    "MasterRepo.get_bookings_for_period: got %d bookings for master %s",
                    len(bookings),
                    master_telegram_id,
                )
                return bookings
        except Exception as e:
            logger.exception("MasterRepo.get_bookings_for_period failed for %s: %s", master_telegram_id, e)
            return []

    # --- Pagination helpers (avoid loading entire master list into FSM state) ---
    @staticmethod
    async def count_masters() -> int:
        """Return total number of masters."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select, func
                return int((await session.execute(select(func.count()).select_from(Master))).scalar() or 0)
        except Exception as e:
            logger.warning("MasterRepo.count_masters failed: %s", e)
            return 0

    @staticmethod
    async def get_masters_page(page: int = 1, page_size: int = 10) -> list[tuple[int, str]]:
        """Return page of masters as (telegram_id, name)."""
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 10
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select
                offset = (page - 1) * page_size
                stmt = select(Master.telegram_id, Master.name).order_by(Master.telegram_id).offset(offset).limit(page_size)
                rows = (await session.execute(stmt)).all()
                return [(int(r[0]), str(r[1]) if r[1] is not None else "") for r in rows]
        except Exception as e:
            logger.warning("MasterRepo.get_masters_page failed (page=%s): %s", page, e)
            return []

    @staticmethod
    async def get_booking_display_data(booking_id: int) -> dict[str, Any] | None:
        """Return display-friendly dict for booking (centralized)."""
        try:
            async with get_session() as session:
                # We'll load Booking + User + Master.name + MasterClientNote.note +
                # BookingItem.service_id + Service.name in a single joined query.
                # This returns one or more rows (one per booking item). We'll
                # assemble service names from the rows and take the booking/user
                # fields from the first row.
                from bot.app.domain.models import Booking, User, MasterClientNote, BookingItem, Service as Svc, Master
                from sqlalchemy import func, cast, String

                # Use Postgres COALESCE(string_agg(...), Booking.service_id::text) to
                # return a filled service_name even if there are no BookingItem rows.
                service_expr = func.coalesce(func.string_agg(Svc.name, ' + '), cast(Booking.service_id, String)).label("service_name")
                currency_expr = func.max(Svc.currency).label("currency")

                stmt = (
                    select(
                        Booking,
                        User,
                        Master.name.label("master_name"),
                        MasterClientNote.note.label("client_note"),
                        service_expr,
                        currency_expr,
                    )
                    .outerjoin(User, User.id == Booking.user_id)
                    .outerjoin(Master, Master.telegram_id == Booking.master_id)
                    .outerjoin(
                        MasterClientNote,
                        and_(
                            MasterClientNote.master_telegram_id == Booking.master_id,
                            MasterClientNote.user_id == Booking.user_id,
                        ),
                    )
                    .outerjoin(BookingItem, BookingItem.booking_id == Booking.id)
                    .outerjoin(Svc, Svc.id == BookingItem.service_id)
                    .where(Booking.id == booking_id)
                    .group_by(Booking.id, User.id, Master.name, MasterClientNote.note, Booking.service_id)
                )

                res = await session.execute(stmt)
                row = res.first()
                if not row:
                    return None

                booking_obj = row[0]
                client = row[1]
                master_name = row[2]
                client_note = row[3]
                service_name = row[4] or str(getattr(booking_obj, "service_id", ""))

                # currency_expr is selected as the next column after service_name
                currency = None
                try:
                    currency = getattr(row, "currency", None) or row[5]
                except Exception:
                    currency = None

                price_cents = getattr(booking_obj, "final_price_cents", None) or getattr(booking_obj, "original_price_cents", None) or 0
                currency = currency or getattr(booking_obj, "currency", None) or "UAH"

                data = {
                    "booking_id": getattr(booking_obj, "id", booking_id),
                    "service_name": service_name,
                    "master_name": master_name,
                    "price_cents": price_cents,
                    "currency": currency,
                    "starts_at": getattr(booking_obj, "starts_at", None),
                    "client_id": getattr(client, "id", None) if client else getattr(booking_obj, "user_id", None),
                    "client_name": getattr(client, "name", None) if client else None,
                    "client_telegram_id": getattr(client, "telegram_id", None) if client else None,
                    "master_id": getattr(booking_obj, "master_id", None),
                    "client_note": client_note,
                }
                return data
        except Exception as e:
            logger.exception("MasterRepo.get_booking_display_data failed: %s", e)
            return None

    @staticmethod
    async def upsert_client_note(booking_id: int, note_text: str) -> bool:
        """Insert or update MasterClientNote for booking's master and user."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Booking, MasterClientNote
                from sqlalchemy import select, and_
                booking = await session.get(Booking, booking_id)
                if not (booking and booking.user_id and booking.master_id):
                    logger.warning("MasterRepo.upsert_client_note: booking or fields missing: %s", booking_id)
                    return False

                note = await session.scalar(
                    select(MasterClientNote).where(
                        and_(
                            MasterClientNote.master_telegram_id == booking.master_id,
                            MasterClientNote.user_id == booking.user_id,
                        )
                    )
                )
                if note:
                    note.note = note_text
                else:
                    note = MasterClientNote(
                        master_telegram_id=booking.master_id,
                        user_id=booking.user_id,
                        note=note_text,
                    )
                    session.add(note)
                await session.commit()
                logger.info("MasterRepo.upsert_client_note: note updated for booking %s", booking_id)
                return True
        except Exception as e:
            logger.exception("MasterRepo.upsert_client_note failed for %s: %s", booking_id, e)
            return False

    @staticmethod
    async def get_master_bio(master_telegram_id: int) -> dict[str, Any]:
        """Return full parsed MasterProfile.bio as dict or {} on error/not found."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterProfile
                prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
                if not prof or not getattr(prof, "bio", None):
                    return {}
                import json
                try:
                    return json.loads(prof.bio or "{}") or {}
                except Exception:
                    return {}
        except Exception as e:
            logger.warning("MasterRepo.get_master_bio failed for %s: %s", master_telegram_id, e)
            return {}

    @staticmethod
    async def update_master_bio(master_telegram_id: int, bio: dict[str, Any]) -> bool:
        """Overwrite MasterProfile.bio with given dict (stores JSON)."""
        try:
            import json
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterProfile
                prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
                if not prof:
                    prof = MasterProfile(master_telegram_id=master_telegram_id, bio=json.dumps(bio or {}))
                    session.add(prof)
                else:
                    prof.bio = json.dumps(bio or {})
                await session.commit()
            # Legacy schedule key (if present) is ignored; schedule now lives solely
            # in master_schedules table via set_master_schedule.
            logger.info("MasterRepo.update_master_bio: bio updated for %s", master_telegram_id)
            return True
        except Exception as e:
            logger.exception("MasterRepo.update_master_bio failed for %s: %s", master_telegram_id, e)
            return False

    @staticmethod
    async def get_client_history_for_master(booking_id: int) -> dict[str, Any] | None:
        """Return a mapping with client history for the booking's master.

        Mapping contains keys: name, visits, total_spent_cents, last_visit, note, rating
        """
        try:
            async with get_session() as session:
                from bot.app.domain.models import Booking
                current_booking = await session.get(Booking, booking_id)
                if not current_booking:
                    return None
                client_id = getattr(current_booking, "user_id", None)
                master_id = getattr(current_booking, "master_id", None)
                if client_id is None or master_id is None:
                    return None
            return await MasterRepo.get_client_history_for_master_by_user(int(master_id), int(client_id))
        except Exception as e:
            logger.exception("MasterRepo.get_client_history_for_master failed for %s: %s", booking_id, e)
            return None

    @staticmethod
    async def get_client_history_for_master_by_user(master_telegram_id: int, user_id: int) -> dict[str, Any] | None:
        """Return a mapping with client history for the given master/user pair.

        Mapping contains keys: name, visits, total_spent_cents, last_visit, note
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select, and_
                from bot.app.domain.models import Booking, MasterClientNote, User

                history_stmt = (
                    select(Booking)
                    .where(Booking.user_id == int(user_id), Booking.master_id == int(master_telegram_id))
                    .order_by(Booking.starts_at.desc())
                )
                history_result = await session.execute(history_stmt)
                all_bookings = history_result.scalars().all()

                note = await session.scalar(
                    select(MasterClientNote.note).where(
                        and_(
                            MasterClientNote.master_telegram_id == int(master_telegram_id),
                            MasterClientNote.user_id == int(user_id),
                        )
                    )
                )

                total_spent_cents = 0
                try:
                    for b in all_bookings:
                        if getattr(b, "status", None) in (
                            getattr(__import__("bot.app.domain.models", fromlist=["BookingStatus"]).BookingStatus, "PAID"),
                            getattr(__import__("bot.app.domain.models", fromlist=["BookingStatus"]).BookingStatus, "CONFIRMED"),
                            getattr(__import__("bot.app.domain.models", fromlist=["BookingStatus"]).BookingStatus, "DONE"),
                        ):
                            total_spent_cents += int(getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", 0) or 0)
                except Exception:
                    total_spent_cents = 0

                user = await session.get(User, user_id)
                texts = _MASTER_TEXT_DEFAULTS
                history = {
                    "name": getattr(user, "name", None) if user else texts.get("unknown_client", "unknown"),
                    "visits": len(all_bookings),
                    "total_spent_cents": total_spent_cents,
                    "total_spent": format_money_cents(total_spent_cents, "UAH"),
                    "last_visit": all_bookings[0].starts_at.strftime('%d.%m.%Y') if all_bookings else texts.get("no_visits", "Нет"),
                    "note": note or texts.get("no_notes", ""),
                }
                logger.info("MasterRepo.get_client_history_for_master_by_user: history built for master=%s user=%s", master_telegram_id, user_id)
                return history
        except Exception as e:
            logger.exception("MasterRepo.get_client_history_for_master_by_user failed for %s/%s: %s", master_telegram_id, user_id, e)
            return None

    @staticmethod
    async def upsert_client_note_for_user(master_telegram_id: int, user_id: int, note_text: str) -> bool:
        """Create or update MasterClientNote by master telegram id and user id."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterClientNote
                from sqlalchemy import select, and_

                note = await session.scalar(
                    select(MasterClientNote).where(
                        and_(
                            MasterClientNote.master_telegram_id == int(master_telegram_id),
                            MasterClientNote.user_id == int(user_id),
                        )
                    )
                )
                if note:
                    note.note = note_text
                else:
                    note = MasterClientNote(master_telegram_id=int(master_telegram_id), user_id=int(user_id), note=note_text)
                    session.add(note)
                await session.commit()
            logger.info("MasterRepo.upsert_client_note_for_user: updated note for master=%s user=%s", master_telegram_id, user_id)
            return True
        except Exception as e:
            logger.exception("MasterRepo.upsert_client_note_for_user failed for %s/%s: %s", master_telegram_id, user_id, e)
            return False

    @staticmethod
    async def get_master_profile_data(master_id: int) -> dict[str, Any] | None:
        """Fetch master profile composed data: master, services, durations_map, about_text, reviews."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master, Service, MasterService, MasterProfile, BookingRating, Booking

                master = await session.get(Master, master_id)
                if not master:
                    return None

                # services offered by master
                svc_stmt = (
                    select(Service.id, Service.name, Service.category, Service.price_cents, Service.currency)
                    .join(MasterService, MasterService.service_id == Service.id)
                    .where(MasterService.master_telegram_id == master_id)
                )
                res = await session.execute(svc_stmt)
                services = [(str(r[0]), r[1], r[2], r[3], r[4]) for r in res.all()]

                # profile bio -> durations and about
                try:
                    prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_id))
                    import json
                    bio = json.loads(prof.bio or "{}") if prof and getattr(prof, "bio", None) else {}
                except Exception:
                    bio = {}

                # Start from bio-provided durations (legacy), then override with MasterService table values
                durations_map = bio.get("durations") or bio.get("durations_map") or {}
                # Normalize to str->int where possible
                try:
                    durations_map = {str(k): int(v) for k, v in (durations_map or {}).items() if k}
                except Exception:
                    # Leave as-is if normalization fails
                    pass
                # Fetch any explicit overrides from master_services table and merge (overrides take precedence)
                try:
                    ms_rows = await session.execute(
                        select(MasterService.service_id, MasterService.duration_minutes).where(MasterService.master_telegram_id == master_id)
                    )
                    for sid, mdur in ms_rows.all():
                        try:
                            if mdur is None:
                                # explicit null means no override; skip
                                continue
                            durations_map[str(sid)] = int(mdur)
                        except Exception:
                            continue
                except Exception:
                    # Non-fatal: keep durations_map from bio
                    pass
                # Merge any per-service overrides stored on MasterService.duration_minutes
                try:
                    ms_rows = await session.execute(
                        select(MasterService.service_id, MasterService.duration_minutes).where(
                            MasterService.master_telegram_id == master_id
                        )
                    )
                    for sid, dur in ms_rows.all():
                        try:
                            if dur is None:
                                # allow bio map to control when override is not set
                                continue
                            dval = int(dur)
                            if dval > 0:
                                durations_map[str(sid)] = dval
                        except Exception:
                            continue
                except Exception:
                    # ignore DB errors here and keep bio durations_map only
                    pass
                about_text = bio.get("about") or None

                # recent reviews (rating, comment)
                reviews_stmt = (
                    select(BookingRating.rating, BookingRating.comment)
                    .join(Booking, Booking.id == BookingRating.booking_id)
                    .where(Booking.master_id == master_id)
                    .order_by(BookingRating.id.desc())
                    .limit(5)
                )
                rev_res = await session.execute(reviews_stmt)
                reviews = [(int(r[0]) if r[0] is not None else 0, r[1]) for r in rev_res.all()]

                data = {
                    "master": master,
                    "services": services,
                    "durations_map": durations_map,
                    "about_text": about_text,
                    "reviews": reviews,
                }
                return data
        except Exception as e:
            logger.exception("MasterRepo.get_master_profile_data failed for %s: %s", master_id, e)
            return None


    @staticmethod
    async def add_master(
        telegram_id: int,
        name: str | None = None,
        *,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
    ) -> bool:
        """Create a Master row if not exists."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select
                if await session.scalar(select(Master).where(Master.telegram_id == telegram_id)):
                    return False
                display_name = name or format_user_display_name(username, first_name, last_name) or str(telegram_id)
                session.add(
                    Master(
                        telegram_id=telegram_id,
                        name=display_name,
                        username=username,
                        first_name=first_name,
                        last_name=last_name,
                    )
                )
                await session.commit()
            try:
                invalidate_masters_cache()
            except Exception:
                pass
            return True
        except Exception as e:
            logger.exception("MasterRepo.add_master failed for %s: %s", telegram_id, e)
            return False

    @staticmethod
    async def delete_master(master_id: int) -> bool:
        """Delete a Master and cascade unlink from MasterService."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master, MasterService
                from sqlalchemy import select, delete
                master = await session.get(Master, master_id)
                if not master:
                    return False
                try:
                    await session.execute(delete(MasterService).where(MasterService.master_telegram_id == int(master_id)))
                except Exception:
                    pass
                await session.delete(master)
                await session.commit()
            try:
                invalidate_masters_cache()
            except Exception:
                pass
            return True
        except Exception as e:
            logger.exception("MasterRepo.delete_master failed for %s: %s", master_id, e)
            return False

    @staticmethod
    async def link_service(master_telegram_id: int, service_id: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterService
                from sqlalchemy import select
                existing = await session.scalar(select(MasterService).where(
                    MasterService.master_telegram_id == master_telegram_id,
                    MasterService.service_id == service_id,
                ))
                if existing:
                    return False
                session.add(MasterService(master_telegram_id=master_telegram_id, service_id=service_id))
                await session.commit()
            return True
        except Exception as e:
            logger.exception("MasterRepo.link_service failed for %s/%s: %s", master_telegram_id, service_id, e)
            return False

    @staticmethod
    async def unlink_service(master_telegram_id: int, service_id: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterService
                from sqlalchemy import delete
                await session.execute(delete(MasterService).where(
                    MasterService.master_telegram_id == master_telegram_id,
                    MasterService.service_id == service_id,
                ))
                await session.commit()
            return True
        except Exception as e:
            logger.exception("MasterRepo.unlink_service failed for %s/%s: %s", master_telegram_id, service_id, e)
            return False

    @staticmethod
    async def get_services_for_master(master_telegram_id: int) -> list[tuple[str, str]]:
        """Return list of (service_id, name) for services linked to the given master."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Service, MasterService
                stmt = select(Service.id, Service.name).join(
                    MasterService, MasterService.service_id == Service.id
                ).where(MasterService.master_telegram_id == master_telegram_id).order_by(Service.name)
                res = await session.execute(stmt)
                rows = res.fetchall()
                return [(str(sid), name) for sid, name in rows]
        except Exception as e:
            logger.exception("MasterRepo.get_services_for_master failed for %s: %s", master_telegram_id, e)
            return []

    @staticmethod
    async def get_services_with_durations_for_master(master_telegram_id: int) -> list[tuple[str, str, int | None]]:
        """Return list of (service_id, name, effective_duration_minutes) for the master.

        The priority for effective duration is:
            1. MasterService.duration_minutes override if >0
            2. ServiceProfile.duration_minutes if >0
            3. slot duration fallback from SettingsRepo
        """
        slot_default = DEFAULT_SERVICE_FALLBACK_DURATION
        try:
            slot_candidate = await SettingsRepo.get_slot_duration()
            if slot_candidate is not None:
                try:
                    slot_default = int(slot_candidate)
                except Exception:
                    slot_default = DEFAULT_SERVICE_FALLBACK_DURATION
        except Exception:
            slot_default = DEFAULT_SERVICE_FALLBACK_DURATION

        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Service, MasterService, ServiceProfile

                stmt = (
                    select(
                        Service.id,
                        Service.name,
                        MasterService.duration_minutes,
                        ServiceProfile.duration_minutes,
                    )
                    .select_from(MasterService)
                    .join(Service, Service.id == MasterService.service_id)
                    .outerjoin(ServiceProfile, ServiceProfile.service_id == Service.id)
                    .where(MasterService.master_telegram_id == master_telegram_id)
                    .order_by(Service.name)
                )
                rows = (await session.execute(stmt)).all()
                out: list[tuple[str, str, int | None]] = []
                for sid, name, ms_dur, sp_dur in rows:
                    eff = None
                    try:
                        ms_val = int(ms_dur) if ms_dur is not None else None
                        sp_val = int(sp_dur) if sp_dur is not None else None
                        if ms_val and ms_val > 0:
                            eff = ms_val
                        elif sp_val and sp_val > 0:
                            eff = sp_val
                        else:
                            eff = slot_default
                    except Exception:
                        eff = slot_default
                    out.append((str(sid), str(name), eff))
                return out
        except Exception as e:
            logger.exception(
                "MasterRepo.get_services_with_durations_for_master failed for %s: %s",
                master_telegram_id,
                e,
            )
            return []

    @staticmethod
    async def set_master_service_duration(master_telegram_id: int, service_id: str, minutes: int) -> bool:
        """Upsert duration override for (master, service)."""
        try:
            if minutes <= 0:
                # Treat non-positive as remove override (persist NULL)
                minutes = None  # type: ignore[assignment]
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterService
                row = await session.scalar(
                    select(MasterService).where(
                        MasterService.master_telegram_id == master_telegram_id,
                        MasterService.service_id == service_id,
                    )
                )
                if not row:
                    # ensure link exists first
                    session.add(MasterService(master_telegram_id=master_telegram_id, service_id=service_id, duration_minutes=minutes))
                else:
                    try:
                        # row.duration_minutes is Optional[int]; safe to assign None
                        setattr(row, "duration_minutes", minutes)
                    except Exception:
                        pass
                await session.commit()
            return True
        except Exception as e:
            logger.exception("set_master_service_duration failed for %s/%s: %s", master_telegram_id, service_id, e)
            return False

    @staticmethod
    async def get_clients_for_master(master_telegram_id: int) -> list[tuple[int, str | None, str | None]]:
        """Return list of unique clients (user_id, name, username) who ever booked with this master."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import User, Booking

                stmt = (
                    select(User.id, User.name, User.username)
                    .join(Booking, Booking.user_id == User.id)
                    .where(Booking.master_id == master_telegram_id)
                    # Use DISTINCT ON (users.id) — Postgres requires the initial
                    # ORDER BY expressions to include the DISTINCT ON columns.
                    # Ensure we order first by users.id then by users.name so the
                    # DISTINCT ON semantics are valid across Postgres versions.
                    .distinct(User.id)
                    .order_by(User.id, User.name)
                )
                res = await session.execute(stmt)
                rows = res.all()
                return [(int(r[0]), r[1], r[2]) for r in rows]
        except Exception as e:
            logger.exception("MasterRepo.get_clients_for_master failed for %s: %s", master_telegram_id, e)
            return []

    @staticmethod
    async def get_masters_for_service(service_id: str) -> list[Any]:
        """Return list of Master models for a given service_id."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master, MasterService
                # Defensive two-step query: first fetch master IDs from junction
                # table, then load Master rows. This avoids subtle JOIN issues
                # when the DB has inconsistent rows or different typing.
                mid_rows = await session.execute(select(MasterService.master_telegram_id).where(MasterService.service_id == service_id))
                raw_mid_rows = mid_rows.all()
                mids = [int(r[0]) for r in raw_mid_rows]
                # Always emit debug about raw junction rows to aid diagnosis of
                # cases where the junction seems to contain values but the
                # in_(mids) query later doesn't match (type/coercion issues,
                # whitespace, different DB/schema instance, etc.).
                logger.debug(
                    "get_masters_for_service: service_id=%r -> raw_mid_rows=%r -> mids=%s",
                    service_id,
                    raw_mid_rows,
                    mids,
                )
                if not mids:
                    # Additional debug: list distinct service_ids present in master_services
                    try:
                        all_rows = await session.execute(select(MasterService.master_telegram_id, MasterService.service_id))
                        logger.debug("get_masters_for_service: master_services full sample=%r", all_rows.all())
                    except Exception:
                        logger.debug("get_masters_for_service: failed to fetch master_services sample for debugging")
                    return []
                res = await session.execute(select(Master).where(Master.telegram_id.in_(mids)))
                masters = list(res.scalars().all())
                logger.debug("get_masters_for_service: service_id=%s -> master_ids=%s, masters_found=%d", service_id, mids, len(masters))
                return masters
        except Exception as e:
            logger.exception("MasterRepo.get_masters_for_service failed for %s: %s", service_id, e)
            return []

    @staticmethod
    async def services_with_masters(wanted_ids: set[str]) -> set[str]:
        """Return subset of wanted_ids that have at least one Master linked."""
        try:
            if not wanted_ids:
                return set()
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterService
                stmt = select(MasterService.service_id).where(MasterService.service_id.in_(wanted_ids)).distinct()
                res = await session.execute(stmt)
                return {str(r[0]) for r in res.all()}
        except Exception as e:
            logger.exception("MasterRepo.services_with_masters failed: %s", e)
            return set()

    @staticmethod
    async def get_master(master_telegram_id: int):
        """Return Master model by telegram id or None."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select
                return await session.scalar(select(Master).where(Master.telegram_id == master_telegram_id))
        except Exception as e:
            logger.exception("MasterRepo.get_master failed for %s: %s", master_telegram_id, e)
            return None

    @staticmethod
    async def get_master_name(master_telegram_id: int) -> str | None:
        """Return master's display name by telegram id."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select
                res = await session.execute(select(Master.name).where(Master.telegram_id == int(master_telegram_id)))
                return res.scalar_one_or_none()
        except Exception:
            return None

    @staticmethod
    async def find_masters_for_services(service_ids: Sequence[str]) -> list[tuple[int, str | None]]:
        """Return masters who offer all given services: list of (telegram_id, name)."""
        try:
            if not service_ids:
                return []
            from bot.app.domain.models import Master, MasterService
            from sqlalchemy import func
            async with get_session() as session:
                stmt = (
                    select(Master.telegram_id, Master.name)
                    .join(MasterService, MasterService.master_telegram_id == Master.telegram_id)
                    .where(MasterService.service_id.in_(list(service_ids)))
                    .group_by(Master.telegram_id, Master.name)
                    .having(func.count(func.distinct(MasterService.service_id)) == len(service_ids))
                    .order_by(Master.name)
                )
                rows = list((await session.execute(stmt)).all())
                return [(int(r[0]), r[1]) for r in rows]
        except Exception as e:
            logger.exception("MasterRepo.find_masters_for_services failed: %s", e)
            return []



async def fetch_booking_safe(booking_id: int) -> Any | None:
    """Safe wrapper around booking fetch that swallows DB errors."""
    try:
        from bot.app.services.client_services import BookingRepo
        return await BookingRepo.get(booking_id)
    except Exception as e:
        logger.warning("fetch_booking_safe (repo): DB access failed %s", e)
        return None


async def enrich_booking_context(booking: Any) -> tuple[Any | None, str]:
    """Return (client_obj, service_name) for a booking.

    Uses get_user_by_id and get_service_name; falls back to raw ids on failure.
    """
    client = None
    service_name = getattr(booking, "service_id", "—")
    try:
        if getattr(booking, "user_id", None):
            from bot.app.services.client_services import UserRepo
            client = await UserRepo.get_by_id(booking.user_id)
    except Exception:
        client = None
    try:
        service_name = await ServiceRepo.get_service_name(getattr(booking, "service_id", ""))
    except Exception:
        service_name = str(getattr(booking, "service_id", "—"))
    return client, service_name


async def build_client_history_view(booking_id: int) -> str | None:
    """Return a formatted client history view for master UI or None.

    This function composes the view using existing history service.
    """
    try:
        booking = await fetch_booking_safe(booking_id)
        if not booking:
            return None
        master_id = getattr(booking, "master_id", None)
        if master_id is None:
            return None
        # Prefer a shared repository-based history fetch where available
        try:
            hist_map = await MasterRepo.get_client_history_for_master(booking_id)
            if not hist_map:
                return None
            return format_client_history(hist_map, booking.user_id)
        except Exception:
            return None
    except Exception as e:
        logger.warning("build_client_history_view failed: %s", e)
        return None


def format_client_history(hist: Mapping, user_id: int, lang: str | None = None) -> str:
    """Format client history mapping into a short text block for master UI.

    This formatter prefers a provided `lang` but will fall back to the
    project-wide default language if none is supplied. It uses translation
    keys (via tr/t) for all visible labels so the output is localized.
    """
    try:
        # Resolve language (caller may pass None)
        l = lang or default_language()

        header = tr("master_client_history_header", lang=l)
        # Keep id in header for clarity (translations generally don't include id)
        lines: list[str] = [f"{header} #{user_id}"]

        visits = hist.get("visits", 0)
        spent = hist.get("total_spent_cents", 0)

        # Prepare localized labels with safe fallbacks when translations are missing
        name_lbl = tr("client_label", lang=l) or "Name"
        visits_lbl = tr("master_total_visits", lang=l) or "Visits"
        spent_lbl = tr("master_total_spent", lang=l) or "Spent"
        last_visit_lbl = tr("master_last_visit", lang=l) or "Last visit"
        rating_lbl = tr("rating_label", lang=l) or "Rating"
        note_lbl = tr("master_note", lang=l) or "Note"

        basic_fields = [
            (name_lbl, hist.get("name")),
            (visits_lbl, visits),
            (spent_lbl, format_money_cents(spent)),
            (last_visit_lbl, hist.get("last_visit")),
            (rating_lbl, (f"{hist.get('rating')}⭐" if isinstance(hist.get("rating"), (int, float)) else None)),
            (note_lbl, hist.get("note")),
        ]

        for label, value in basic_fields:
            if value is None or value == "":
                continue
            lines.append(f"{label}: {value}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("format_client_history failed: %s", e)
        return ""


async def get_master_bookings_for_period(
    master_telegram_id: int,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    days: int | None = 7,
) -> Sequence[BookingInfo]:
    """Получает записи мастера за указанный период.

    This function supports two calling conventions for backward compatibility:
    - legacy: pass `days=int` (as before) — it will use now..now+days
    - preferred: pass explicit `start` (datetime) and optional `end` (datetime)

    If neither `start` nor `days` is provided, `days=7` is used.
    """
    try:
        # Prefer canonical BookingRepo.get_paginated_list which centralizes
        # all booking list logic and supports start/end filtering.
        from bot.app.services.client_services import BookingRepo

        # Legacy behaviour: if start not provided and days given, compute range
        if start is None and days:
            now = datetime.now(UTC)
            start = now
            end = now + timedelta(days=int(days))

        rows, _meta = await BookingRepo.get_paginated_list(master_id=master_telegram_id, start=start, end=end, page_size=None)
        return list(rows or [])
    except Exception as e:
        logger.exception("get_master_bookings_for_period (shared) failed for %s: %s", master_telegram_id, e)
        return []


async def check_future_booking_conflicts(
    master_telegram_id: int,
    *,
    day_to_clear: int | None = None,
    clear_all: bool = False,
    horizon_days: int = 365,
    return_ids: bool = False,
) -> list[str]:
    """Return a list of human-readable conflict strings for future bookings.

    - If day_to_clear is provided, only bookings that fall into that weekday's
      configured windows are considered.
    - If clear_all is True, all weekdays configured in the master's schedule are
      considered.
    - horizon_days bounds how far into the future we scan (default 365 days).

    Returns a list of formatted strings like '#<id> 2025-01-01 09:00 — <client>'.
    """
    try:
        now = datetime.now(UTC)
        end = now + timedelta(days=horizon_days)
        sched = _normalize_schedule(await get_master_schedule(master_telegram_id) or {})

        if clear_all:
            days_to_check = set(range(7))
        elif day_to_clear is not None:
            days_to_check = {int(day_to_clear)}
        else:
            return []

        windows: list[tuple[int, int, int]] = []
        for day in days_to_check:
            raw_windows = sched.get(str(day)) or []
            for w in raw_windows:
                if not isinstance(w, (list, tuple)) or len(w) < 2:
                    continue
                try:
                    a = str(w[0])
                    b = str(w[1])
                    a_min = _parse_hm_to_minutes(a)
                    b_min = _parse_hm_to_minutes(b)
                    if a_min >= b_min:
                        continue
                except Exception:
                    continue
                windows.append((day, a_min, b_min))

        if not windows:
            return []

        try:
            from bot.app.services.client_services import BookingRepo
        except Exception as exc:
            logger.exception("check_future_booking_conflicts: BookingRepo import failed: %s", exc)
            return []

        conflicts_source = await BookingRepo.get_conflicting_bookings_ids(
            master_id=master_telegram_id,
            windows=windows,
            start=now,
            end=end,
            return_ids_only=return_ids,
        )

        if return_ids:
            return [str(r) for r in cast(list[int], conflicts_source)]

        conflicts: list[str] = []
        for row in conflicts_source:
            try:
                bid = getattr(row, "booking_id", None)
                starts = getattr(row, "starts_at", None)
                status = getattr(row, "status", None)
                master_id = getattr(row, "master_id", None)
                user_id = getattr(row, "user_id", None)
                user_name = getattr(row, "user_name", None)
                username = getattr(row, "username", None)
                iso = starts.isoformat() if starts and hasattr(starts, "isoformat") else str(starts)
                display_user = user_name or f"id:{user_id}"
                if username:
                    display_user = f"{display_user} (@{username})"
                conflicts.append(
                    f"#{bid} {iso} (master={master_id}) status={getattr(status, 'value', status)} — {display_user}"
                )
            except Exception:
                continue
        return conflicts
    except Exception as e:
        logger.exception("check_future_booking_conflicts failed for master %s: %s", master_telegram_id, e)
        return []


async def cancel_bookings_and_notify(bot, booking_ids: list[int] | None, *, notify_admins: bool = True) -> int:
    """Cancel bookings by id and notify clients + admins. Returns number cancelled.

    This centralizes the logic used by master and admin handlers so history is
    preserved and notification logic is consistent.
    """
    if not booking_ids:
        return 0

    cancelled = 0
    try:
        from bot.app.services.client_services import BookingRepo
    except Exception as e:
        logger.exception("cancel_bookings_and_notify: BookingRepo unavailable: %s", e)
        return 0
    try:
        from bot.app.services.client_services import send_booking_notification
    except Exception:
        send_booking_notification = None

    for bid in booking_ids:
        try:
            bd = await MasterRepo.get_booking_display_data(int(bid))
            ok = await BookingRepo.set_cancelled(int(bid))
            if ok:
                cancelled += 1
                # notify client + admins
                try:
                    client_tid = None
                    if bd:
                        client_tid = bd.get("client_telegram_id") if isinstance(bd, dict) else None
                    recipients = []
                    if client_tid:
                        recipients.append(int(client_tid))
                    if notify_admins:
                        admins = get_admin_ids()
                        if admins:
                            recipients.extend(admins)
                    # de-duplicate and send
                    recipients = list(dict.fromkeys(recipients))
                    if bot and recipients and send_booking_notification:
                        await send_booking_notification(bot, int(bid), "cancelled", recipients)
                except Exception:
                    logger.exception("Failed to notify recipients for cancelled booking %s", bid)
        except Exception:
            logger.exception("cancel_bookings_and_notify: failed for %s", bid)
            continue

    return cancelled


async def cancel_booking(booking_id: int) -> bool:
    """Cancel a single booking by id via Repo. Thin facade for handlers.

    Returns True if status was updated, False otherwise. Notifications are
    not sent here; use cancel_bookings_and_notify if needed.
    """
    try:
        from bot.app.services.client_services import BookingRepo
        return await BookingRepo.set_cancelled(int(booking_id))
    except Exception as e:
        logger.exception("cancel_booking failed for %s: %s", booking_id, e)
        return False

async def ensure_booking_owner(user_id: int, booking_id: int) -> Optional[Booking]:
    """Проверяет, принадлежит ли запись пользователю (служебный метод).

    Возвращает объект Booking или None. Помещено в сервисный слой, чтобы
    инкапсулировать доступ к базе данных и избежать ленивых импортов в
    слое интерфейсов/хендлеров.
    """
    try:
        from bot.app.services.client_services import BookingRepo
        return await BookingRepo.ensure_owner(user_id, booking_id)
    except Exception as e:
        logger.exception("ensure_booking_owner (repo) failed for %s: %s", booking_id, e)
        return None




async def get_master_profile_data(master_id: int) -> Optional[Dict[str, Any]]:
    """Fetch master profile data only (no formatting).

    Returns a dict with keys: master, services, durations_map, about_text, reviews
    or None if master not found.
    """
    try:
        from bot.app.services.master_services import MasterRepo
        return await MasterRepo.get_master_profile_data(master_id)
    except Exception as e:
        logger.exception("get_master_profile_data (repo) failed: %s", e)
        return None
async def get_master_schedule(master_telegram_id: int) -> dict:
    """Возвращает расписание мастера, хранящееся в MasterProfile.bio как JSON.

    Формат: {"schedule": {"0": [["09:00","12:00"], ...], "1": [...], ...}}
    Возвращает пустой dict при отсутствии данных или на ошибке.
    """
    # Delegate to repository to centralize DB access
    try:
        from bot.app.services.master_services import MasterRepo
        return await MasterRepo.get_schedule(master_telegram_id)
    except Exception as e:
        logger.warning("get_master_schedule (repo) failed for %s: %s", master_telegram_id, e)
        return {}


def _parse_master_schedule_time(value: str | _time | None) -> _time | None:
    """Convert normalized HH:MM tokens into datetime.time for storage."""
    if isinstance(value, _time):
        return value
    if not value:
        return None
    token = str(value).strip()
    if not token:
        return None
    try:
        return _time.fromisoformat(token)
    except Exception:
        return None


async def set_master_schedule(master_telegram_id: int, schedule: dict) -> bool:
    """Persist master schedule into relational table (DB-only).

    Input format: keys are weekday numbers (0=Mon..6=Sun) as int/str, values are
    lists of [start,end] pairs (HH:MM). Legacy JSON storage removed.
    """
    try:
        canonical = _normalize_schedule(schedule or {})
        async with get_session() as session:
            from sqlalchemy import select, delete
            from bot.app.domain.models import MasterProfile, MasterSchedule
            # Ensure master_profile exists
            prof = await session.scalar(select(MasterProfile).where(MasterProfile.master_telegram_id == master_telegram_id))
            if not prof:
                prof = MasterProfile(master_telegram_id=master_telegram_id, bio=None)
                session.add(prof)
                await session.flush()
            # Delete existing rows
            await session.execute(delete(MasterSchedule).where(MasterSchedule.master_profile_id == prof.id))
            # Insert new windows
            now_ts = datetime.now(UTC)
            to_add = []
            for k, windows in canonical.items():
                try:
                    dow = int(k)
                except Exception:
                    continue
                for win in windows or []:
                    if not (isinstance(win, (list, tuple)) and len(win) >= 2):
                        continue
                    start_time = _parse_master_schedule_time(win[0])
                    end_time = _parse_master_schedule_time(win[1])
                    if not start_time or not end_time:
                        continue
                    to_add.append(MasterSchedule(master_profile_id=prof.id, day_of_week=dow, start_time=start_time, end_time=end_time, updated_at=now_ts))
            for obj in to_add:
                session.add(obj)
            await session.commit()
        logger.info("set_master_schedule: stored %d windows for master %s", len(to_add), master_telegram_id)
        return True
    except Exception as e:
        logger.exception("set_master_schedule failed for %s: %s", master_telegram_id, e)
        return False


async def set_master_schedule_day(master_telegram_id: int, day: int, windows: list[list[str]]) -> bool:
    """Set windows for a single weekday (day: 0..6). Pass windows=[] to clear the day.
    windows should be list of [start, end] pairs as strings.
    """
    try:
        sched = await get_master_schedule(master_telegram_id) or {}
        sched[str(day)] = windows
        return await set_master_schedule(master_telegram_id, sched)
    except Exception as e:
        logger.exception("Failed to set schedule day for master %s day=%s: %s", master_telegram_id, day, e)
        return False

async def remove_schedule_window_by_value(master_telegram_id: int, day: int, start: str, end: str) -> tuple[bool, list[str]]:
    """Remove a window identified by its start/end value instead of positional index.

    This is safer under concurrent modifications where indices can shift
    between the time the keyboard was rendered and the time the callback
    arrives. We first locate the window by value (exact HH:MM match after
    normalization). If the window no longer exists we return (False, []).

    Returns (True, []) on successful removal with no booking conflicts.
    Returns (False, conflicts) if future bookings fall inside the window.
    Returns (False, []) if the window was not found or an error occurred.
    """
    try:
        mid = int(master_telegram_id)
        d = int(day)
    except Exception:
        return False, []
    # Normalize input times to HH:MM (window storage format)
    def _norm(x: str) -> str | None:
        try:
            x = x.strip()
            h, m = map(int, x.split(":"))
            if 0 <= h < 24 and 0 <= m < 60:
                return f"{h:02d}:{m:02d}"
        except Exception:
            return None
        return None
    a = _norm(str(start))
    b = _norm(str(end))
    if not a or not b:
        return False, []
    try:
        sched = await get_master_schedule(mid)
        day_slots = sched.get(str(d)) if isinstance(sched, dict) else []
        if not isinstance(day_slots, list) or not day_slots:
            return False, []
        # locate index by value match
        idx: int | None = None
        for i, w in enumerate(day_slots):
            try:
                if isinstance(w, (list, tuple)) and len(w) >= 2:
                    if str(w[0]) == a and str(w[1]) == b:
                        idx = i
                        break
                else:
                    # legacy 'HH:MM-HH:MM' string form
                    s = str(w)
                    if "-" in s:
                        aa, bb = s.split("-", 1)
                        if aa.strip() == a and bb.strip() == b:
                            idx = i
                            break
            except Exception:
                continue
        if idx is None:
            # window disappeared or modified — treat as benign no-op
            return False, []
        # Reuse conflict detection logic (specialize for single window)
        conflicts: list[str] = []
        now = datetime.now(timezone.utc)
        try:
            bookings = await MasterRepo.get_bookings_for_period(mid, start=now, days=365)
        except SQLAlchemyError:
            bookings = []
        try:
            a_h, a_m = map(int, a.split(":"))
            b_h, b_m = map(int, b.split(":"))
            a_min = a_h * 60 + a_m
            b_min = b_h * 60 + b_m
        except Exception:
            return False, []
        for booking in (bookings or []):
            try:
                starts = getattr(booking, "starts_at", None)
                if not starts or starts.weekday() != d:
                    continue
                start_min = starts.hour * 60 + starts.minute
                if start_min >= a_min and start_min < b_min:
                    try:
                        client, _ = await enrich_booking_context(booking)
                        user_name = getattr(client, "name", None) or f"id:{getattr(client, 'id', '?')}"
                    except SQLAlchemyError:
                        user_name = f"id:{getattr(booking, 'user_id', '?')}"
                    except Exception:
                        user_name = f"id:{getattr(booking, 'user_id', '?')}"
                    try:
                        iso = starts.isoformat()
                    except AttributeError:
                        iso = starts.strftime('%Y-%m-%d %H:%M') if hasattr(starts, 'strftime') else str(starts)
                    conflicts.append(f"#{getattr(booking, 'id', '?')} {iso} — {user_name}")
            except (AttributeError, ValueError, TypeError, SQLAlchemyError):
                continue
        if conflicts:
            return False, conflicts
        # Perform removal and persist
        try:
            day_slots.pop(idx)
        except Exception:
            return False, []
        await set_master_schedule(mid, sched)
        return True, []
    except SQLAlchemyError as e:
        logger.exception("remove_schedule_window_by_value failed for %s: %s", master_telegram_id, e)
        return False, []
    except Exception as e:
        logger.exception("remove_schedule_window_by_value unexpected error for %s: %s", master_telegram_id, e)
        return False, []

def _normalize_schedule(schedule: dict | None) -> dict:
    """Normalize various schedule shapes into canonical mapping of str(weekday)->list[[HH:MM,HH:MM],...].

    This consolidates the duplicated inner functions used elsewhere.
    """
    out: dict = {}
    if not schedule:
        return out
    for k, v in (schedule or {}).items():
        key = str(k)
        vals = v or []
        normalized_windows: list[list[str]] = []
        if not isinstance(vals, (list, tuple)):
            out[key] = []
            continue
        # Strict time range regex: hours 00-23, minutes 00-59
        _strict_re = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")
        for item in vals:
            try:
                # pair-like item
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    a = str(item[0]).strip()
                    b = str(item[1]).strip()
                    if _strict_re.match(a) and _strict_re.match(b):
                        a_h, a_m = list(map(int, a.split(":")))
                        b_h, b_m = list(map(int, b.split(":")))
                        a_norm = f"{a_h:02d}:{a_m:02d}"
                        b_norm = f"{b_h:02d}:{b_m:02d}"
                        if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                            normalized_windows.append([a_norm, b_norm])
                    else:
                        # try recover from single-string inside tuple/list
                        s = str(item[0])
                        if "-" in s:
                            parts = s.split("-", 1)
                            if (
                                len(parts) == 2
                                and _strict_re.match(parts[0].strip())
                                and _strict_re.match(parts[1].strip())
                            ):
                                a = parts[0].strip()
                                b = parts[1].strip()
                                a_h, a_m = list(map(int, a.split(":")))
                                b_h, b_m = list(map(int, b.split(":")))
                                a_norm = f"{a_h:02d}:{a_m:02d}"
                                b_norm = f"{b_h:02d}:{b_m:02d}"
                                if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                                    normalized_windows.append([a_norm, b_norm])
                else:
                    # legacy string like '09:00-12:00'
                    s = str(item)
                    if "-" in s:
                        a, b = s.split("-", 1)
                        a = a.strip()
                        b = b.strip()
                        if _strict_re.match(a) and _strict_re.match(b):
                            a_h, a_m = list(map(int, a.split(":")))
                            b_h, b_m = list(map(int, b.split(":")))
                            a_norm = f"{a_h:02d}:{a_m:02d}"
                            b_norm = f"{b_h:02d}:{b_m:02d}"
                            if _parse_hm_to_minutes(a_norm) < _parse_hm_to_minutes(b_norm):
                                normalized_windows.append([a_norm, b_norm])
            except Exception:
                continue
        out[key] = normalized_windows
    return out


# ---------------------------------------------------------------------------
# Facade functions (thin delegators) for handlers
# Handlers should import these functions instead of touching Repo classes.
# ---------------------------------------------------------------------------
async def get_master_name(master_id: int) -> str | None:
    try:
        return await MasterRepo.get_master_name(master_id)
    except Exception as e:
        logger.exception("get_master_name failed for %s: %s", master_id, e)
        return None


async def get_master(master_id: int):
    try:
        return await MasterRepo.get_master(master_id)
    except Exception as e:
        logger.exception("get_master failed for %s: %s", master_id, e)
        return None


# Note: compatibility shims were removed — use the canonical MasterRepo
# methods implemented above in this module (`MasterRepo.get_master_name`,
# `MasterRepo.get_services_for_master`, etc.).


async def find_masters_for_services(service_ids: Sequence[str]) -> list[tuple[int, str | None]]:
    try:
        return await MasterRepo.find_masters_for_services(service_ids)
    except Exception as e:
        logger.exception("find_masters_for_services failed: %s", e)
        return []

async def get_master_stats_summary(master_telegram_id: int, *, days: int = 7) -> dict[str, Any]:
    """Return a small stats summary for a master for the given period.

    Returns a dict with keys: total_bookings, completed_bookings, no_shows, next_booking_time
    """
    try:
        local_tz = get_local_tz() or UTC
        from sqlalchemy import select, func, case
        from bot.app.domain.models import Booking, BookingStatus
        now = datetime.now(UTC)
        start = now - timedelta(days=days)
        end = now
        async with get_session() as session:
            # Single aggregate query for counts and revenue
            agg_stmt = (
                select(
                    func.count(Booking.id).label("total"),
                    func.sum(case((Booking.status == BookingStatus.DONE, 1), else_=0)).label("completed"),
                    func.sum(case((Booking.status == BookingStatus.NO_SHOW, 1), else_=0)).label("noshow"),
                    func.coalesce(func.sum(func.coalesce(Booking.final_price_cents, Booking.original_price_cents)), 0).label("revenue_cents"),
                )
                .where(
                    Booking.master_id == int(master_telegram_id),
                    Booking.starts_at.between(start, end),
                )
            )
            res = await session.execute(agg_stmt)
            row = res.first()
            if row:
                total = int(row[0] or 0)
                completed = int(row[1] or 0)
                noshow = int(row[2] or 0)
                revenue_cents = int(row[3] or 0)
            else:
                total = 0
                completed = 0
                noshow = 0
                revenue_cents = 0

            # average bookings per day over the period
            avg_per_day = (total / max(1, days)) if isinstance(total, int) else 0.0

            # no-show rate percentage
            no_show_rate = (noshow / total * 100.0) if total > 0 else 0.0

            # find next upcoming booking (after now)
            next_stmt = select(Booking.starts_at).where(Booking.master_id == int(master_telegram_id), Booking.starts_at >= now).order_by(Booking.starts_at.asc()).limit(1)
            nb = await session.execute(next_stmt)
            next_row = nb.first()
            next_time = None
            if next_row and next_row[0]:
                try:
                    next_time = next_row[0].astimezone(local_tz).strftime('%d.%m %H:%M')
                except Exception:
                    next_time = str(next_row[0])
            return {
                "total_bookings": total,
                "completed_bookings": completed,
                "no_shows": noshow,
                "next_booking_time": next_time,
                "revenue_cents": revenue_cents,
                "avg_per_day": avg_per_day,
                "no_show_rate": no_show_rate,
            }
    except Exception as e:
        logger.exception("get_master_stats_summary failed for %s: %s", master_telegram_id, e)
        return {"total_bookings": 0, "completed_bookings": 0, "no_shows": 0, "next_booking_time": None}


async def get_masters_for_service(service_id: str) -> list[Any]:
    try:
        return await MasterRepo.get_masters_for_service(service_id)
    except Exception as e:
        logger.exception("get_masters_for_service failed for %s: %s", service_id, e)
        return []


async def services_with_masters(wanted_ids: set[str]) -> set[str]:
    try:
        return await MasterRepo.services_with_masters(wanted_ids)
    except Exception as e:
        logger.debug("MasterRepo.services_with_masters failed, falling back to module query: %s", e)
    try:
        if not wanted_ids:
            return set()
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import MasterService
            stmt = select(MasterService.service_id).where(MasterService.service_id.in_(wanted_ids)).distinct()
            res = await session.execute(stmt)
            return {str(r[0]) for r in res.all()}
    except Exception as e:
        logger.exception("module.services_with_masters fallback failed: %s", e)
        return set()



def _parse_windows_from_bio(bio: dict | None, target_date: _date | datetime) -> list[tuple[_time, _time]]:
    """Parse MasterProfile.bio dict and return list of (time, time) windows for target_date.

    This encapsulates exceptions, weekly schedule and settings fallbacks. It accepts
    a pre-parsed bio dictionary (as returned by get_master_bio) so callers that already
    loaded bio can reuse it without causing extra DB queries.
    """
    try:
        if bio is None:
            return [( _time(hour=9), _time(hour=18) )]
        # Normalize target_date to a date object
        if isinstance(target_date, datetime):
            td = target_date.date()
        else:
            td = target_date

        # Exceptions override specific dates
        exceptions = bio.get("exceptions") or {}
        day_key = td.isoformat()
        windows: list[tuple[_time, _time]] = []
        if exceptions and day_key in exceptions:
            exc = exceptions.get(day_key) or {}
            if exc.get("off"):
                return []
            date_windows = exc.get("windows") or []
            for rng in date_windows:
                try:
                    if isinstance(rng, (list, tuple)) and len(rng) >= 2:
                        a = str(rng[0])
                        b = str(rng[1])
                    else:
                        s = str(rng)
                        if "-" in s:
                            a, b = s.split("-", 1)
                        else:
                            continue
                    a = a.strip()
                    b = b.strip()
                    # validate HH:MM
                    if re.match(r"^\d{1,2}:\d{2}$", a) and re.match(r"^\d{1,2}:\d{2}$", b):
                        a_h, a_m = list(map(int, a.split(":")))
                        b_h, b_m = list(map(int, b.split(":")))
                        windows.append((_time(hour=a_h, minute=a_m), _time(hour=b_h, minute=b_m)))
                except Exception:
                    continue
            return windows

        # No date-specific exception -> check canonical weekly schedule
        schedule = bio.get("schedule") or {}
        # Ensure schedule is normalized shape (strings 'HH:MM') using existing normalizer
        try:
            schedule_norm = _normalize_schedule(schedule)
        except Exception:
            schedule_norm = {}

        wd = td.weekday()
        day_windows = schedule_norm.get(str(wd)) or []
        for rng in day_windows:
            try:
                a = str(rng[0]).strip()
                b = str(rng[1]).strip()
                a_h, a_m = list(map(int, a.split(":")))
                b_h, b_m = list(map(int, b.split(":")))
                windows.append((_time(hour=a_h, minute=a_m), _time(hour=b_h, minute=b_m)))
            except Exception:
                continue

        # If still empty, check settings for defaults
        if not windows:
            settings = bio.get("settings") or {}
            from typing import Any
            start_h: Any = settings.get("start_hour")
            end_h: Any = settings.get("end_hour")
            try:
                if start_h and end_h:
                    sh_parts = [int(x) for x in str(start_h).split(":")[:2]]
                    eh_parts = [int(x) for x in str(end_h).split(":")[:2]]
                    windows = [(_time(hour=sh_parts[0], minute=sh_parts[1]), _time(hour=eh_parts[0], minute=eh_parts[1]))]
            except Exception:
                windows = []

        if not windows:
            windows = [(_time(hour=9), _time(hour=18))]
        return windows
    except Exception:
        return [(_time(hour=9), _time(hour=18))]


async def get_work_windows_for_day(master_telegram_id: int, target_date: _date | datetime) -> list[tuple[_time, _time]]:
    """Async helper: fetch MasterProfile.bio and return work windows for target_date.

    This is the externally visible helper recommended for use by clients needing
    the canonical work windows for a master on a given date.
    """
    try:
        # Normalize target_date to a date object
        if isinstance(target_date, datetime):
            td = target_date.date()
        else:
            td = target_date

        # First, attempt to honor explicit date exceptions stored in bio
        try:
            bio = await MasterRepo.get_master_bio(master_telegram_id)
        except Exception:
            bio = {}
        day_key = td.isoformat()
        exceptions = (bio or {}).get("exceptions") or {}
        if exceptions and day_key in exceptions:
            # Delegate to existing parser which handles date-specific exceptions
            return _parse_windows_from_bio(bio, target_date)

        # Next, prefer relational master_schedules rows when present.
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterProfile, MasterSchedule

                prof_id = await session.scalar(select(MasterProfile.id).where(MasterProfile.master_telegram_id == master_telegram_id))
                if prof_id is not None:
                    stmt = (
                        select(MasterSchedule.start_time, MasterSchedule.end_time)
                        .where(MasterSchedule.master_profile_id == prof_id, MasterSchedule.day_of_week == int(td.weekday()))
                        .order_by(MasterSchedule.start_time)
                    )
                    res = await session.execute(stmt)
                    rows = res.all()
                    if rows:
                        windows: list[tuple[_time, _time]] = []
                        for st, et in rows:
                            try:
                                s = st.strftime('%H:%M') if hasattr(st, 'strftime') else str(st)
                                e = et.strftime('%H:%M') if hasattr(et, 'strftime') else str(et)
                            except Exception:
                                s = str(st)
                                e = str(et)
                            try:
                                a_h, a_m = list(map(int, str(s).split(":")[:2]))
                                b_h, b_m = list(map(int, str(e).split(":")[:2]))
                                windows.append((_time(hour=a_h, minute=a_m), _time(hour=b_h, minute=b_m)))
                            except Exception:
                                continue
                        if windows:
                            return windows
        except Exception:
            # If anything fails querying relational schedules, fall back to bio parser
            pass

        # Final fallback: legacy bio parsing (may return defaults)
        return _parse_windows_from_bio(bio, target_date)
    except Exception:
        return [(_time(hour=9), _time(hour=18))]


def insert_window(schedule: dict | None, day: int, start: str, end: str, adjacency_min: int = 0) -> dict:
    """Insert a time window into schedule[day], merging overlaps and normalizing.

    - schedule: dict as returned by get_master_schedule (may be None).
    - day: weekday number 0..6
    - start/end: strings 'HH:MM'
    - adjacency_min: merge windows that are within this many minutes

    Returns normalized schedule dict (mutated copy).
    """
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    key = str(day)
    existing = out.get(key) or []
    # Normalize existing windows into minute tuples
    tuples: list[tuple[int, int]] = []
    for w in existing:
        try:
            s = _parse_hm_to_minutes(w[0])
            e = _parse_hm_to_minutes(w[1])
            if e > s:
                tuples.append((s, e))
        except Exception:
            continue

    # Validate input strings before parsing. If caller passed malformed
    # strings, refuse to modify schedule and log a warning. This prevents a
    # pattern where invalid input becomes 00:00 due to parsing fallback.
    # Strict validation: hours 00-23, minutes 00-59
    time_re = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")
    if not time_re.match(str(start)) or not time_re.match(str(end)):
        logger.warning("insert_window: invalid time strings start=%r end=%r", start, end)
        return out

    s_new = _parse_hm_to_minutes(start)
    e_new = _parse_hm_to_minutes(end)
    if e_new <= s_new:
        # invalid window; ignore and return original schedule
        return out

    tuples.append((s_new, e_new))
    # sort and merge
    tuples.sort(key=lambda x: x[0])
    merged: list[tuple[int, int]] = []
    for s, e in tuples:
        if not merged:
            merged.append((s, e))
            continue
        last_s, last_e = merged[-1]
        if s <= last_e + adjacency_min:
            # overlap or adjacent -> merge
            merged[-1] = (last_s, max(last_e, e))
        else:
            merged.append((s, e))

    # format back
    out[key] = [[_minutes_to_hm(s), _minutes_to_hm(e)] for s, e in merged]
    return out


def remove_all_windows(schedule: dict | None, day: int) -> dict:
    """Mark given day as empty list (workday cleared)."""
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    out[str(day)] = []
    return out


def copy_day(schedule: dict | None, target_day: int, source_day: int, mode: str = "replace") -> dict:
    """Copy windows from source_day to target_day.

    mode: 'replace' (default) or 'append' (append and normalize)
    """
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    src = out.get(str(source_day)) or []
    if mode == "replace":
        out[str(target_day)] = [list(w) for w in src]
        return out

    # append mode: insert each window and normalize
    result = out.get(str(target_day)) or []
    tmp = {**out}
    tmp[str(target_day)] = [list(w) for w in result]
    for w in src:
        if not (isinstance(w, (list, tuple)) and len(w) >= 2):
            continue
        tmp = insert_window(tmp, target_day, str(w[0]), str(w[1]))
    return tmp


def render_schedule_table(schedule: dict | None) -> str:
    """Render schedule dict into human-readable multi-line table for Mon..Sun."""
    sched = schedule or {}
    days = tr("weekday_short")
    lines: list[str] = []
    for idx, name in enumerate(days):
        w = sched.get(str(idx)) or sched.get(idx) or []
        if not w:
            lines.append(f"{name}: выходной")
            continue
        parts = []
        for rng in (w or []):
            try:
                parts.append(f"{str(rng[0])}-{str(rng[1])}")
            except Exception:
                continue
        lines.append(f"{name}: {', '.join(parts) if parts else 'выходной'}")
    return "\n".join(lines)


# Note: public facades are defined above as thin delegators to MasterRepo.