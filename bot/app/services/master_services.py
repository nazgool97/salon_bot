from __future__ import annotations

import logging
from contextlib import suppress
from datetime import UTC, datetime, date as _date, time as _time, timedelta
from typing import Any, cast
from collections.abc import Mapping, Sequence
import re
import sqlalchemy as sa

from sqlalchemy import select, and_, func, case
from sqlalchemy.exc import SQLAlchemyError

from bot.app.core.constants import (
    DEFAULT_PAGE_SIZE,
    DEFAULT_DAY_END_HOUR,
    DEFAULT_DAY_START_HOUR,
    DEFAULT_SERVICE_FALLBACK_DURATION,
    DEFAULT_TIME_STEP_MINUTES,
)

from bot.app.core.db import get_session
from bot.app.domain.models import (
    Booking,
    BookingStatus,
    TERMINAL_STATUSES,
)
from bot.app.services.admin_services import ServiceRepo, SettingsRepo
from bot.app.services.shared_services import (
    format_money_cents,
    _parse_hm_to_minutes,
    _minutes_to_hm,
    default_language,
    get_admin_ids,
    format_date,
    format_slot_label,
    format_booking_list_item,
    format_booking_details_text,
    get_local_tz,
    format_user_display_name,
    utc_now,
)
from bot.app.translations import tr, t
from aiogram.types import InlineKeyboardMarkup

logger = logging.getLogger(__name__)


# --- Master formatters ------------------------------------------------------
def format_master_booking_row(fields: dict[str, str]) -> str:
    """Format booking row for master-facing compact lists."""
    status_label = str(fields.get("status_label") or "")
    st = str(fields.get("st") or "")
    dt = str(fields.get("dt") or "")
    client_name = str(fields.get("client_name") or "")
    service_name = str(fields.get("service_name") or "")
    price_txt = str(fields.get("price_txt") or "")
    # Keep status label at front, then use bullets between datetime, client, and service+price
    datetime_part = f"{dt} {st}".strip()
    service_part = f"{service_name[:24]} {price_txt}".strip()
    parts = [datetime_part, client_name[:20].strip(), service_part]
    parts = [p for p in parts if p]
    body = " • ".join(parts)
    return (f"{status_label} " + body).strip()


def compute_time_end_items(
    day: int,
    start_time: str,
    *,
    end_hour: int = DEFAULT_DAY_END_HOUR,
    step_min: int = DEFAULT_TIME_STEP_MINUTES,
) -> list[tuple[str, str]]:
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


def format_master_profile_text(
    data: Mapping[str, Any] | None, lang: str, *, with_title: bool = True
) -> str:
    """Pure formatter: build profile text from pre-fetched `data`.

    This was previously defined in the master UI module. Move it here so
    formatting logic lives in the service layer and keyboards receive ready
    text or DTOs.
    """
    try:
        if not data:
            return str(tr("master_not_found", lang=lang))

        master = data.get("master")
        services = data.get("services") or []
        durations_map = dict(data.get("durations_map") or {})
        about_text = data.get("about_text")

        lines: list[str] = []
        if with_title:
            lines.append(tr("profile_title", lang=lang))

        uname = getattr(master, "username", None)
        master_name = getattr(master, "name", "")
        master_tid = getattr(master, "telegram_id", "")
        contact_block = [
            f"👤 {master_name} (@{uname})" if uname else f"👤 {master_name}",
            f"🆔 {master_tid}",
        ]
        phone = getattr(master, "phone", None)
        email = getattr(master, "email", None)
        if phone:
            contact_block.append(f"📞 {phone}")
        if email:
            contact_block.append(f"✉️ {email}")
        lines.extend(contact_block)

        if services:
            lines.append("")
            lines.append(tr("services_list_title", lang=lang))
            svc_lines = []
            for sid, sname, category, price_cents, currency in services:
                dur = durations_map.get(str(sid))
                dur_txt = (
                    f"{dur} {tr('minutes_short', lang=lang)}"
                    if isinstance(dur, int) and dur > 0
                    else None
                )
                price_txt = format_money_cents(price_cents or 0, currency)
                tail = " ".join(
                    filter(
                        None,
                        [
                            f"({dur_txt})" if dur_txt else None,
                            f"— {price_txt}" if price_txt else None,
                        ],
                    )
                )
                head = f"• {sname}" if not category else f"• {category} → {sname}"
                svc_lines.append(f"{head} {tail}".strip())
            lines.extend(svc_lines or [])
        else:
            lines.extend(["", "❌ " + str(tr("no_services_for_master", lang=lang))])

        rating_val = getattr(master, "rating", None)
        if rating_val is not None:
            lines.append("")
            rating_label = tr("rating_label", lang=lang)
            orders_word = tr("orders", lang=lang)
            completed = int(getattr(master, "completed_orders", 0) or 0)
            # Show simplified rating: average /5 and completed orders count
            # Example: "⭐ Рейтинг: 4.8/5 (120 замовлень)"
            lines.append(
                f"⭐ {rating_label}: {float(rating_val or 0.0):.1f}/5 ({completed} {orders_word})"
            )

        if about_text:
            lines.extend(["", str(tr("about_title", lang=lang)), str(about_text)])

        sched = data.get("schedule") or {}
        if isinstance(sched, dict):
            with suppress(Exception):
                lines.append("")
                lines.append(f"{tr('schedule_title', lang=lang)}:")
                wd_full = tr("weekday_full", lang=lang)

                def fmt_windows(windows: list[Any]) -> str:
                    formatted = []
                    for w in windows:
                        try:
                            if isinstance(w, (list, tuple)) and len(w) >= 2:
                                formatted.append(f"{w[0]}–{w[1]}")
                            else:
                                s = str(w)
                                if "-" in s:
                                    a, b = s.split("-", 1)
                                    formatted.append(f"{a.strip()}–{b.strip()}")
                        except Exception:
                            continue
                    return ", ".join(formatted) if formatted else "—"

                for i in range(7):
                    windows = sched.get(str(i)) or sched.get(i) or []
                    lines.append(f"• {wd_full[i]}: {fmt_windows(windows)}")
        return "\n".join(lines)
    except Exception:
        return str(tr("error", lang=lang))


# ---------------- Masters cache (moved here from shared_services) ----------------
_masters_cache_store: dict[int, str] | None = None
_resolve_master_cache: dict[int, int] = {}

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
                ).where(Master.is_active)
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


async def get_master_dashboard_summary(master_id: int, *, lang: str | None = None) -> str:
    """Build a small "today" dashboard summary string for a master.

    Returns a localized text block ready to be prepended to the master menu.
    """
    try:
        lang_value = lang or await SettingsRepo.get_setting("language", default_language())

        # Normalize provided identifier (surrogate id or telegram id) to canonical masters.id
        try:
            resolved_mid = await MasterRepo.resolve_master_id(int(master_id))
        except Exception:
            resolved_mid = None
        mid = resolved_mid or int(master_id)

        # Pre-compute rating summary for header (always show, default 0.0)
        rating_line = ""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import BookingRating, Booking

                res = await session.execute(
                    select(func.avg(BookingRating.rating), func.count(BookingRating.rating))
                    .select_from(BookingRating)
                    .join(Booking, Booking.id == BookingRating.booking_id)
                    .where(Booking.master_id == mid)
                )
                rating_avg, ratings_count = res.one_or_none() or (0.0, 0)

                completed_orders = int(
                    (
                        await session.execute(
                            select(func.count())
                            .select_from(Booking)
                            .where(Booking.master_id == mid, Booking.status == BookingStatus.DONE)
                        )
                    ).scalar()
                    or 0
                )

                rating_label = tr("rating_label", lang=lang_value)
                orders_word = tr("orders", lang=lang_value)
                rating_line = f"⭐ {rating_label}: {float(rating_avg or 0.0):.1f}/5 ({completed_orders} {orders_word})"
        except Exception:
            try:
                rating_label = tr("rating_label", lang=lang_value)
                orders_word = tr("orders", lang=lang_value)
                rating_line = f"⭐ {rating_label}: 0.0/5 (0 {orders_word})"
            except Exception:
                rating_line = ""

        # compute local day bounds and convert to UTC
        local_tz = get_local_tz() or UTC
        try:
            now_utc = utc_now()
            local_now = now_utc.astimezone(local_tz)
            local_day_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
            local_day_end = local_day_start + timedelta(days=1)
            day_start_utc = local_day_start.astimezone(UTC)
            day_end_utc = local_day_end.astimezone(UTC)
        except Exception:
            now_utc = utc_now()
            day_start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end_utc = day_start_utc + timedelta(days=1)
            local_day_start = day_start_utc.astimezone(local_tz)

        # Optimized: fetch top N upcoming bookings for today (for list display)
        try:
            from bot.app.services.client_services import BookingRepo
        except Exception:
            BookingRepo = None  # type: ignore

        if resolved_mid and BookingRepo:
            rows, _meta = await BookingRepo.get_paginated_list(
                master_id=resolved_mid,
                start=day_start_utc,
                end=day_end_utc,
                mode="upcoming",
                page=1,
                page_size=DEFAULT_PAGE_SIZE,
            )
        else:
            rows = []
        # Format rows inline using shared formatter (role-aware).
        formatted_rows: list[tuple[str, int]] = []
        for r in rows:
            try:
                txt, bid = format_booking_list_item(r, role="master", lang=lang_value)
                formatted_rows.append((txt, bid))
            except Exception:
                continue

        # Separate aggregate query for full-day counts to avoid incorrect
        # totals caused by limiting page_size above (previous bug: totals
        # reflected only first 5 bookings).
        try:
            async with get_session() as session:
                counts_stmt = select(
                    func.count(Booking.id).label("total"),
                    func.sum(case((Booking.status == BookingStatus.DONE, 1), else_=0)).label(
                        "done"
                    ),
                    func.sum(case((Booking.status == BookingStatus.CANCELLED, 1), else_=0)).label(
                        "cancelled"
                    ),
                ).where(
                    Booking.master_id == int(mid),
                    Booking.starts_at >= day_start_utc,
                    Booking.starts_at < day_end_utc,
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

            date_label = (
                local_day_start.strftime("%d.%m.%Y") if "local_day_start" in locals() else ""
            )
            header_raw = (
                tr("master_schedule_today_header", lang=lang_value).format(date=date_label)
                if date_label
                else tr("master_schedule_today_header", lang=lang_value)
            )
            header = header_raw

            # Build vertical stats (one stat per line), using translations when available
            today_lbl = (
                tr("dashboard_today_label", lang=lang_value).replace("{count}", "").strip()
                or "Today"
            )
            done_lbl = (
                tr("dashboard_done_label", lang=lang_value).replace("{count}", "").strip() or "Done"
            )
            cancelled_lbl = (
                tr("dashboard_cancelled_label", lang=lang_value).replace("{count}", "").strip()
                or "Cancelled"
            )
            pending_lbl = (
                tr("dashboard_pending_label", lang=lang_value).replace("{count}", "").strip()
                or "Pending"
            )

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
            base = [tr("master_no_bookings_today", lang=lang_value)]
            summary = "\n".join(base)

        # also fetch a 7-day stats summary and append to dashboard
        try:
            stats = await get_master_stats_summary(int(mid), days=7)
        except Exception:
            stats = {
                "total_bookings": 0,
                "completed_bookings": 0,
                "no_shows": 0,
                "next_booking_time": None,
            }
        try:
            # Build 7-day stats as vertical lines without emojis
            total7_lbl = (
                t("master_stats_7d_total", lang_value).split(":")[0]
                if t("master_stats_7d_total", lang_value)
                else "Total"
            )
            done7_lbl = (
                t("master_stats_7d_done", lang_value).split(":")[0]
                if t("master_stats_7d_done", lang_value)
                else "Done"
            )
            noshow7_lbl = (
                t("master_stats_7d_noshow", lang_value).split(":")[0]
                if t("master_stats_7d_noshow", lang_value)
                else "No-shows"
            )
            seven_lines = ["", (t("last_7_days", lang_value) or "Last 7 days:")]
            seven_lines.append(f"{total7_lbl}: {stats.get('total_bookings', 0)}")
            seven_lines.append(f"{done7_lbl}: {stats.get('completed_bookings', 0)}")
            seven_lines.append(f"{noshow7_lbl}: {stats.get('no_shows', 0)}")
            # Revenue (format cents to human-friendly string)
            try:
                rev_cents = int(stats.get("revenue_cents", 0) or 0)
                from bot.app.services.shared_services import format_money_cents, normalize_currency

                # Use the service-level currency when available, otherwise fall back
                # to the global SettingsRepo currency (normalized ISO code).
                global_cur = await SettingsRepo.get_currency()
                from bot.app.services.shared_services import _default_currency

                cur_code = normalize_currency(global_cur) or _default_currency()
                rev_txt = format_money_cents(rev_cents, cur_code)
            except Exception:
                rev_txt = str(int(stats.get("revenue_cents", 0) or 0) / 100.0)
            # Revenue (localized label)
            try:
                rev_lbl = t("revenue_title", lang_value)
                # keep only label part if translation contains a colon
                rev_lbl = rev_lbl.split(":")[0] if rev_lbl else "Revenue"
            except Exception:
                rev_lbl = "Revenue"
            seven_lines.append(f"{rev_lbl}: {rev_txt}")
            # Avg per day (localized)
            with suppress(Exception):
                avgd = float(stats.get("avg_per_day", 0.0) or 0.0)
                avg_lbl = t("avg_per_day", lang_value) or "Avg/day"
                seven_lines.append(f"{avg_lbl}: {avgd:.1f}")
            # No-show rate (localized)
            with suppress(Exception):
                nsr = float(stats.get("no_show_rate", 0.0) or 0.0)
                nsr_lbl = t("no_show_rate", lang_value) or "No-show rate"
                seven_lines.append(f"{nsr_lbl}: {nsr:.1f}%")
            # Next booking time (localized)
            with suppress(Exception):
                if stats.get("next_booking_time"):
                    next_lbl = t("next_label", lang_value) or "Next"
                    seven_lines.append(f"{next_lbl}: {stats.get('next_booking_time')}")
            seven_line = "\n" + "\n".join(seven_lines)
        except Exception:
            seven_line = ""

        return f"{summary}{seven_line}"
    except Exception:
        return str(tr("master_menu_header", lang=lang or default_language()))


async def handle_mark_done(
    booking_id: int, lang: str | None = None
) -> tuple[bool, str, InlineKeyboardMarkup]:
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

        return (
            False,
            t("error_retry", lang or default_language()),
            InlineKeyboardMarkup(inline_keyboard=[]),
        )


async def handle_mark_noshow(
    booking_id: int, lang: str | None = None
) -> tuple[bool, str, InlineKeyboardMarkup]:
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

        return (
            False,
            t("error_retry", lang or default_language()),
            InlineKeyboardMarkup(inline_keyboard=[]),
        )


async def handle_client_history(
    booking_id: int, lang: str | None = None
) -> tuple[str, InlineKeyboardMarkup] | None:
    """Return (view_text, kb) for client history; None indicates no history available."""
    try:
        view = await build_client_history_view(booking_id)
        if not view:
            return None
        from aiogram.utils.keyboard import InlineKeyboardBuilder

        kb = InlineKeyboardBuilder()
        from bot.app.telegram.common.callbacks import pack_cb, BookingActionCB

        kb.button(
            text=t("back", lang or default_language()),
            callback_data=pack_cb(BookingActionCB, act="master_detail", booking_id=booking_id),
        )
        kb.adjust(1)
        return view, kb.as_markup()
    except Exception as e:
        logger.exception("handle_client_history failed for %s: %s", booking_id, e)
        return None


async def handle_add_note(
    booking_id: int, lang: str | None = None
) -> tuple[str, InlineKeyboardMarkup]:
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
        kb.button(
            text=t("cancel", lang or default_language()),
            callback_data=pack_cb(BookingActionCB, act="cancel_note", booking_id=booking_id),
        )
        kb.adjust(1)

        if existing_note and isinstance(existing_note, str) and existing_note.strip():
            prompt = f"{t('master_enter_note', lang or default_language())}\n\n{t('master_current_note_prefix', lang or default_language())}: {existing_note}"
        else:
            prompt = t("master_enter_note", lang or default_language())

        return prompt, kb.as_markup()
    except Exception as e:
        logger.exception("handle_add_note failed for %s: %s", booking_id, e)
        from aiogram.types import InlineKeyboardMarkup

        return (
            t("master_enter_note", lang or default_language()),
            InlineKeyboardMarkup(inline_keyboard=[]),
        )


async def handle_cancel_note(
    booking_id: int, lang: str | None = None
) -> tuple[str, InlineKeyboardMarkup] | None:
    """Return booking card text and markup to restore master booking view after cancelling note edit."""
    try:
        # Reuse client_services to build canonical booking details and card
        from bot.app.services.client_services import build_booking_details
        from bot.app.telegram.client.client_keyboards import build_booking_card_kb

        bd = await build_booking_details(booking_id)
        text = format_booking_details_text(bd, lang or "uk", role="master")
        kb = build_booking_card_kb(bd, booking_id, role="master", lang=lang)
        return text, kb
    except Exception as e:
        logger.exception("handle_cancel_note failed for %s: %s", booking_id, e)
        return None


def invalidate_masters_cache() -> None:
    """Invalidate masters cache (useful after CRUD)."""
    global _masters_cache_store
    _masters_cache_store = None
    # Also clear resolve cache so future lookups hit DB once and refresh.
    with suppress(Exception):
        _resolve_master_cache.clear()


# ---------------- MasterRepo (merged from shared_services) -----------------
class MasterRepo:
    """Repository for Master-related persistence (profiles, schedules, bio).

    This implementation contains the canonical methods previously defined
    in `shared_services.MasterRepo`. It consolidates master-related DB
    access in one place so callers can import `MasterRepo` from
    `bot.app.services.master_services` without indirection.
    """

    @staticmethod
    async def _resolve_mid(session: Any, master_identifier: int) -> int | None:
        """Resolve surrogate master id using an existing session.

        Accepts either surrogate id or telegram_id. Returns None if not found.
        """
        try:
            key = int(master_identifier)
        except Exception:
            return None
        try:
            from bot.app.domain.models import Master
            from sqlalchemy import select

            mid = await session.scalar(select(Master.id).where(Master.id == key))
            if mid:
                return int(mid)
            mid = await session.scalar(select(Master.id).where(Master.telegram_id == key))
            return int(mid) if mid else None
        except Exception as e:
            logger.exception("MasterRepo._resolve_mid failed for %s: %s", master_identifier, e)
            return None

    @staticmethod
    async def get_schedule(master_id: int) -> dict[str, Any]:
        """Return normalized schedule dict (DB-only).

        Accepts either a surrogate `masters.id` or a legacy `masters.telegram_id`.
        Preference: treat the argument as surrogate id; if no master found, try
        resolving it as a telegram_id for backwards compatibility.
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterSchedule

                mid = await MasterRepo._resolve_mid(session, master_id)
                if not mid:
                    return {}
                ms_stmt = (
                    select(
                        MasterSchedule.day_of_week,
                        MasterSchedule.start_time,
                        MasterSchedule.end_time,
                    )
                    .where(MasterSchedule.master_id == int(mid))
                    .order_by(MasterSchedule.day_of_week, MasterSchedule.start_time)
                )
                ms_res = await session.execute(ms_stmt)
                rows = ms_res.all()
                if not rows:
                    return {}
                sched: dict[str, list[list[str]]] = {}
                for dow, st, et in rows:
                    try:
                        s = format_slot_label(st, fmt="%H:%M") if st is not None else str(st)
                        e = format_slot_label(et, fmt="%H:%M") if et is not None else str(et)
                    except Exception:
                        s = str(st)
                        e = str(et)
                    sched.setdefault(str(int(dow)), []).append([s, e])
                from bot.app.services.master_services import _normalize_schedule

                return _normalize_schedule(sched)
        except Exception as e:
            logger.warning("MasterRepo.get_schedule failed for %s: %s", master_id, e)
            return {}

    @staticmethod
    async def set_schedule(master_id: int, schedule: dict[str, Any]) -> bool:
        """Persist canonical schedule into `master_schedules` rows.

        Replaces any existing rows for the master with the provided schedule.
        """
        try:
            if not isinstance(schedule, dict):
                logger.warning(
                    "MasterRepo.set_schedule: rejecting non-dict schedule for %s", master_id
                )
                return False
            async with get_session() as session:

                mid = await MasterRepo._resolve_mid(session, master_id)
                if not mid:
                    return False
                canonical = schedule or {}
                # Remove existing schedule rows
                await session.execute(
                    sa.text("DELETE FROM master_schedules WHERE master_id = :mid").bindparams(
                        mid=int(mid)
                    )
                )
                # Insert new rows
                for dow_str, slots in canonical.items():
                    try:
                        dow = int(dow_str)
                    except Exception:
                        continue
                    if not isinstance(slots, (list, tuple)):
                        continue
                    for slot in slots:
                        try:
                            start_label, end_label = slot
                        except Exception:
                            continue
                        await session.execute(
                            sa.text(
                                "INSERT INTO master_schedules (master_id, day_of_week, start_time, end_time, is_day_off, updated_at) "
                                "VALUES (:mid, :dow, :st::time, :et::time, FALSE, now())"
                            ).bindparams(
                                mid=int(mid), dow=dow, st=str(start_label), et=str(end_label)
                            )
                        )
                await session.commit()
            logger.info("MasterRepo.set_schedule: schedule set for %s", master_id)
            return True
        except Exception as e:
            logger.exception("MasterRepo.set_schedule failed for %s: %s", master_id, e)
            return False

    @staticmethod
    async def get_bookings_for_period(
        master_id: int,
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
                base = utc_now()
                if days is None:
                    start = base
                    end = None
                else:
                    start = base
                    end = base + timedelta(days=days)
            else:
                if end is None:
                    end = start + timedelta(days=days) if days is not None else None

            async with get_session() as session:
                from bot.app.domain.models import Booking
                from sqlalchemy import select

                mid = await MasterRepo._resolve_mid(session, master_id)
                if not mid:
                    return []
                stmt = select(Booking).where(Booking.master_id == int(mid))
                if end is not None:
                    stmt = stmt.where(Booking.starts_at.between(start, end))
                else:
                    stmt = stmt.where(Booking.starts_at >= start)

                stmt = stmt.where(Booking.status.notin_(tuple(TERMINAL_STATUSES))).order_by(
                    Booking.starts_at
                )

                result = await session.execute(stmt)
                bookings = list(result.scalars().all())
                logger.info(
                    "MasterRepo.get_bookings_for_period: got %d bookings for master %s",
                    len(bookings),
                    master_id,
                )
                return bookings
        except Exception as e:
            logger.exception("MasterRepo.get_bookings_for_period failed for %s: %s", master_id, e)
            return []

    # --- Pagination helpers (avoid loading entire master list into FSM state) ---
    @staticmethod
    async def count_masters() -> int:
        """Return total number of masters."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select, func

                return int(
                    (
                        await session.execute(
                            select(func.count()).select_from(Master).where(Master.is_active)
                        )
                    ).scalar()
                    or 0
                )
        except Exception as e:
            logger.warning("MasterRepo.count_masters failed: %s", e)
            return 0

    @staticmethod
    async def get_masters_page(page: int = 1, page_size: int = 10) -> list[tuple[int, str]]:
        """Return page of masters as (id, name).

        Note: previously this returned `telegram_id`. During the masters.id
        surrogate rollout callers expect the keyboard payload to carry the
        canonical `Master.id` value so booking flows pass the correct
        identifier into `Booking.master_id`.
        """
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 10
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select

                offset = (page - 1) * page_size
                stmt = (
                    select(Master.id, Master.name)
                    .where(Master.is_active)
                    .order_by(Master.id)
                    .offset(offset)
                    .limit(page_size)
                )
                rows = (await session.execute(stmt)).all()
                return [(int(r[0]), str(r[1]) if r[1] is not None else "") for r in rows]
        except Exception as e:
            logger.warning("MasterRepo.get_masters_page failed (page=%s): %s", page, e)
            return []

    @staticmethod
    async def resolve_master_id(master_identifier: int) -> int | None:
        """Resolve a provided identifier (surrogate id or telegram id) into the
        canonical surrogate `masters.id`.

        Returns the surrogate id (int) or None if resolution failed/not found.
        This central helper unifies resolution logic used across handlers.
        """
        try:
            key = int(master_identifier)
        except Exception:
            return None

        # Fast path: in-memory cache to avoid repeated DB hits for the same id/tid.
        with suppress(Exception):
            if key in _resolve_master_cache:
                return _resolve_master_cache[key]

        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master

                mid = await session.scalar(select(Master.id).where(Master.id == key))
                if mid:
                    resolved = int(mid)
                else:
                    mid = await session.scalar(select(Master.id).where(Master.telegram_id == key))
                    resolved = int(mid) if mid else None

            if resolved is not None:
                with suppress(Exception):
                    _resolve_master_cache[key] = resolved
            return resolved
        except Exception as e:
            logger.exception("MasterRepo.resolve_master_id failed for %s: %s", master_identifier, e)
            return None

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
                from bot.app.domain.models import (
                    Booking,
                    User,
                    MasterClientNote,
                    BookingItem,
                    Service as Svc,
                    Master,
                )
                from sqlalchemy import func

                # Aggregate booking item names; fall back to empty string when
                # there are no BookingItem rows. We intentionally avoid selecting
                # per-service currency values here because currency is sourced
                # from environment configuration (DEFAULT_CURRENCY).
                service_expr = func.coalesce(func.string_agg(Svc.name, " + "), "").label(
                    "service_name"
                )

                stmt = (
                    select(
                        Booking,
                        User,
                        Master.name.label("master_name"),
                        Master.telegram_id.label("master_telegram_id"),
                        MasterClientNote.note.label("client_note"),
                        service_expr,
                    )
                    .outerjoin(User, User.id == Booking.user_id)
                    .outerjoin(Master, Master.id == Booking.master_id)
                    .outerjoin(
                        MasterClientNote,
                        and_(
                            MasterClientNote.master_id == Booking.master_id,
                            MasterClientNote.user_id == Booking.user_id,
                        ),
                    )
                    .outerjoin(BookingItem, BookingItem.booking_id == Booking.id)
                    .outerjoin(Svc, Svc.id == BookingItem.service_id)
                    .where(Booking.id == booking_id)
                    .group_by(
                        Booking.id, User.id, Master.name, Master.telegram_id, MasterClientNote.note
                    )
                )

                res = await session.execute(stmt)
                row = res.first()
                if not row:
                    return None

                booking_obj = row[0]
                client = row[1]
                master_name = row[2]
                master_tid = row[3]
                client_note = row[4]
                service_name = row[5] or ""

                # currency_expr is selected as the next column after service_name
                # Currency is read from global configuration (env), not from
                # per-service DB column. Use SettingsRepo to resolve the
                # canonical currency for this deployment.
                from bot.app.services.admin_services import SettingsRepo

                try:
                    currency = await SettingsRepo.get_currency()
                except Exception:
                    from bot.app.services.shared_services import _default_currency

                    currency = _default_currency()

                price_cents = (
                    getattr(booking_obj, "final_price_cents", None)
                    or getattr(booking_obj, "original_price_cents", None)
                    or 0
                )

                data = {
                    "booking_id": getattr(booking_obj, "id", booking_id),
                    "service_name": service_name,
                    "master_name": master_name,
                    "master_telegram_id": master_tid,
                    "price_cents": price_cents,
                    "currency": currency,
                    "starts_at": getattr(booking_obj, "starts_at", None),
                    "ends_at": getattr(booking_obj, "ends_at", None),
                    "duration_minutes": None,
                    "client_id": (
                        getattr(client, "id", None)
                        if client
                        else getattr(booking_obj, "user_id", None)
                    ),
                    "client_name": getattr(client, "name", None) if client else None,
                    "client_telegram_id": getattr(client, "telegram_id", None) if client else None,
                    "master_id": getattr(booking_obj, "master_id", None),
                    "client_note": client_note,
                }
                # If ends_at is present, compute duration_minutes for display purposes
                with suppress(Exception):
                    sa = data.get("starts_at")
                    ea = data.get("ends_at")
                    if sa and ea:
                        diff = ea - sa
                        data["duration_minutes"] = int(diff.total_seconds() // 60)
                return data
        except Exception as e:
            logger.exception("MasterRepo.get_booking_display_data failed: %s", e)
            return None

    @staticmethod
    async def upsert_client_note(booking_id: int, note_text: str) -> bool:
        """Insert or update MasterClientNote for booking's master and user."""
        try:
            logger.info(
                "MasterRepo.upsert_client_note called booking_id=%s note_len=%s",
                booking_id,
                len(note_text or ""),
            )
            async with get_session() as session:
                from bot.app.domain.models import Booking, MasterClientNote
                from sqlalchemy import select, and_

                booking = await session.get(Booking, booking_id)
                if not (booking and booking.user_id and booking.master_id):
                    logger.warning(
                        "MasterRepo.upsert_client_note: booking or fields missing booking_id=%s booking=%s",
                        booking_id,
                        bool(booking),
                    )
                    return False

                logger.info(
                    "MasterRepo.upsert_client_note: found booking id=%s user_id=%s master_id=%s",
                    getattr(booking, "id", None),
                    getattr(booking, "user_id", None),
                    getattr(booking, "master_id", None),
                )

                note = await session.scalar(
                    select(MasterClientNote).where(
                        and_(
                            MasterClientNote.master_id == booking.master_id,
                            MasterClientNote.user_id == booking.user_id,
                        )
                    )
                )
                if note:
                    logger.info(
                        "MasterRepo.upsert_client_note: updating existing note for master=%s user=%s",
                        booking.master_id,
                        booking.user_id,
                    )
                    note.note = note_text
                else:
                    logger.info(
                        "MasterRepo.upsert_client_note: creating new note for master=%s user=%s",
                        booking.master_id,
                        booking.user_id,
                    )
                    note = MasterClientNote(
                        master_id=booking.master_id,
                        user_id=booking.user_id,
                        note=note_text,
                    )
                    session.add(note)
                await session.commit()
                logger.info(
                    "MasterRepo.upsert_client_note: note updated for booking %s master=%s user=%s",
                    booking_id,
                    booking.master_id,
                    booking.user_id,
                )
                return True
        except Exception as e:
            logger.exception("MasterRepo.upsert_client_note failed for %s: %s", booking_id, e)
            return False

    @staticmethod
    async def get_master_bio(master_telegram_id: int) -> dict[str, Any]:
        """Return master's bio from `masters.bio` as dict or {}."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master

                # Resolve surrogate id and select by master_id
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return {}
                # Read bio from masters table
                bio_text = await session.scalar(
                    sa.text("SELECT bio FROM masters WHERE id = :mid").bindparams(mid=int(mid))
                )
                if not bio_text:
                    return {}
                import json

                try:
                    return json.loads(bio_text or "{}") or {}
                except Exception:
                    return {}
        except Exception as e:
            logger.warning("MasterRepo.get_master_bio failed for %s: %s", master_telegram_id, e)
            return {}

    @staticmethod
    async def update_master_bio(master_telegram_id: int, bio: dict[str, Any]) -> bool:
        """Overwrite master's bio in `masters.bio` with given dict."""
        try:
            import json

            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master

                # Resolve surrogate id
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return False
                await session.execute(
                    sa.text("UPDATE masters SET bio = :bio WHERE id = :mid").bindparams(
                        bio=json.dumps(bio or {}), mid=int(mid)
                    )
                )
                await session.commit()
            # Legacy schedule key (if present) is ignored; schedule now lives solely
            # in master_schedules table via set_master_schedule.
            logger.info("MasterRepo.update_master_bio: bio updated for %s", master_telegram_id)
            return True
        except Exception as e:
            logger.exception(
                "MasterRepo.update_master_bio failed for %s: %s", master_telegram_id, e
            )
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
            return await MasterRepo.get_client_history_for_master_by_user(
                int(master_id), int(client_id)
            )
        except Exception as e:
            logger.exception(
                "MasterRepo.get_client_history_for_master failed for %s: %s", booking_id, e
            )
            return None

    @staticmethod
    async def get_client_history_for_master_by_user(
        master_telegram_id: int, user_id: int
    ) -> dict[str, Any] | None:
        """Return a mapping with client history for the given master/user pair.

        Mapping contains keys: name, visits, total_spent_cents, last_visit, note
        """
        try:
            async with get_session() as session:
                from sqlalchemy import select, and_
                from bot.app.domain.models import Booking, MasterClientNote, User, Master

                # Resolve surrogate master id from provided telegram id or accept if already surrogate
                mid = await session.scalar(
                    select(Master.id).where(Master.id == int(master_telegram_id))
                )
                if not mid:
                    mid = await session.scalar(
                        select(Master.id).where(Master.telegram_id == int(master_telegram_id))
                    )
                if not mid:
                    return None

                history_stmt = (
                    select(Booking)
                    .where(Booking.user_id == int(user_id), Booking.master_id == int(mid))
                    .order_by(Booking.starts_at.desc())
                )
                history_result = await session.execute(history_stmt)
                all_bookings = history_result.scalars().all()

                note = await session.scalar(
                    select(MasterClientNote.note).where(
                        and_(
                            MasterClientNote.master_id == int(mid),
                            MasterClientNote.user_id == int(user_id),
                        )
                    )
                )

                total_spent_cents = 0
                try:
                    for b in all_bookings:
                        if getattr(b, "status", None) in (
                            (
                                __import__(
                                    "bot.app.domain.models", fromlist=["BookingStatus"]
                                ).BookingStatus
                            ).PAID,
                            (
                                __import__(
                                    "bot.app.domain.models", fromlist=["BookingStatus"]
                                ).BookingStatus
                            ).CONFIRMED,
                            (
                                __import__(
                                    "bot.app.domain.models", fromlist=["BookingStatus"]
                                ).BookingStatus
                            ).DONE,
                        ):
                            total_spent_cents += int(
                                getattr(b, "final_price_cents", None)
                                or getattr(b, "original_price_cents", 0)
                                or 0
                            )
                except Exception:
                    total_spent_cents = 0

                user = await session.get(User, user_id)
                texts = _MASTER_TEXT_DEFAULTS
                # Use configured currency (SettingsRepo) rather than hardcoded UAH
                try:
                    cur_code = await SettingsRepo.get_currency()
                except Exception:
                    from bot.app.services.shared_services import _default_currency

                    cur_code = _default_currency()
                history = {
                    "name": (
                        getattr(user, "name", None)
                        if user
                        else texts.get("unknown_client", "unknown")
                    ),
                    "visits": len(all_bookings),
                    "total_spent_cents": total_spent_cents,
                    "total_spent": format_money_cents(total_spent_cents, cur_code),
                    "last_visit": (
                        format_date(all_bookings[0].starts_at, "%d.%m.%Y")
                        if all_bookings
                        else texts.get("no_visits", "Нет")
                    ),
                    "note": note or texts.get("no_notes", ""),
                }
                logger.info(
                    "MasterRepo.get_client_history_for_master_by_user: history built for master=%s user=%s",
                    master_telegram_id,
                    user_id,
                )
                return history
        except Exception as e:
            logger.exception(
                "MasterRepo.get_client_history_for_master_by_user failed for %s/%s: %s",
                master_telegram_id,
                user_id,
                e,
            )
            return None

    @staticmethod
    async def upsert_client_note_for_user(
        master_telegram_id: int, user_id: int, note_text: str
    ) -> bool:
        """Create or update MasterClientNote by master telegram id and user id."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterClientNote, Master
                from sqlalchemy import select, and_

                # Resolve surrogate master id from provided telegram id
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == int(master_telegram_id))
                )
                if not mid:
                    return False

                note = await session.scalar(
                    select(MasterClientNote).where(
                        and_(
                            MasterClientNote.master_id == int(mid),
                            MasterClientNote.user_id == int(user_id),
                        )
                    )
                )
                if note:
                    note.note = note_text
                else:
                    note = MasterClientNote(
                        master_id=int(mid), user_id=int(user_id), note=note_text
                    )
                    session.add(note)
                await session.commit()
            logger.info(
                "MasterRepo.upsert_client_note_for_user: updated note for master=%s user=%s",
                master_telegram_id,
                user_id,
            )
            return True
        except Exception as e:
            logger.exception(
                "MasterRepo.upsert_client_note_for_user failed for %s/%s: %s",
                master_telegram_id,
                user_id,
                e,
            )
            return False

    @staticmethod
    async def get_master_profile_data(master_id: int) -> dict[str, Any] | None:
        """Fetch master profile composed data: master, services, durations_map, about_text, reviews."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import (
                    Master,
                    Service,
                    MasterService,
                    BookingRating,
                    Booking,
                )

                master = await session.get(Master, master_id)
                if not master:
                    return None

                # services offered by master
                svc_stmt = (
                    select(Service.id, Service.name, Service.category, Service.price_cents)
                    .join(MasterService, MasterService.service_id == Service.id)
                    .where(MasterService.master_id == master_id)
                )
                res = await session.execute(svc_stmt)
                # Map rows to tuples: id, name, category, price_cents
                rows = res.all()
                try:
                    from bot.app.services.admin_services import SettingsRepo

                    global_currency = await SettingsRepo.get_currency()
                except Exception:
                    from bot.app.services.shared_services import _default_currency

                    global_currency = _default_currency()

                services = [(str(r[0]), r[1], r[2], r[3], global_currency) for r in rows]

                # Aggregate rating and completed orders for the profile header
                rating_avg = None
                ratings_count = 0
                completed_orders = 0
                try:
                    rating_row = await session.execute(
                        select(func.avg(BookingRating.rating), func.count(BookingRating.rating))
                        .select_from(BookingRating)
                        .join(Booking, Booking.id == BookingRating.booking_id)
                        .where(Booking.master_id == master_id)
                    )
                    rating_avg, ratings_count = rating_row.one_or_none() or (None, 0)
                except Exception:
                    rating_avg, ratings_count = None, 0

                try:
                    completed_orders = int(
                        (
                            await session.execute(
                                select(func.count())
                                .select_from(Booking)
                                .where(
                                    Booking.master_id == master_id,
                                    Booking.status == BookingStatus.DONE,
                                )
                            )
                        ).scalar()
                        or 0
                    )
                except Exception:
                    completed_orders = 0

                # Attach metrics to master instance for downstream formatter
                with suppress(Exception):
                    if rating_avg is not None:
                        master.rating = float(rating_avg)  # type: ignore[attr-defined]
                    master.completed_orders = completed_orders  # type: ignore[attr-defined]
                    master.ratings_count = int(ratings_count or 0)  # type: ignore[attr-defined]

                # profile bio -> durations and about (now stored on masters.bio)
                try:
                    import json

                    bio_text = await session.scalar(
                        sa.text("SELECT bio FROM masters WHERE id = :mid").bindparams(
                            mid=int(master_id)
                        )
                    )
                    bio = json.loads(bio_text or "{}") if bio_text else {}
                except Exception:
                    bio = {}

                # Start from bio-provided durations (legacy), then override with MasterService table values
                durations_map = bio.get("durations") or bio.get("durations_map") or {}
                # Normalize to str->int where possible
                with suppress(Exception):
                    durations_map = {str(k): int(v) for k, v in (durations_map or {}).items() if k}
                # Fetch any explicit overrides from master_services table and merge (overrides take precedence)
                try:
                    ms_rows = await session.execute(
                        select(MasterService.service_id, MasterService.duration_minutes).where(
                            MasterService.master_id == master_id
                        )
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
                            MasterService.master_id == master_id
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
                    "rating": rating_avg,
                    "ratings_count": ratings_count,
                    "completed_orders": completed_orders,
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

                existing = await session.scalar(
                    select(Master).where(Master.telegram_id == telegram_id)
                )
                display_name = (
                    name
                    or format_user_display_name(username, first_name, last_name)
                    or str(telegram_id)
                )
                if existing:
                    # If a master row exists but was soft-deleted, resurrect it.
                    try:
                        if not getattr(existing, "is_active", True):
                            existing.is_active = True
                            existing.name = display_name
                            existing.username = username
                            existing.first_name = first_name
                            existing.last_name = last_name
                            session.add(existing)
                            await session.commit()
                            with suppress(Exception):
                                invalidate_masters_cache()
                            return True
                        # Active master already present => do not create duplicate
                        return False
                    except Exception:
                        return False

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
            with suppress(Exception):
                invalidate_masters_cache()
            return True
        except Exception as e:
            logger.exception("MasterRepo.add_master failed for %s: %s", telegram_id, e)
            return False

    @staticmethod
    async def delete_master(master_id: int) -> bool:
        """Delete a Master and cascade unlink from MasterService."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master

                mid = await MasterRepo._resolve_mid(session, master_id)
                if not mid:
                    # Nothing to delete
                    return False

                master = await session.get(Master, int(mid))
                if not master:
                    return False

                # Soft-delete: mark as inactive instead of physical delete.
                master.is_active = False
                session.add(master)
                await session.commit()
            with suppress(Exception):
                invalidate_masters_cache()
            return True
        except Exception as e:
            logger.exception("MasterRepo.delete_master failed for %s: %s", master_id, e)
            return False

    @staticmethod
    async def force_delete_master(
        master_id: int, *, backup: bool = False
    ) -> tuple[bool, dict[str, object]]:
        """Permanently delete a master with cascade and optional JSON backup.

        Behavior:
        - Resolve provided identifier (surrogate id or telegram id).
        - Collect related rows (master, master_services, profile, client notes, bookings)
          and write them to a JSON file under ./backups/ if `backup=True`.
        - Within a single transaction, update `bookings.master_id` -> NULL for
          bookings referencing the master, delete `master_services`, `master_profiles`,
          `master_client_notes`, and then delete the master row physically.
        - Invalidate masters cache and return metadata including backup path.

        Returns: (success: bool, metadata: dict)
        """
        try:
            async with get_session() as session:
                from bot.app.domain.models import (
                    Master,
                    MasterService,
                    MasterClientNote,
                    Booking,
                )
                from sqlalchemy import select, delete, update

                mid = await MasterRepo._resolve_mid(session, master_id)
                if not mid:
                    return False, {}

                # Collect backup data
                data: dict[str, object] = {}
                master_row = await session.get(Master, int(mid))
                if master_row:
                    data["master"] = {
                        "id": getattr(master_row, "id", None),
                        "telegram_id": getattr(master_row, "telegram_id", None),
                        "name": getattr(master_row, "name", None),
                        "username": getattr(master_row, "username", None),
                        "first_name": getattr(master_row, "first_name", None),
                        "last_name": getattr(master_row, "last_name", None),
                        "created_at": getattr(master_row, "created_at", None),
                        "is_active": getattr(master_row, "is_active", None),
                    }
                else:
                    data["master"] = None

                # master_services
                ms_res = await session.execute(
                    select(MasterService).where(MasterService.master_id == int(mid))
                )
                ms_rows = []
                for r in ms_res.scalars().all():
                    ms_rows.append(
                        {
                            "master_id": getattr(r, "master_id", None),
                            "service_id": getattr(r, "service_id", None),
                            "duration_minutes": getattr(r, "duration_minutes", None),
                        }
                    )
                data["master_services"] = ms_rows

                # profile: store bio text from masters
                try:
                    bio_text = await session.scalar(
                        sa.text("SELECT bio FROM masters WHERE id = :mid").bindparams(mid=int(mid))
                    )
                    data["profile"] = {"bio": bio_text}
                except Exception:
                    data["profile"] = None

                # client notes
                notes_res = await session.execute(
                    select(MasterClientNote).where(MasterClientNote.master_id == int(mid))
                )
                notes_rows = []
                for n in notes_res.scalars().all():
                    notes_rows.append(
                        {
                            "id": getattr(n, "id", None),
                            "user_id": getattr(n, "user_id", None),
                            "note": getattr(n, "note", None),
                        }
                    )
                data["client_notes"] = notes_rows

                # bookings referencing this master (minimal snapshot)
                bk_res = await session.execute(select(Booking).where(Booking.master_id == int(mid)))
                bk_rows = []
                for b in bk_res.scalars().all():
                    try:
                        starts = getattr(b, "starts_at", None)
                        starts = starts.isoformat() if starts is not None else None
                    except Exception:
                        starts = None
                    # Normalize status: prefer enum.value when available, otherwise use raw value
                    status_attr = getattr(b, "status", None)
                    try:
                        status_val = status_attr.value
                    except Exception:
                        status_val = status_attr
                    bk_rows.append(
                        {
                            "id": getattr(b, "id", None),
                            "user_id": getattr(b, "user_id", None),
                            "starts_at": starts,
                            "status": status_val,
                        }
                    )
                data["bookings"] = bk_rows

                backup_file = None
                if backup:
                    try:
                        import json
                        import os
                        import time

                        ts = int(time.time())
                        base_dir = os.path.join(os.getcwd(), "backups")
                        os.makedirs(base_dir, exist_ok=True)
                        fname = f"master_backup_{mid}_{ts}.json"
                        backup_file = os.path.join(base_dir, fname)
                        with open(backup_file, "w", encoding="utf-8") as fh:
                            json.dump(data, fh, default=str, ensure_ascii=False, indent=2)
                    except Exception as e:
                        logger.exception(
                            "MasterRepo.force_delete_master: failed to write backup: %s", e
                        )
                        backup_file = None

                # Apply destructive changes inside the same transaction
                # 1) Unassign bookings -> set master_id = NULL
                await session.execute(
                    update(Booking).where(Booking.master_id == int(mid)).values(master_id=None)
                )

                # 2) Delete master_services rows
                await session.execute(
                    delete(MasterService).where(MasterService.master_id == int(mid))
                )

                # 3) Delete notes (profile table removed)
                await session.execute(
                    delete(MasterClientNote).where(MasterClientNote.master_id == int(mid))
                )

                # 4) Delete the master row physically
                await session.execute(delete(Master).where(Master.id == int(mid)))

                await session.commit()

            # Invalidate cache
            with suppress(Exception):
                invalidate_masters_cache()

            metadata = {
                "backup_file": backup_file,
                "deleted_master_id": int(mid),
                "deleted_master_services": len(ms_rows),
                "deleted_client_notes": len(notes_rows),
                "deleted_profile": 1 if data.get("profile") else 0,
                "unassigned_bookings": len(bk_rows),
            }
            return True, metadata
        except Exception as e:
            logger.exception("MasterRepo.force_delete_master failed for %s: %s", master_id, e)
            return False, {}

    @staticmethod
    async def link_service(master_telegram_id: int, service_id: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterService, Master
                from sqlalchemy import select

                # Resolve surrogate master.id from the provided telegram id.
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    # No known master with this telegram id
                    return False
                existing = await session.scalar(
                    select(MasterService).where(
                        MasterService.master_id == int(mid),
                        MasterService.service_id == service_id,
                    )
                )
                if existing:
                    return False
                session.add(MasterService(master_id=int(mid), service_id=service_id))
                await session.commit()
            return True
        except Exception as e:
            logger.exception(
                "MasterRepo.link_service failed for %s/%s: %s", master_telegram_id, service_id, e
            )
            return False

    @staticmethod
    async def unlink_service(master_telegram_id: int, service_id: str) -> bool:
        try:
            async with get_session() as session:
                from bot.app.domain.models import MasterService, Master
                from sqlalchemy import delete, select

                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return False
                await session.execute(
                    delete(MasterService).where(
                        MasterService.master_id == int(mid),
                        MasterService.service_id == service_id,
                    )
                )
                await session.commit()
            return True
        except Exception as e:
            logger.exception(
                "MasterRepo.unlink_service failed for %s/%s: %s", master_telegram_id, service_id, e
            )
            return False

    @staticmethod
    async def get_services_for_master(master_telegram_id: int) -> list[tuple[str, str]]:
        """Return list of (service_id, name) for services linked to the given master."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Service, MasterService, Master

                # Resolve surrogate id and query by master_id for correctness/efficiency
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return []
                stmt = (
                    select(Service.id, Service.name)
                    .join(MasterService, MasterService.service_id == Service.id)
                    .where(MasterService.master_id == int(mid))
                    .order_by(Service.name)
                )
                res = await session.execute(stmt)
                rows = res.fetchall()
                return [(str(sid), name) for sid, name in rows]
        except Exception as e:
            logger.exception(
                "MasterRepo.get_services_for_master failed for %s: %s", master_telegram_id, e
            )
            return []

    @staticmethod
    async def get_services_with_durations_for_master(
        master_identifier: int,
    ) -> list[tuple[str, str, int | None]]:
        """Return list of (service_id, name, effective_duration_minutes) for the master.

        The priority for effective duration is:
            1. MasterService.duration_minutes override if >0
            2. Service.duration_minutes if >0
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
                from bot.app.domain.models import Service, MasterService, Master

                # Resolve provided identifier (could be surrogate id or telegram id)
                mid = await session.scalar(
                    select(Master.id).where(Master.id == int(master_identifier))
                )
                if not mid:
                    mid = await session.scalar(
                        select(Master.id).where(Master.telegram_id == int(master_identifier))
                    )
                if not mid:
                    return []

                stmt = (
                    select(
                        Service.id,
                        Service.name,
                        MasterService.duration_minutes,
                        Service.duration_minutes,
                    )
                    .select_from(MasterService)
                    .join(Service, Service.id == MasterService.service_id)
                    .where(MasterService.master_id == int(mid))
                    .order_by(Service.name)
                )
                rows = (await session.execute(stmt)).all()
                out: list[tuple[str, str, int | None]] = []
                for sid, name, ms_dur, svc_dur in rows:
                    eff = None
                    try:
                        ms_val = int(ms_dur) if ms_dur is not None else None
                        svc_val = int(svc_dur) if svc_dur is not None else None
                        if ms_val and ms_val > 0:
                            eff = ms_val
                        elif svc_val and svc_val > 0:
                            eff = svc_val
                        else:
                            eff = slot_default
                    except Exception:
                        eff = slot_default
                    out.append((str(sid), str(name), eff))
                return out
        except Exception as e:
            logger.exception(
                "MasterRepo.get_services_with_durations_for_master failed for %s: %s",
                master_identifier,
                e,
            )
            return []

    @staticmethod
    async def set_master_service_duration(
        master_telegram_id: int, service_id: str, minutes: int | None
    ) -> bool:
        """Upsert duration override for (master, service)."""
        try:
            if minutes is not None and minutes <= 0:
                # Treat non-positive as remove override (persist NULL)
                minutes = None
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import MasterService, Master

                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return False
                row = await session.scalar(
                    select(MasterService).where(
                        MasterService.master_id == int(mid),
                        MasterService.service_id == service_id,
                    )
                )
                if not row:
                    # ensure link exists first
                    session.add(
                        MasterService(
                            master_id=int(mid), service_id=service_id, duration_minutes=minutes
                        )
                    )
                else:
                    with suppress(Exception):
                        # row.duration_minutes is Optional[int]; safe to assign None
                        row.duration_minutes = minutes
                await session.commit()
            return True
        except Exception as e:
            logger.exception(
                "set_master_service_duration failed for %s/%s: %s",
                master_telegram_id,
                service_id,
                e,
            )
            return False

    @staticmethod
    async def get_clients_for_master(
        master_telegram_id: int,
    ) -> list[tuple[int, str | None, str | None]]:
        """Return list of unique clients (user_id, name, username) who ever booked with this master."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import User, Booking, Master

                # Resolve surrogate master id from provided telegram id and query
                mid = await session.scalar(
                    select(Master.id).where(Master.telegram_id == master_telegram_id)
                )
                if not mid:
                    return []

                stmt = (
                    select(User.id, User.name, User.username)
                    .join(Booking, Booking.user_id == User.id)
                    .where(Booking.master_id == int(mid))
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
            logger.exception(
                "MasterRepo.get_clients_for_master failed for %s: %s", master_telegram_id, e
            )
            return []

    @staticmethod
    async def get_masters_for_service(service_id: str) -> list[Any]:
        """Return list of Master models for a given service_id."""
        try:
            async with get_session() as session:
                from sqlalchemy import select
                from bot.app.domain.models import Master, MasterService

                # Fetch master surrogate IDs from the junction table, then
                # load Master rows by id. This avoids scalar subqueries using
                # Master.telegram_id and uses the indexed surrogate key.
                mid_rows = await session.execute(
                    select(MasterService.master_id).where(MasterService.service_id == service_id)
                )
                raw_mid_rows = mid_rows.all()
                mids = [int(r[0]) for r in raw_mid_rows if r and r[0] is not None]

                logger.info(
                    "get_masters_for_service: service_id=%r -> raw_mid_rows=%r -> mids=%s",
                    service_id,
                    raw_mid_rows,
                    mids,
                )

                if not mids:
                    # Debug: sample master_services rows if none found
                    try:
                        all_rows = await session.execute(
                            select(MasterService.master_id, MasterService.service_id)
                        )
                        logger.info(
                            "get_masters_for_service: master_services full sample=%r",
                            all_rows.all(),
                        )
                    except Exception:
                        logger.info(
                            "get_masters_for_service: failed to fetch master_services sample for debugging"
                        )
                    return []

                res = await session.execute(select(Master).where(Master.id.in_(mids)))
                masters = list(res.scalars().all())
                logger.info(
                    "get_masters_for_service: service_id=%s -> master_ids=%s, masters_found=%d",
                    service_id,
                    mids,
                    len(masters),
                )
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

                stmt = (
                    select(MasterService.service_id)
                    .where(MasterService.service_id.in_(wanted_ids))
                    .distinct()
                )
                res = await session.execute(stmt)
                return {str(r[0]) for r in res.all()}
        except Exception as e:
            logger.exception("MasterRepo.services_with_masters failed: %s", e)
            return set()

    @staticmethod
    async def get_master(master_telegram_id: int) -> object | None:
        """Return Master model by telegram id or None."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select

                res = await session.scalar(
                    select(Master).where(Master.telegram_id == master_telegram_id)
                )
                return cast(object | None, res)
        except Exception as e:
            logger.exception("MasterRepo.get_master failed for %s: %s", master_telegram_id, e)
            return None

    @staticmethod
    async def get_master_name(master_identifier: int) -> str | None:
        """Return master's display name by surrogate ID or telegram id."""
        try:
            async with get_session() as session:
                from bot.app.domain.models import Master
                from sqlalchemy import select, or_

                # Ищем совпадение либо по id (первичному ключу), либо по telegram_id
                res = await session.execute(
                    select(Master.name).where(
                        or_(
                            Master.id == int(master_identifier),
                            Master.telegram_id == int(master_identifier),
                        )
                    )
                )
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
                    .join(MasterService, MasterService.master_id == Master.id)
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


def format_client_history(hist: Mapping[str, Any], user_id: int, lang: str | None = None) -> str:
    """Format client history mapping into a short text block for master UI.

    This formatter prefers a provided `lang` but will fall back to the
    project-wide default language if none is supplied. It uses translation
    keys (via tr/t) for all visible labels so the output is localized.
    """
    try:
        lang_value = lang or default_language()

        header = tr("master_client_history_header", lang=lang_value)
        lines: list[str] = [f"{header} #{user_id}"]

        visits = hist.get("visits", 0)
        spent = hist.get("total_spent_cents", 0)
        rating = hist.get("rating")
        rating_txt = f"{rating}⭐" if isinstance(rating, (int, float)) else None

        fields = [
            (tr("client_label", lang=lang_value) or "Name", hist.get("name")),
            (tr("master_total_visits", lang=lang_value) or "Visits", visits),
            (tr("master_total_spent", lang=lang_value) or "Spent", format_money_cents(spent)),
            (tr("master_last_visit", lang=lang_value) or "Last visit", hist.get("last_visit")),
            (tr("rating_label", lang=lang_value) or "Rating", rating_txt),
            (tr("master_note", lang=lang_value) or "Note", hist.get("note")),
        ]

        lines.extend([f"{label}: {value}" for label, value in fields if value not in (None, "")])
        return "\n".join(lines)
    except Exception as e:
        logger.warning("format_client_history failed: %s", e)
        return ""


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
        now = utc_now()
        end = now + timedelta(days=horizon_days)
        from bot.app.services.master_services import MasterRepo

        sched = _normalize_schedule(await MasterRepo.get_schedule(master_telegram_id) or {})

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
        logger.exception(
            "check_future_booking_conflicts failed for master %s: %s", master_telegram_id, e
        )
        return []


async def cancel_bookings_and_notify(
    bot: Any, booking_ids: list[int] | None, *, notify_admins: bool = True
) -> int:
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
        from bot.app.core.notifications import send_booking_notification
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


async def ensure_booking_owner(user_id: int, booking_id: int) -> Booking | None:
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


# Removed thin wrappers `get_master_profile_data` and `get_master_schedule`.
# Call `MasterRepo.get_master_profile_data(...)` and
# `MasterRepo.get_schedule(...)` directly from handlers when repo access
# is required. These wrappers previously hid errors and added indirection.


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


async def set_master_schedule(
    master_telegram_id: int, schedule: dict[str, list[list[str]]]
) -> bool:
    """Persist master schedule into relational table (DB-only).

    Input format: keys are weekday numbers (0=Mon..6=Sun) as int/str, values are
    lists of [start,end] pairs (HH:MM). Legacy JSON storage removed.
    """
    try:
        canonical = _normalize_schedule(schedule or {})
        async with get_session() as session:
            from sqlalchemy import select, delete
            from bot.app.domain.models import MasterSchedule, Master

            # Resolve surrogate master.id from telegram id
            mid = await session.scalar(
                select(Master.id).where(Master.telegram_id == master_telegram_id)
            )
            if not mid:
                # Unknown master; nothing to store
                return False
            # Delete existing rows for this master
            await session.execute(
                delete(MasterSchedule).where(MasterSchedule.master_id == int(mid))
            )
            # Insert new windows
            now_ts = utc_now()
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
                    to_add.append(
                        MasterSchedule(
                            master_id=int(mid),
                            day_of_week=dow,
                            start_time=start_time,
                            end_time=end_time,
                            updated_at=now_ts,
                        )
                    )
            for obj in to_add:
                session.add(obj)
            await session.commit()
        logger.info(
            "set_master_schedule: stored %d windows for master %s", len(to_add), master_telegram_id
        )
        return True
    except Exception as e:
        logger.exception("set_master_schedule failed for %s: %s", master_telegram_id, e)
        return False


async def set_master_schedule_day(
    master_telegram_id: int, day: int, windows: list[list[str]]
) -> bool:
    """Set windows for a single weekday (day: 0..6). Pass windows=[] to clear the day.
    windows should be list of [start, end] pairs as strings.
    """
    try:
        from bot.app.services.master_services import MasterRepo

        sched = await MasterRepo.get_schedule(master_telegram_id) or {}
        sched[str(day)] = windows
        return await set_master_schedule(master_telegram_id, sched)
    except Exception as e:
        logger.exception(
            "Failed to set schedule day for master %s day=%s: %s", master_telegram_id, day, e
        )
        return False


async def remove_schedule_window_by_value(
    master_telegram_id: int, day: int, start: str, end: str
) -> tuple[bool, list[str]]:
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
    try:
        a_min = _parse_hm_to_minutes(start)
        b_min = _parse_hm_to_minutes(end)
        a = _minutes_to_hm(a_min)
        b = _minutes_to_hm(b_min)
    except Exception:
        return False, []
    if not a or not b:
        return False, []
    try:
        from bot.app.services.master_services import MasterRepo

        sched = await MasterRepo.get_schedule(mid)
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
        # Use shared utc_now() helper to obtain an aware UTC datetime
        now = utc_now()
        try:
            bookings = await MasterRepo.get_bookings_for_period(mid, start=now, days=365)
        except SQLAlchemyError:
            bookings = []
        # a_min and b_min already computed above
        for booking in bookings or []:
            try:
                starts = getattr(booking, "starts_at", None)
                if not starts or starts.weekday() != d:
                    continue
                start_min = starts.hour * 60 + starts.minute
                if start_min >= a_min and start_min < b_min:
                    try:
                        client, _ = await enrich_booking_context(booking)
                        user_name = (
                            getattr(client, "name", None) or f"id:{getattr(client, 'id', '?')}"
                        )
                    except SQLAlchemyError:
                        user_name = f"id:{getattr(booking, 'user_id', '?')}"
                    except Exception:
                        user_name = f"id:{getattr(booking, 'user_id', '?')}"
                    try:
                        iso = starts.isoformat()
                    except AttributeError:
                        try:
                            iso = format_date(starts, "%Y-%m-%d %H:%M")
                        except Exception:
                            iso = str(starts)
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
        logger.exception(
            "remove_schedule_window_by_value unexpected error for %s: %s", master_telegram_id, e
        )
        return False, []


def _normalize_schedule(schedule: dict[str, Any] | None) -> dict[str, list[list[str]]]:
    """Normalize schedule into {weekday: [[HH:MM, HH:MM], ...]} mapping."""

    out: dict[str, list[list[str]]] = {}
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


# Thin delegator facades removed; use `MasterRepo` methods directly.


async def get_master_stats_summary(master_telegram_id: int, *, days: int = 7) -> dict[str, Any]:
    """Return a small stats summary for a master for the given period.

    Returns a dict with keys: total_bookings, completed_bookings, no_shows, next_booking_time
    """
    try:
        try:
            resolved_mid = await MasterRepo.resolve_master_id(int(master_telegram_id))
        except Exception:
            resolved_mid = None
        master_id = resolved_mid or int(master_telegram_id)
        from sqlalchemy import select, func, case
        from bot.app.domain.models import Booking, BookingStatus

        now = utc_now()
        start = now - timedelta(days=days)
        end = now
        async with get_session() as session:
            # Single aggregate query for counts and revenue
            agg_stmt = select(
                func.count(Booking.id).label("total"),
                func.sum(case((Booking.status == BookingStatus.DONE, 1), else_=0)).label(
                    "completed"
                ),
                func.sum(case((Booking.status == BookingStatus.NO_SHOW, 1), else_=0)).label(
                    "noshow"
                ),
                func.coalesce(
                    func.sum(
                        func.coalesce(Booking.final_price_cents, Booking.original_price_cents)
                    ),
                    0,
                ).label("revenue_cents"),
            ).where(
                Booking.master_id == int(master_id),
                Booking.starts_at.between(start, end),
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
            next_stmt = (
                select(Booking.starts_at)
                .where(Booking.master_id == int(master_id), Booking.starts_at >= now)
                .order_by(Booking.starts_at.asc())
                .limit(1)
            )
            nb = await session.execute(next_stmt)
            next_row = nb.first()
            next_time = None
            if next_row and next_row[0]:
                try:
                    next_time = format_date(next_row[0], "%d.%m %H:%M")
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
        return {
            "total_bookings": 0,
            "completed_bookings": 0,
            "no_shows": 0,
            "next_booking_time": None,
        }


async def get_work_windows_for_day(
    master_id: int, target_date: _date | datetime
) -> list[tuple[_time, _time]]:
    """Async helper: fetch MasterSchedule rows and return
    work windows for target_date.

    IMPORTANT: this function accepts only the surrogate `masters.id` value.
    Callers must pass the database primary key (`Master.id`). Legacy
    `telegram_id` values are NOT accepted here. If callers still pass
    telegram IDs, schedules may not be found — callers should resolve
    telegram->id before calling this helper.
    """

    def _default_window() -> list[tuple[_time, _time]]:
        """Return the configured default working window using constants.

        Falls back to 09:00–18:00 if constants are missing/invalid.
        """
        with suppress(Exception):
            start_h = int(DEFAULT_DAY_START_HOUR)
            end_h = int(DEFAULT_DAY_END_HOUR)
            if 0 <= start_h < 24 and 0 < end_h <= 24 and start_h < end_h:
                return [(_time(hour=start_h), _time(hour=end_h))]
        return [(_time(hour=9), _time(hour=18))]

    try:
        # Normalize target_date to a date object
        td = target_date.date() if isinstance(target_date, datetime) else target_date
        # Use relational tables only: check per-date exceptions first, then
        # weekly schedules (respecting `is_day_off`), and finally default
        # working hours. We no longer read `master_profiles.bio` for schedules.
        async with get_session() as session:
            from sqlalchemy import select
            from bot.app.domain.models import MasterSchedule, MasterScheduleException

            # Inspect exceptions for the target date by master_id
            stmt_exc = (
                select(
                    MasterScheduleException.start_time,
                    MasterScheduleException.end_time,
                    MasterScheduleException.reason,
                )
                .where(
                    MasterScheduleException.master_id == int(master_id),
                    MasterScheduleException.exception_date == td,
                )
                .order_by(MasterScheduleException.start_time)
            )
            res_exc = await session.execute(stmt_exc)
            exc_rows = res_exc.all()
            if exc_rows:
                # If any exception row is a sentinel off-day (start==end==00:00 or reason=='off'), treat as day off
                for st, et, reason in exc_rows:
                    with suppress(Exception):
                        if (
                            format_slot_label(st, fmt="%H:%M") == "00:00"
                            and format_slot_label(et, fmt="%H:%M") == "00:00"
                        ) or (reason and str(reason).lower() == "off"):
                            return []
                # Otherwise, return explicit exception windows
                windows = []
                for st, et, _ in exc_rows:
                    try:
                        s = format_slot_label(st, fmt="%H:%M") if st is not None else str(st)
                        e = format_slot_label(et, fmt="%H:%M") if et is not None else str(et)
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

            # No per-date exceptions -> consider weekly schedules
            wd = int(td.weekday())
            stmt = (
                select(
                    MasterSchedule.start_time, MasterSchedule.end_time, MasterSchedule.is_day_off
                )
                .where(MasterSchedule.master_id == int(master_id), MasterSchedule.day_of_week == wd)
                .order_by(MasterSchedule.start_time)
            )
            res = await session.execute(stmt)
            rows = res.all()
            if rows:
                # If any row marks the weekday as a day off, return empty
                for _st, _et, is_off in rows:
                    try:
                        if bool(is_off):
                            return []
                    except Exception:
                        continue
                windows = []
                for st, et, _ in rows:
                    try:
                        s = format_slot_label(st, fmt="%H:%M") if st is not None else str(st)
                        e = format_slot_label(et, fmt="%H:%M") if et is not None else str(et)
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

            # No schedules found: return default working window
            return _default_window()
    except Exception:
        return _default_window()


def insert_window(
    schedule: dict[str, list[list[str]]] | None,
    day: int,
    start: str,
    end: str,
    adjacency_min: int = 0,
) -> dict[str, list[list[str]]]:
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


def remove_all_windows(
    schedule: dict[str, list[list[str]]] | None, day: int
) -> dict[str, list[list[str]]]:
    """Mark given day as empty list (workday cleared)."""
    if schedule is None:
        schedule = {}
    out = {str(k): v for k, v in (schedule or {}).items()}
    out[str(day)] = []
    return out


def copy_day(
    schedule: dict[str, list[list[str]]] | None,
    target_day: int,
    source_day: int,
    mode: str = "replace",
) -> dict[str, list[list[str]]]:
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


def render_schedule_table(
    schedule: dict[str, list[list[str]]] | None, lang: str | None = None
) -> str:
    """Render schedule dict into human-readable multi-line table for Mon..Sun.

    Accepts optional `lang` so output can be localized. Uses the translation
    key `closed_label` when a day has no windows.
    """
    sched: dict[str, list[list[str]]] = schedule or {}
    days = tr("weekday_short", lang=lang) or ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    closed_lbl = tr("closed_label", lang=lang) or "Closed"
    lines: list[str] = []
    for idx, name in enumerate(days):
        w = sched.get(str(idx), [])
        if not w:
            lines.append(f"{name}: {closed_lbl}")
            continue
        parts = []
        for rng in w or []:
            try:
                parts.append(f"{str(rng[0])}-{str(rng[1])}")
            except Exception:
                continue
        if parts:
            lines.append(f"{name}: {', '.join(parts)}")
        else:
            lines.append(f"{name}: {closed_lbl}")
    return "\n".join(lines)
