"""Background worker to send 24h visit reminders.

Scans upcoming bookings and sends a reminder message about 24 hours before start.
Marks a per-booking flag to avoid duplicate notifications.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collections.abc import Awaitable, Callable

from aiogram import Bot
from sqlalchemy import select, update

from bot.app.core.db import get_session
from bot.app.core.constants import REMINDERS_CHECK_SECONDS, REMINDERS_CHECK_SECONDS_INVALID
from bot.app.domain.models import Booking
from bot.app.services.shared_services import safe_get_locale, local_now, utc_now, get_local_tz
from bot.app.translations import t

logger = logging.getLogger(__name__)


async def _remind_once(now_utc: datetime, bot: Bot) -> int:
    """Scan upcoming bookings within the next 24 hours and send reminders.

    Returns the number of reminders successfully sent.
    """
    # Determine lead time from settings (minutes) and compute window
    from bot.app.services.client_services import UserRepo

    # Use ServiceRepo + SettingsRepo directly
    from bot.app.services.admin_services import ServiceRepo, SettingsRepo
    from bot.app.services.shared_services import _safe_send

    local_tz = get_local_tz() or ZoneInfo("UTC")
    try:
        lead_primary = int(await SettingsRepo.get_reminder_lead_minutes())
    except Exception:
        lead_primary = 60
    try:
        lead_same_day = int(await SettingsRepo.get_same_day_lead_minutes())
    except Exception:
        lead_same_day = 0

    configs: list[tuple[str, int, str]] = []
    if lead_primary and lead_primary > 0:
        configs.append(("lead", lead_primary, "remind_24h_sent"))
    if lead_same_day and lead_same_day > 0:
        configs.append(("same_day", lead_same_day, "remind_1h_sent"))

    if not configs:
        logger.info("Reminders worker: all reminder lead times disabled; skipping sweep")
        return 0

    total_sent = 0

    try:
        from bot.app.domain.models import Booking as BookingModel, REMINDER_ELIGIBLE_STATUSES

        async def _process_config(kind: str, minutes: int, flag_attr: str) -> int:
            window_utc = now_utc + timedelta(minutes=minutes)
            flag_column = getattr(BookingModel, flag_attr)

            async with get_session() as session:
                stmt = (
                    select(BookingModel)
                    .where(
                        BookingModel.starts_at >= now_utc,
                        BookingModel.starts_at < window_utc,
                        BookingModel.status.in_(tuple(REMINDER_ELIGIBLE_STATUSES)),
                        flag_column.is_(False),
                    )
                    .order_by(BookingModel.starts_at)
                )
                res = await session.execute(stmt)
                bookings = res.scalars().all()

            user_ids = {
                int(getattr(b, "user_id", 0) or 0) for b in bookings if getattr(b, "user_id", None)
            }
            clients_map = await UserRepo.get_by_ids(user_ids) if user_ids else {}

            sent_count = 0
            for booking in bookings:
                try:
                    uid = int(
                        getattr(booking, "user_id", getattr(booking, "client_id", 0) or 0) or 0
                    )
                    user = clients_map.get(uid)
                    chat_id = getattr(user, "telegram_id", None) if user else None
                    if not chat_id:
                        logger.debug(
                            "Reminder: no chat_id resolved for booking %s (user_id=%s)",
                            getattr(booking, "id", "?"),
                            uid,
                        )
                        continue

                    lang = await safe_get_locale(int(chat_id))

                    # Resolve service and master display names
                    try:
                        svc_id = getattr(booking, "service_id", None)
                        if not svc_id:
                            service_name = t("service_label", lang)
                        else:
                            service_name = await ServiceRepo.get_service_name(str(svc_id))
                    except Exception:
                        service_name = t("service_label", lang)
                    try:
                        from bot.app.services.master_services import MasterRepo

                        master_name = await MasterRepo.get_master_name(
                            int(getattr(booking, "master_id", 0) or 0)
                        )
                    except Exception:
                        master_name = t("master_label", lang)
                    dt_local = None
                    date_txt = "—"
                    try:
                        starts_at = booking.starts_at
                        dt_local = (
                            starts_at.astimezone(local_tz) if (starts_at is not None) else starts_at
                        )
                        time_txt = f"{dt_local:%H:%M}"
                        date_txt = f"{dt_local:%d.%m}"
                    except Exception:
                        time_txt = "--:--"

                    try:
                        now_local = local_now()
                        starts_date = dt_local.date() if dt_local is not None else None
                        now_date = now_local.date()
                        days_diff = (starts_date - now_date).days if starts_date else None
                        tomorrow_date = (now_local + timedelta(days=1)).date()
                    except Exception:
                        starts_date = None
                        tomorrow_date = None
                        days_diff = None

                    # Pick body/title depending on kind and lead
                    if kind == "same_day":
                        use_key = "reminder_same_day_body"
                        title_key = "reminder_same_day_title"
                    else:
                        if days_diff is not None and days_diff >= 2:
                            use_key = "reminder_future_body"
                            title_key = "reminder_24h_title"
                        elif starts_date is not None and starts_date == tomorrow_date:
                            use_key = "reminder_24h_body"
                            title_key = "reminder_24h_title"
                        elif abs(int(minutes) - 60) <= 5:
                            use_key = "reminder_1h_body"
                            title_key = "reminder_1h_title"
                        else:
                            use_key = "reminder_same_day_body"
                            title_key = "reminder_same_day_title"

                    title = t(title_key, lang)
                    if title == title_key:
                        title = {
                            "uk": "Нагадування про запис",
                            "ru": "Напоминание о записи",
                            "en": "Appointment reminder",
                        }.get(lang, "Appointment reminder")

                    body_template = t(use_key, lang)
                    if not isinstance(body_template, str) or body_template == use_key:
                        if use_key == "reminder_24h_body":
                            body_template = t("reminder_24h_body", lang)
                        elif use_key == "reminder_1h_body":
                            body_template = t("reminder_1h_body", lang)
                        else:
                            body_template = t("reminder_same_day_body", lang)

                    body = body_template.format(
                        time=time_txt, service=service_name, master=master_name, date=date_txt
                    )
                    text = f"<b>{title}</b>\n\n{body}"

                    ok = await _safe_send(bot, chat_id, text)
                    if not ok:
                        logger.warning(
                            "Failed to send reminder to %s for booking %s",
                            chat_id,
                            getattr(booking, "id", "?"),
                        )
                        continue

                    try:
                        async with get_session() as session:
                            now_ts = utc_now()
                            values = {
                                "last_reminder_sent_at": now_ts,
                                "last_reminder_lead_minutes": int(minutes),
                                flag_attr: True,
                            }
                            await session.execute(
                                update(Booking)
                                .where(Booking.id == getattr(booking, "id", None))
                                .values(**values)
                            )
                            await session.commit()
                    except Exception:
                        logger.exception(
                            "Failed to mark reminder metadata for booking %s",
                            getattr(booking, "id", "?"),
                        )

                    sent_count += 1
                except Exception as ie:
                    logger.exception(
                        "Error processing reminder for booking %s: %s",
                        getattr(booking, "id", "?"),
                        ie,
                    )

            return sent_count

        for cfg in configs:
            try:
                total_sent += await _process_config(*cfg)
            except Exception as e:
                logger.exception("Reminder sweep failed for %s: %s", cfg[0], e)

        return total_sent
    except Exception as e:
        logger.error("Reminder sweep failed: %s", e)
        return 0


async def _run_loop(stop_event: asyncio.Event, bot: Bot, interval_seconds: int) -> None:
    # small initial delay
    try:
        await asyncio.sleep(2)
    except Exception:
        logger.exception("reminders: initial sleep interrupted")
    while not stop_event.is_set():
        try:
            await _remind_once(utc_now(), bot)
        except Exception as e:
            logger.exception("Reminders worker iteration error: %s", e)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
        except Exception:
            break


async def start_reminders_worker(bot: Bot) -> Callable[[], Awaitable[None]]:
    """Start the reminders worker and return an async stop() function."""
    interval_seconds = REMINDERS_CHECK_SECONDS
    if REMINDERS_CHECK_SECONDS_INVALID:
        logger.warning("Invalid REMINDERS_CHECK_SECONDS; defaulting to %s", REMINDERS_CHECK_SECONDS)
    stop_event: asyncio.Event = asyncio.Event()
    task = asyncio.create_task(
        _run_loop(stop_event, bot, interval_seconds), name="reminders-worker"
    )

    async def _stop() -> None:
        try:
            stop_event.set()
            try:
                await asyncio.wait_for(task, timeout=5)
            except Exception:
                task.cancel()
        except Exception:
            logger.exception("reminders: stop failed")

    logger.info("Reminders worker started (interval=%ss)", interval_seconds)
    return _stop


async def stop_reminders_worker(
    stop_callable: Callable[[], Awaitable[None]] | None = None,
) -> None:
    if stop_callable:
        await stop_callable()


__all__ = ["start_reminders_worker", "stop_reminders_worker"]
