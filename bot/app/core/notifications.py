from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Iterable, Any
from dataclasses import replace

from aiogram import Bot

from bot.app.services.shared_services import _safe_send

logger = logging.getLogger(__name__)

__all__ = ["notify_admins", "notify_admins_bot_started", "send_booking_notification"]


async def notify_admins(message: str, bot: Bot) -> None:
    """Send a notification message to configured admin IDs using the provided Bot.

    This function now requires an explicit aiogram.Bot instance. It will not
    attempt to import or create a Bot implicitly — callers must pass the
    running bot (for example, the instance created in `run_bot.py`).

    Args:
        message: Text to send to admins.
        bot: An initialized aiogram.Bot instance.
    """
    admin_ids_str = os.getenv("ADMIN_IDS", "")
    admin_ids: Iterable[int] = [
        int(part.strip()) for part in admin_ids_str.split(",") if part.strip().isdigit()
    ]
    if not admin_ids:
        logger.debug("notify_admins: no ADMIN_IDS configured; skipping")
        return

    for admin_id in admin_ids:
        try:
            # Use the centralized safe send helper; fall back to Bot.send_message
            try:
                await _safe_send(bot, admin_id, message)
            except Exception:
                await bot.send_message(admin_id, message)
        except Exception as e:
            logger.error("notify_admins: failed to send to %s: %s", admin_id, e)


async def notify_admins_bot_started(bot: Bot) -> None:
    """Send a bot-started notification to all admins with locale-sensitive text.

    This centralizes the startup ping that was previously in `run_bot.py` so
    all notifications live in one module.
    """
    try:
        from bot.app.services.shared_services import get_admin_ids, safe_get_locale, _safe_send
        from bot.app.translations import t
    except Exception as e:
        logger.exception("notify_admins_bot_started: failed to import dependencies: %s", e)
        return

    admin_ids = get_admin_ids()
    if not admin_ids:
        logger.debug("notify_admins_bot_started: no admins configured; skipping")
        return

    for uid in admin_ids:
        try:
            lang = await safe_get_locale(int(uid or 0))
            msg = t("bot_started_notice", lang)

            if msg == "bot_started_notice":
                msg = {
                    "uk": "Бот запущено. Надішліть /start або /ping.",
                    "ru": "Бот запущен. Отправьте /start или /ping.",
                    "en": "Bot started. Send /start or /ping.",
                }.get(lang, "Bot started. Send /start or /ping.")

            try:
                await _safe_send(bot, uid, msg)
            except Exception:
                await bot.send_message(uid, msg)
        except Exception:
            logger.exception("notify_admins_bot_started: failed to notify admin %s", uid)


async def send_booking_notification(bot: Bot, booking_id: int, event_type: str, recipients: Iterable[int] | None) -> None:
    """Send booking-related notification to the given recipients.

    This is moved here from client_services to centralize notification sending.
    Text/markup construction stays here with lazy imports to avoid cycles.
    """
    logger.info("send_booking_notification: booking=%s event=%s recipients=%s", booking_id, event_type, recipients)
    try:
        # Lazy imports to avoid import-time cycles
        from bot.app.services.client_services import build_booking_details
        from bot.app.services.shared_services import (
            format_booking_details_text,
            format_date,
            safe_get_locale,
            utc_now,
            format_money_cents,
        )
        from bot.app.translations import tr
        from bot.app.domain.models import Booking, BookingStatus
        from bot.app.core.db import get_session
    except Exception as e:
        logger.exception("send_booking_notification: failed to import dependencies: %s", e)
        return

    if not recipients:
        logger.debug("send_booking_notification: no recipients; skipping")
        return

    try:
        no_show_count_recent: int | None = None
        async with get_session() as session:
            booking = await session.get(Booking, booking_id)
            if not booking:
                logger.warning("send_booking_notification: booking %s not found", booking_id)
                return

            if event_type == "no_show":
                try:
                    from sqlalchemy import select, func, and_  # type: ignore

                    window_start = utc_now() - timedelta(days=90)
                    no_show_count_recent = int(
                        await session.scalar(
                            select(func.count(Booking.id)).where(
                                and_(
                                    Booking.user_id == booking.user_id,
                                    Booking.status == BookingStatus.NO_SHOW,
                                    Booking.starts_at >= window_start,
                                )
                            )
                        )
                        or 0
                    )
                except Exception:
                    no_show_count_recent = None

        sent_ids: set[int] = set()
        for rid in recipients:
            try:
                rid_int = int(rid)
            except Exception:
                logger.warning("send_booking_notification: invalid recipient id, skipping: %r", rid)
                continue

            # Resolve master_id to telegram_id if needed
            try:
                master_id_val = getattr(booking, "master_id", None)
                if master_id_val and int(rid_int) == int(master_id_val):
                    from bot.app.domain.models import Master
                    async with get_session() as session:
                        from sqlalchemy import select
                        res = await session.execute(select(Master.telegram_id).where(Master.id == int(rid_int)))
                        tg = res.scalar_one_or_none()
                        if tg:
                            rid_int = int(tg)
            except Exception:
                pass

            # Deduplicate if admin and master resolve to the same Telegram user
            if rid_int in sent_ids:
                continue
            sent_ids.add(rid_int)

            lang = await safe_get_locale(rid_int)
            try:
                bd = await build_booking_details(booking, user_id=rid_int, lang=lang)
            except Exception as be:
                logger.exception("send_booking_notification: build_booking_details failed: %s", be)
                continue

            svc_names = bd.service_name or ""
            starts = bd.starts_at
            dt_txt = format_date(starts) if starts else ""
            client_line = bd.client_name or ""
            master_id_val = getattr(booking, "master_id", None)
            client_tg_id = bd.client_telegram_id

            bd_for_body = bd
            discount_line = None

            try:
                if event_type == "paid":
                    try:
                        final_cents = int(getattr(booking, "final_price_cents", None) or 0)
                    except Exception:
                        final_cents = 0

                    try:
                        original_cents = int(getattr(booking, "original_price_cents", None) or final_cents)
                    except Exception:
                        original_cents = final_cents

                    try:
                        discount_cents = int(getattr(booking, "discount_amount_cents", None) or 0)
                    except Exception:
                        discount_cents = 0

                    # Fallback: derive discount from price delta when explicit amount is missing
                    if discount_cents <= 0:
                        try:
                            if original_cents and original_cents > final_cents:
                                discount_cents = original_cents - final_cents
                        except Exception:
                            pass

                    try:
                        bd_for_body = replace(bd, price_cents=final_cents)
                    except Exception:
                        bd_for_body = bd

                    if discount_cents > 0:
                        try:
                            pct_hint = int(getattr(booking, "discount_percent", None) or 0)
                        except Exception:
                            pct_hint = 0

                        try:
                            pct = pct_hint if pct_hint > 0 else (round((discount_cents * 100) / original_cents) if original_cents else 0)
                        except Exception:
                            pct = pct_hint if pct_hint > 0 else 0

                        try:
                            savings_text = format_money_cents(discount_cents, getattr(bd_for_body, "currency", None))
                        except Exception:
                            savings_text = format_money_cents(discount_cents)

                        disc_label = tr("online_discount_label_plain", lang=lang) or tr("online_discount_label", lang=lang) or "Online discount"
                        if pct and pct > 0:
                            discount_line = f"{disc_label}: -{pct}% ({savings_text})"
                        else:
                            discount_line = f"{disc_label}: {savings_text}"

                if event_type == "paid":
                    title_tpl = tr("notif_paid_online_confirmed", lang=lang)
                    if title_tpl == "notif_paid_online_confirmed":
                        title_tpl = tr("notif_paid_confirmed", lang=lang)
                    title = title_tpl.format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "cash_confirmed":
                    title = tr("notif_cash_confirmed", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "cancelled":
                    title = tr("notif_client_cancelled", lang=lang).format(id=booking_id, user=client_line)
                elif event_type == "rescheduled_by_client":
                    if int(rid_int) == int(master_id_val or 0):
                        title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                    else:
                        title = tr("notif_client_rescheduled", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "rescheduled_by_master":
                    if client_tg_id and int(rid_int) == int(client_tg_id):
                        title = tr("notif_master_rescheduled_client", lang=lang).format(service=svc_names, dt=dt_txt)
                    else:
                        title = tr("notif_master_rescheduled_admin", lang=lang).format(master=master_id_val or "", id=booking_id, service=svc_names, dt=dt_txt)
                elif event_type == "done":
                    title = tr("master_checkin_success", lang=lang)
                elif event_type == "no_show":
                    title = tr("notif_no_show", lang=lang).format(id=booking_id, service=svc_names, dt=dt_txt)
                else:
                    title = f"#{booking_id}: {svc_names} {dt_txt}".strip()
            except Exception:
                title = f"#{booking_id}"

            body = format_booking_details_text(bd_for_body, lang)
            if event_type == "paid":
                try:
                    paid_label = tr("amount_paid_label", lang=lang)
                    amount_label = tr("amount_label", lang=lang)
                    if paid_label and amount_label:
                        body = body.replace(f"{amount_label}:", f"{paid_label}:", 1)
                except Exception:
                    pass
                if discount_line:
                    body = f"{body}\n{discount_line}".strip()
            if event_type == "no_show" and no_show_count_recent is not None:
                try:
                    stats_tpl = tr("no_show_stats_line", lang=lang)
                    if stats_tpl:
                        stats_line = stats_tpl.format(count=no_show_count_recent)
                        body = f"{body}\n\n{stats_line}".strip()
                except Exception:
                    pass
            reply_kb = None
            if event_type == "done":
                try:
                    from bot.app.telegram.client.client_keyboards import build_rating_keyboard
                    if getattr(bd, "client_telegram_id", None) and int(getattr(bd, "client_telegram_id")) == int(rid_int):
                        reply_kb = build_rating_keyboard(int(booking_id))
                        # Use localized prompt for rating instead of hardcoded text
                        try:
                            prompt = tr("rate_prompt_title", lang=lang)
                        except Exception:
                            prompt = "Please rate your visit:"
                        body = f"{body}\n\n{prompt}"
                except Exception:
                    reply_kb = None

            try:
                await bot.send_message(
                    chat_id=rid_int,
                    text=f"{title}\n\n{body}".strip(),
                    reply_markup=reply_kb,
                    parse_mode="HTML",
                )
                logger.info("send_booking_notification: sent to %s", rid_int)
            except Exception as se:
                logger.warning("Failed to send notification to %s: %s", rid_int, se)
    except Exception as e:
        logger.exception("send_booking_notification failed: %s", e)
