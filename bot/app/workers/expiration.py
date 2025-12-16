"""Background worker to expire overdue booking holds.

Runs a periodic task that marks bookings as EXPIRED when their cash_hold_expires_at
deadline has passed and the status is still RESERVED or PENDING_PAYMENT.

start_expiration_worker returns an async callable that stops the worker gracefully.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import update, select

from bot.app.core.db import get_session
from bot.app.services.shared_services import get_env_int as _get_env_int, get_admin_ids, utc_now
from bot.app.domain.models import Booking, BookingStatus
from aiogram import Bot
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


"""Use get_env_int from shared_services; local implementation removed."""


async def _expire_once(now_utc: datetime) -> int:
    try:
        async with get_session() as session:
            hold_minutes = _get_env_int("RESERVATION_HOLD_MINUTES", 5)
            # Find candidate bookings that should expire and group them by
            # (master_id, starts_at). For each group acquire an advisory lock
            # on that pair and then perform the update. This prevents races
            # where a concurrent creator inserts a RESERVED/PENDING_PAYMENT
            # booking for the same slot while we're expiring another.
            result = await session.execute(
                select(Booking.id, Booking.master_id, Booking.starts_at).where(
                    Booking.status.in_([BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT]),
                    Booking.cash_hold_expires_at.is_not(None),
                    Booking.cash_hold_expires_at <= now_utc,
                )
            )
            rows_all = result.fetchall()
            groups: dict[tuple[int, object], list[int]] = {}
            for bid, mid, starts in rows_all:
                key = (int(mid), starts)
                groups.setdefault(key, []).append(int(bid))

            count = 0
            for (mid, starts), ids in groups.items():
                try:
                    from sqlalchemy import text
                    k1 = int(mid) % 2147483647
                    k2 = int(starts.timestamp()) % 2147483647 if hasattr(starts, 'timestamp') else 0
                    await session.execute(text("SELECT pg_advisory_xact_lock(:k1, :k2)"), {"k1": k1, "k2": k2})
                except Exception:
                    # best-effort advisory lock
                    pass

                stmt_upd = (
                    update(Booking)
                    .where(
                        Booking.master_id == mid,
                        Booking.starts_at == starts,
                        Booking.status.in_([BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT]),
                    )
                    .values(status=BookingStatus.EXPIRED, cash_hold_expires_at=None)
                    .returning(Booking.id)
                )
                res = await session.execute(stmt_upd)
                got = res.fetchall()
                if got:
                    logger.debug("Expired bookings for master=%s starts_at=%s: %s", mid, starts, [r[0] for r in got])
                count += len(got)

            stmt_no_hold = (
                update(Booking)
                .where(
                    Booking.status.in_([BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT]),
                    Booking.cash_hold_expires_at.is_(None),
                    Booking.created_at <= now_utc - timedelta(minutes=max(1, hold_minutes)),
                )
                .values(status=BookingStatus.EXPIRED)
                .returning(Booking.id)
            )
            result2 = await session.execute(stmt_no_hold)
            rows2 = result2.fetchall()
            count += len(rows2)
            logger.debug("Expired %d bookings without cash_hold_expires_at (based on created_at): %s", len(rows2), [row[0] for row in rows2])
            # Дополнительно: если created_at отсутствует (NULL) и нет удержания — тоже считаем просроченным
            stmt_no_hold_null_created = (
                update(Booking)
                .where(
                    Booking.status.in_([BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT]),
                    Booking.cash_hold_expires_at.is_(None),
                    Booking.created_at.is_(None),
                )
                .values(status=BookingStatus.EXPIRED)
                .returning(Booking.id)
            )
            result3 = await session.execute(stmt_no_hold_null_created)
            rows3 = result3.fetchall()
            count += len(rows3)
            if rows3:
                logger.debug("Expired %d bookings without cash_hold_expires_at and NULL created_at: %s", len(rows3), [row[0] for row in rows3])
            await session.commit()
            if count:
                logger.info("Expired %d overdue reservations/payments", count)
            return count
    except Exception as e:
        logger.error("Expiration sweep failed: %s", e)
        return 0


async def _run_loop(stop_event: asyncio.Event, interval_seconds: int | None) -> None:
    # initial small delay to avoid hammering immediately at startup
    try:
        await asyncio.sleep(2)
    except Exception:
        logger.exception("expiration: initial sleep interrupted")
    while not stop_event.is_set():
        try:
            await _expire_once(utc_now())
        except Exception as e:
            logger.exception("Expiration worker iteration error: %s", e)
        # Recompute interval each iteration from ENV so changes
        # take effect without restarting the process.
        cur_interval = _get_env_int("RESERVATION_EXPIRE_CHECK_SECONDS", 30)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cur_interval)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break


from typing import Awaitable, Callable, Optional


async def start_expiration_worker() -> Callable[[], Awaitable[None]]:
    """Start the expiration worker and return an async stop() function."""
    interval_seconds = _get_env_int("RESERVATION_EXPIRE_CHECK_SECONDS", 30)
    stop_event: asyncio.Event = asyncio.Event()
    # pass initial interval (kept for signature/backwards compat)
    task = asyncio.create_task(_run_loop(stop_event, interval_seconds), name="expire-worker")

    async def _stop() -> None:
        try:
            stop_event.set()
            try:
                await asyncio.wait_for(task, timeout=5)
            except Exception:
                task.cancel()
        except Exception:
            logger.exception("expiration: stop failed")

    logger.info("Expiration worker started (interval=%ss)", interval_seconds)
    return _stop


async def stop_expiration_worker(stop_callable: Optional[Callable[[], Awaitable[None]]] = None) -> None:
    """Compatibility helper: call provided stop callable if any."""
    if stop_callable:
        await stop_callable()




# Примерная логика для cleanup_worker.py
from datetime import datetime, timedelta, UTC
from sqlalchemy import select, update, or_


# Статусы, которые считаются "активными", но уже должны были завершиться
LIMBO_STATUSES = [
    BookingStatus.CONFIRMED,
    BookingStatus.PAID,
    BookingStatus.RESERVED,
    BookingStatus.PENDING_PAYMENT
]

# Как долго ждать после начала записи, прежде чем считать ее "неявкой"
# (Например, 2 часа, чтобы дать мастеру время закончить и нажать "Готово")
NO_SHOW_GRACE_PERIOD_HOURS = 2 

async def _cleanup_loop(stop_event: asyncio.Event, interval_seconds: int | None = None, bot: Bot | None = None) -> None:
    """Internal cleanup loop that marks long-past LIMBO bookings as NO_SHOW.

    This loop checks for limbo bookings every `interval_seconds` (default 900s)
    and respects `stop_event` for graceful shutdown.
    """
    check_interval_seconds = interval_seconds if interval_seconds is not None else _get_env_int("CLEANUP_CHECK_SECONDS", 900)

    logger.info("Запуск воркера очистки 'лимбо' записей...")
    while not stop_event.is_set():
        try:
            now = utc_now()

            # Ищем записи, которые начались более N часов назад и все еще "активны"
            cutoff_time = now - timedelta(hours=NO_SHOW_GRACE_PERIOD_HOURS)

            async with get_session() as session:
                # 1. Находим кандидатов на авто-неявку
                stmt = select(Booking.id).where(
                    Booking.starts_at < cutoff_time,
                    Booking.status.in_(LIMBO_STATUSES)
                )
                result = await session.execute(stmt)
                booking_ids_to_fail = result.scalars().all()

                if booking_ids_to_fail:
                    logger.info(f"Найдено {len(booking_ids_to_fail)} 'лимбо' записей. Обновление статуса на NO_SHOW...")

                    # 2. Обновляем их статус на NO_SHOW
                    update_stmt = update(Booking).where(
                        Booking.id.in_(booking_ids_to_fail)
                    ).values(
                        status=BookingStatus.NO_SHOW
                    )
                    await session.execute(update_stmt)
                    await session.commit()
                    logger.info(f"Обновлено {len(booking_ids_to_fail)} записей.")

                    # 3. Notify affected parties about NO_SHOW (if bot provided)
                    if bot is not None:
                        try:
                            from bot.app.core.notifications import send_booking_notification
                            from bot.app.services.master_services import MasterRepo
                            admins = get_admin_ids() or []
                            for bid in booking_ids_to_fail:
                                try:
                                    bd = await MasterRepo.get_booking_display_data(int(bid))
                                    client_tid = bd.get("client_telegram_id") if bd else None
                                    master_tid = bd.get("master_telegram_id") if bd else None
                                    recipients: list[int] = []
                                    if client_tid:
                                        try:
                                            recipients.append(int(client_tid))
                                        except Exception:
                                            pass
                                    if master_tid:
                                        try:
                                            recipients.append(int(master_tid))
                                        except Exception:
                                            pass
                                    for a in admins:
                                        try:
                                            recipients.append(int(a))
                                        except Exception:
                                            pass
                                    recipients = list(dict.fromkeys(recipients))
                                    if recipients:
                                        try:
                                            await send_booking_notification(bot, int(bid), "no_show", recipients)
                                        except Exception:
                                            logger.exception("Failed to send no-show notification for booking %s", bid)
                                except Exception:
                                    logger.exception("Failed to prepare/notify for booking %s", bid)
                        except Exception:
                            logger.exception("Failed to run NO_SHOW notifications loop")

        except Exception as e:
            logger.exception(f"Ошибка в cleanup loop: {e}")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=check_interval_seconds)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break




__all__ = ["start_expiration_worker", "stop_expiration_worker"]


async def start_cleanup_worker(bot: Bot | None = None) -> Callable[[], Awaitable[None]]:
    """Start the cleanup worker and return an async stop() function.

    Uses the same stop-event pattern as `start_expiration_worker` for
    graceful shutdown.
    """
    interval_seconds = _get_env_int("CLEANUP_CHECK_SECONDS", 900)
    stop_event: asyncio.Event = asyncio.Event()
    task = asyncio.create_task(_cleanup_loop(stop_event, interval_seconds, bot=bot), name="cleanup-worker")

    async def _stop() -> None:
        try:
            stop_event.set()
            try:
                await asyncio.wait_for(task, timeout=5)
            except Exception:
                task.cancel()
        except Exception:
            logger.exception("cleanup: stop failed")

    logger.info("Cleanup worker started (interval=%ss)", interval_seconds)
    return _stop


async def stop_cleanup_worker(stop_callable: Optional[Callable[[], Awaitable[None]]] = None) -> None:
    """Compatibility helper: call provided stop callable if any."""
    if stop_callable:
        await stop_callable()

__all__.extend(["start_cleanup_worker", "stop_cleanup_worker"])
