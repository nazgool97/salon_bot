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
from bot.app.domain.models import Booking, BookingStatus
import bot.config as cfg

logger = logging.getLogger(__name__)


async def _expire_once(now_utc: datetime) -> int:
    try:
        async with get_session() as session:
            hold_minutes = int(getattr(cfg, "get_hold_minutes", lambda: 1)())
            stmt = (
                update(Booking)
                .where(
                    Booking.status.in_([BookingStatus.RESERVED, BookingStatus.PENDING_PAYMENT]),
                    Booking.cash_hold_expires_at.is_not(None),
                    Booking.cash_hold_expires_at <= now_utc,
                )
                .values(status=BookingStatus.EXPIRED, cash_hold_expires_at=None)
                .returning(Booking.id)
            )
            result = await session.execute(stmt)
            rows = result.fetchall()
            count = len(rows)
            logger.debug("Expired %d bookings with cash_hold_expires_at: %s", count, [row[0] for row in rows])

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
        pass
    while not stop_event.is_set():
        try:
            await _expire_once(datetime.now(UTC))
        except Exception as e:
            logger.exception("Expiration worker iteration error: %s", e)
        # Recompute interval each iteration from runtime SETTINGS so changes
        # take effect without restarting the process.
        try:
            settings = getattr(cfg, "SETTINGS", {})
            cur_interval = int(settings.get("reservation_expire_check_seconds", 30))
        except Exception:
            cur_interval = 30
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cur_interval)
        except asyncio.TimeoutError:
            continue
        except Exception:
            break


from typing import Awaitable, Callable, Optional


async def start_expiration_worker() -> Callable[[], Awaitable[None]]:
    """Start the expiration worker and return an async stop() function."""
    # Allow configuration via SETTINGS; default: run every 30 seconds
    settings = getattr(cfg, "SETTINGS", {})
    interval_seconds = int(settings.get("reservation_expire_check_seconds", 30))
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
            pass

    logger.info("Expiration worker started (interval=%ss)", interval_seconds)
    return _stop


async def stop_expiration_worker(stop_callable: Optional[Callable[[], Awaitable[None]]] = None) -> None:
    """Compatibility helper: call provided stop callable if any."""
    if stop_callable:
        await stop_callable()

__all__ = ["start_expiration_worker", "stop_expiration_worker"]
