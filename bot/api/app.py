"""Minimal FastAPI facade for Telegram Mini App.

This layer wraps existing Aiogram services without touching business logic.
Expose small REST endpoints for the WebApp UI while relying on existing DB/services.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import urllib.parse
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any, Dict, Optional
from enum import Enum

import jwt
from aiogram import Bot
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from fastapi.staticfiles import StaticFiles

# Centralized business logic helpers (booking, pricing, etc.)
from bot.app.services import client_services

from bot.app.domain.models import BookingStatus, normalize_booking_status


from bot.app.core.constants import BOT_TOKEN, TELEGRAM_PROVIDER_TOKEN
from bot.app.services.shared_services import (
    get_telegram_provider_token,
    is_online_payments_available,
    get_admin_ids,
    safe_get_locale,
    status_to_emoji,
    format_money_cents,
    get_contact_info,
)
from bot.app.telegram.common.status import get_status_label
from bot.app.core.db import get_session
from bot.app.services.admin_services import ServiceRepo
from bot.app.services.client_services import UserRepo
from bot.app.services.master_services import MasterRepo
from bot.app.services.client_services import (
    BookingResult,
    BookingRepo,
    can_client_reschedule,
    get_available_days_for_month,
    format_booking_details_text,
    get_local_tz,
    get_services_duration_and_price,
    process_booking_hold,
    process_booking_rating,
    process_booking_cancellation,
    process_booking_reschedule,
    process_booking_finalization,
    process_invoice_link,
    process_booking_details,
)
from bot.app.services.shared_services import get_service_duration
from bot.app.core.notifications import send_booking_notification
from bot.app.services.admin_services import SettingsRepo

logger = logging.getLogger(__name__)

# JWT settings
JWT_SECRET = os.getenv("TWA_JWT_SECRET") or BOT_TOKEN
JWT_ALGO = "HS256"
JWT_TTL_SECONDS = int(os.getenv("TWA_JWT_TTL_SECONDS", "3600"))

# Allowed origins for Telegram clients; adjust to include your domain
_raw_origins = [
    "https://web.telegram.org",
    "https://telegram.org",
    "https://t.me",
    "https://tg.dev",
    os.getenv("TWA_ORIGIN"),
]
ALLOW_ALL_ORIGINS = os.getenv("TWA_ALLOW_ALL_ORIGINS", "false").lower() in {"1", "true", "yes"}
ALLOWED_ORIGINS = [o for o in _raw_origins if o]


class SessionRequest(BaseModel):
    init_data: str = Field(..., alias="initData")


class TelegramUser(BaseModel):
    id: int
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    username: Optional[str] = None


class SessionResponse(BaseModel):
    token: str
    user: TelegramUser
    currency: Optional[str] = None
    locale: Optional[str] = None
    webapp_title: Optional[str] = None
    online_payments_available: Optional[bool] = None
    reminder_lead_minutes: Optional[int] = None
    # optional contact/address provided by admin
    address: Optional[str] = None
    webapp_address: Optional[str] = None


class BookingRequest(BaseModel):
    service_ids: list[str] = Field(..., min_length=1)
    slot: datetime
    master_id: Optional[int] = None
    payment_method: Optional[str] = None


class BookingResponse(BaseModel):
    ok: bool
    booking_id: Optional[int] = None
    status: Optional[str] = None
    starts_at: Optional[str] = None
    cash_hold_expires_at: Optional[str] = None
    original_price_cents: Optional[int] = None
    final_price_cents: Optional[int] = None
    discount_amount_cents: Optional[int] = None
    currency: Optional[str] = None
    master_id: Optional[int] = None
    master_name: Optional[str] = None
    payment_method: Optional[str] = None
    invoice_url: Optional[str] = None
    duration_minutes: Optional[int] = None
    text: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Error handling helpers
# ---------------------------------------------------------------------------

def _normalize_error_code(val: str | Exception | None, default: str) -> str:
    """Return a safe error code for frontend without leaking exception text."""
    if val is None:
        return default
    try:
        code = str(val).strip().lower()
    except Exception:
        return default
    if not code:
        return default
    if not all(ch.isalnum() or ch in {"_", "-"} for ch in code):
        return default
    return code[:64]


def booking_error_handler(default_error: str):
    """Decorator to de-duplicate try/except in booking endpoints.

    - Passes through FastAPI `HTTPException` untouched.
    - Converts business `ValueError` to a BookingResponse with the message.
    - Logs unexpected exceptions and returns a unified error code.
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except HTTPException:
                raise
            except ValueError as exc:
                code = _normalize_error_code(exc, default_error)
                return BookingResponse(ok=False, error=code)
            except Exception as exc:  # noqa: BLE001 – catch-all is intentional for API boundary
                logger.exception("%s failed: %s", func.__name__, exc)
                return BookingResponse(ok=False, error=default_error)

        return wrapper

    return decorator


class BookingItemOut(BaseModel):
    id: int
    status: str
    status_label: Optional[str] = None
    status_emoji: Optional[str] = None
    price_cents: Optional[int] = None
    price_formatted: Optional[str] = None
    currency: Optional[str] = None
    starts_at: Optional[str] = None
    ends_at: Optional[str] = None
    master_id: Optional[int] = None
    master_name: Optional[str] = None
    service_names: Optional[str] = None
    payment_method: Optional[str] = None
    can_cancel: bool = False
    can_reschedule: bool = False


class SlotsResponse(BaseModel):
    slots: list[str]
    timezone: Optional[str] = None


class AvailableDaysResponse(BaseModel):
    days: list[int]
    timezone: Optional[str] = None


class PriceQuoteRequest(BaseModel):
    service_ids: list[str] = Field(..., min_length=1)
    class PaymentMethod(str, Enum):
        cash = "cash"
        online = "online"

    payment_method: PaymentMethod = PaymentMethod.cash
    master_id: Optional[int] = None


class PriceQuoteResponse(BaseModel):
    final_price_cents: int
    original_price_cents: Optional[int] = None
    currency: str
    discount_percent_applied: Optional[float] = None
    duration_minutes: Optional[int] = None


class ServiceOut(BaseModel):
    id: str
    name: str
    duration_minutes: Optional[int] = None
    price_cents: Optional[int] = None


class MasterOut(BaseModel):
    id: int
    name: str


class MastersMatchRequest(BaseModel):
    service_ids: list[str] = Field(..., min_length=1)


class MasterProfileOut(MasterOut):
    telegram_id: Optional[int] = None
    bio: Optional[str] = None
    rating: Optional[float] = None
    ratings_count: Optional[int] = None
    completed_orders: Optional[int] = None
    title: Optional[str] = None
    experience_years: Optional[int] = None
    specialities: Optional[list[str]] = None
    avatar_url: Optional[str] = None
    services: Optional[list[dict[str, Any]]] = None
    schedule_lines: Optional[list[str]] = None


class MeOut(BaseModel):
    id: int
    telegram_id: int
    username: Optional[str]
    name: Optional[str]
    locale: Optional[str]


class Principal(BaseModel):
    user_id: int
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    language: Optional[str] = None


class RatingRequest(BaseModel):
    booking_id: int
    rating: int = Field(..., ge=1, le=5)


class PayRequest(BaseModel):
    booking_id: int


class CancelRequest(BaseModel):
    booking_id: int


class RescheduleRequest(BaseModel):
    booking_id: int
    new_slot: datetime


class InvoiceRequest(BaseModel):
    booking_id: int


# ---------------------------------------------------------------------------
# Security helpers
# ---------------------------------------------------------------------------

def _parse_init_data(init_data: str) -> Dict[str, str]:
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_init_data_format") from exc
    if "hash" not in parsed:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="missing_hash")
    return parsed


def _calc_expected_hash(data: Dict[str, str], token: str) -> str:
    check_hash = data.get("hash", "")
    data_to_check = {k: v for k, v in data.items() if k != "hash"}
    data_check_string = "\n".join(f"{k}={data_to_check[k]}" for k in sorted(data_to_check))
    secret_key = hmac.new("WebAppData".encode(), token.encode(), hashlib.sha256).digest()
    computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    logger.debug("initData data_check_string=%s computed_hash=%s", data_check_string, computed)
    if not hmac.compare_digest(computed, check_hash):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_init_data_signature")
    return computed


def validate_init_data(init_data: str) -> TelegramUser:
    parsed = _parse_init_data(init_data)
    _calc_expected_hash(parsed, BOT_TOKEN)

    try:
        user_raw = parsed.get("user")
        user_payload = json.loads(user_raw) if user_raw else None
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid_user_payload") from exc

    if not isinstance(user_payload, dict) or "id" not in user_payload:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="missing_user")

    try:
        auth_date_raw = parsed.get("auth_date")
        if auth_date_raw:
            auth_ts = int(auth_date_raw)
            if auth_ts < int(datetime.now(UTC).timestamp()) - 86400:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="stale_init_data")
    except HTTPException:
        raise
    except Exception:
        pass

    user_id_raw = user_payload.get("id")
    if user_id_raw is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="missing_user_id")

    return TelegramUser(
        id=int(user_id_raw),
        first_name=user_payload.get("first_name"),
        last_name=user_payload.get("last_name"),
        username=user_payload.get("username"),
    )


def issue_jwt(user_id: int, tg_user: TelegramUser) -> str:
    payload = {
        "sub": str(user_id),
        "tg_id": int(tg_user.id),
        "username": tg_user.username,
        "first_name": tg_user.first_name,
        "exp": datetime.now(UTC) + timedelta(seconds=JWT_TTL_SECONDS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def _decode_token(token: str) -> Principal:
    try:
        data = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="token_expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_token") from exc

    return Principal(
        user_id=int(data.get("sub")),
        telegram_id=int(data.get("tg_id")),
        username=data.get("username"),
        first_name=data.get("first_name"),
    )


async def _safe_service_repo_call(method_name: str, *args, **kwargs):
    """Call a ServiceRepo method by name safely for type-checker friendliness.

    Returns None on any error or if method is missing.
    """
    try:
        method = getattr(ServiceRepo, method_name)
    except Exception:
        return None
    try:
        return await method(*args, **kwargs)
    except Exception:
        return None


async def get_current_principal(
    authorization: str | None = Header(default=None),
    x_twa_lang: str | None = Header(default=None, alias="X-TWA-Lang"),
    accept_language: str | None = Header(default=None, alias="Accept-Language"),
    lang_q: str | None = Query(default=None, alias="lang"),
) -> Principal:
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing_authorization")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid_authorization_header")

    principal = _decode_token(token)

    # Derive preferred language: header X-TWA-Lang > query param lang > Accept-Language
    lang = None
    if x_twa_lang:
        lang = x_twa_lang
    elif lang_q:
        lang = lang_q
    elif accept_language:
        # Accept-Language may contain 'en-US,en;q=0.9' — take first part
        try:
            lang = accept_language.split(",")[0].strip()
        except Exception:
            lang = accept_language

    principal.language = lang
    return principal


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="SalonBot TMA API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if ALLOW_ALL_ORIGINS else ALLOWED_ORIGINS,
    allow_credentials=not ALLOW_ALL_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/session", response_model=SessionResponse)
async def create_session(payload: SessionRequest) -> SessionResponse:
    tg_user = validate_init_data(payload.init_data)

    # Ensure user exists in DB, reuse existing service layer
    user = await UserRepo.get_or_create(
        telegram_id=int(tg_user.id),
        name=tg_user.first_name or tg_user.username,
        username=tg_user.username,
    )
    token = issue_jwt(user_id=int(user.id), tg_user=tg_user)
    # Provide authoritative runtime settings to the WebApp: currency and locale
    try:
        currency = await SettingsRepo.get_currency()
    except Exception as exc:
        logger.exception("Failed to get currency setting: %s", exc)
        currency = None

    try:
        locale = await safe_get_locale(int(tg_user.id))
    except Exception as exc:
        logger.exception("Failed to resolve user locale: %s", exc)
        locale = None

    try:
        online_payments_available = await is_online_payments_available()
    except Exception as exc:
        logger.exception("Failed to determine online payment availability: %s", exc)
        online_payments_available = None

    try:
        title = await SettingsRepo.get_setting("webapp_title", "Telegram Mini App • Beauty")
        # If stored as JSON/string, coerce
        if isinstance(title, str):
            webapp_title = title
        else:
            webapp_title = str(title) if title is not None else None
    except Exception as exc:
        logger.exception("Failed to read webapp title setting: %s", exc)
        webapp_title = "Telegram Mini App • Beauty"

    try:
        reminder_lead = await SettingsRepo.get_reminder_lead_minutes()
    except Exception:
        reminder_lead = None

    # Try to include contact address/title for the WebApp (best-effort)
    try:
        contact = await get_contact_info()
        address_val = contact.get("address") if isinstance(contact, dict) else None
        webapp_address_val = address_val
    except Exception:
        address_val = None
        webapp_address_val = None

    return SessionResponse(
        token=token,
        user=tg_user,
        currency=currency,
        locale=locale,
        webapp_title=webapp_title,
        online_payments_available=online_payments_available,
        reminder_lead_minutes=reminder_lead,
        address=address_val,
        webapp_address=webapp_address_val,
    )


@app.get("/api/me", response_model=MeOut)
async def get_me(principal: Principal = Depends(get_current_principal)) -> MeOut:
    user = await UserRepo.get_by_id(principal.user_id)
    if not user:
        user = await UserRepo.get_or_create(principal.telegram_id, name=principal.first_name, username=principal.username)

    locale = None
    try:
        locale = await UserRepo.get_locale_by_telegram_id(principal.telegram_id)
    except Exception as exc:
        logger.exception("Failed to get user locale for %s: %s", principal.telegram_id, exc)
        locale = None

    return MeOut(
        id=int(getattr(user, "id", 0) or 0),
        telegram_id=principal.telegram_id,
        username=getattr(user, "username", None) or principal.username,
        name=getattr(user, "name", None) or principal.first_name,
        locale=locale,
    )


@app.get("/api/slots", response_model=SlotsResponse)
async def get_slots(
    master_id: int,
    date: str,  # YYYY-MM-DD
    service_ids: str = Query(...), # Приходит как "1,2,3"
):
    try:
        dt_obj = datetime.strptime(date, "%Y-%m-%d")
        ids_list = [sid.strip() for sid in service_ids.split(",") if sid.strip()]
        
        # Resolve total duration using the service-layer helper so all
        # duration/price logic stays in `client_services` (single source of truth).
        try:
            totals = await get_services_duration_and_price(ids_list, online_payment=False, master_id=master_id)
            total_minutes = int(totals.get("total_minutes") or 0)
        except Exception as exc:
            logger.exception("Failed to compute service durations for %s: %s", ids_list, exc)
            # Fallback to a sensible default slot duration if the service call fails
            try:
                total_minutes = int(await SettingsRepo.get_slot_duration())
            except Exception as exc2:
                logger.exception("Failed to read default slot duration: %s", exc2)
                total_minutes = 60

        # Call the canonical slot calculation with a single-element list
        # containing the aggregated total minutes (call sums service_durations).
        from bot.app.services.client_services import get_available_time_slots_for_services

        slots = await get_available_time_slots_for_services(
            date=dt_obj,
            master_id=master_id,
            service_durations=[total_minutes]
        )

        # Превращаем объекты time в строки "HH:MM"
        return SlotsResponse(
            slots=[s.strftime("%H:%M") for s in slots],
            timezone=str(get_local_tz())
        )
    except Exception as e:
        logger.exception("Ошибка при получении слотов для WebApp: %s", e)
        raise HTTPException(status_code=500, detail="slots_failed")


@app.get("/api/check_slot")
async def check_slot(
    master_id: int | None = Query(None),
    slot: datetime = Query(...),
    service_ids: list[str] = Query(..., alias="service_ids[]"),
    principal: Principal = Depends(get_current_principal),
) -> dict:
    """Return whether the given slot is currently available for booking.

    Uses existing BookingRepo logic so checks mirror bot behavior.
    """
    try:
        # compute total duration for requested services
        agg = await ServiceRepo.aggregate_services(service_ids)
        total_minutes = int(agg.get("total_minutes") or 60)
    except Exception as exc:
        logger.exception("Failed to aggregate services %s: %s", service_ids, exc)
        total_minutes = 60

    # Interpret incoming `slot` using salon local timezone when naive.
    if slot.tzinfo is None:
        try:
            local_tz = get_local_tz() or UTC
        except Exception as exc:
            logger.exception("Failed to determine local timezone: %s", exc)
            local_tz = UTC
        # Treat naive datetime as salon-local, then convert to UTC for repo checks
        slot = slot.replace(tzinfo=local_tz).astimezone(UTC)
    else:
        slot = slot.astimezone(UTC)

    new_start = slot
    new_end = new_start + timedelta(minutes=total_minutes)

    async with get_session() as session:
        conflict = await BookingRepo.find_conflicting_booking(
            session,
            None,
            master_id,
            new_start,
            new_end,
            service_ids=service_ids,
        )

    return {"available": conflict is None, "conflict": conflict}


@app.get("/api/available_days", response_model=AvailableDaysResponse)
async def available_days(
    master_id: int,
    year: int,
    month: int,
    service_ids: list[str] = Query(..., alias="service_ids[]"),
    principal: Principal = Depends(get_current_principal),
) -> AvailableDaysResponse:
    agg = await ServiceRepo.aggregate_services(service_ids)
    total_minutes = int(agg.get("total_minutes") or 60)
    days = await get_available_days_for_month(master_id, year, month, service_duration_min=total_minutes)
    try:
        tz = get_local_tz()
        tz_name = getattr(tz, "key", None) or str(tz)
    except Exception as exc:
        logger.exception("Failed to determine local timezone for available_days: %s", exc)
        tz_name = "UTC"
    return AvailableDaysResponse(days=sorted(days), timezone=tz_name)


@app.post("/api/hold", response_model=BookingResponse)
@booking_error_handler("booking_failed")
async def create_hold(
    payload: BookingRequest,
    principal: Principal = Depends(get_current_principal),
) -> BookingResponse:
    """Create a short-term reservation (hold) for the requested slot.

    The booking will be created with a cash hold that expires after
    `SettingsRepo.get_reservation_hold_minutes()` (best-effort). This mirrors
    the bot behaviour and allows the TMA to hold a slot while the user
    completes payment details.
    """
    res: BookingResult = await process_booking_hold(
        principal.user_id,
        principal.telegram_id,
        payload.service_ids,
        payload.slot,
        master_id=payload.master_id,
        payment_method=payload.payment_method,
        client_name=principal.first_name or principal.username,
        client_username=principal.username,
    )
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        status=res.get("status"),
        starts_at=res.get("starts_at"),
        cash_hold_expires_at=res.get("cash_hold_expires_at"),
        original_price_cents=res.get("original_price_cents"),
        final_price_cents=res.get("final_price_cents"),
        discount_amount_cents=res.get("discount_amount_cents"),
        currency=res.get("currency"),
        duration_minutes=res.get("duration_minutes"),
        master_id=res.get("master_id"),
        payment_method=res.get("payment_method"),
        error=res.get("error"),
    )


@app.get("/api/services", response_model=list[ServiceOut])
async def list_services(principal: Principal = Depends(get_current_principal)) -> list[ServiceOut]:
    try:
        async with get_session() as session:  # type: ignore  # re-use existing helper
            from sqlalchemy import select
            from bot.app.domain.models import Service

            rows = await session.execute(
                select(
                    Service.id,
                    Service.name,
                    Service.duration_minutes,
                    Service.price_cents,
                ).order_by(Service.id)
            )
            return [
                ServiceOut(
                    id=str(r[0]),
                    name=str(r[1]) if r[1] is not None else "",
                    duration_minutes=int(r[2]) if r[2] is not None else None,
                    price_cents=int(r[3]) if isinstance(r[3], int) else None,
                )
                for r in rows.all()
            ]
    except Exception as exc:
        logger.exception("list_services failed: %s", exc)
        raise HTTPException(status_code=500, detail="services_unavailable") from exc


@app.get("/api/masters", response_model=list[MasterOut])
async def list_masters(principal: Principal = Depends(get_current_principal)) -> list[MasterOut]:
    masters: list[tuple[int, str]] = await MasterRepo.get_masters_page(page=1, page_size=200)
    return [MasterOut(id=m[0], name=m[1]) for m in masters]


@app.get("/api/service_ranges")
async def service_ranges(service_ids: list[str] = Query(..., alias="service_ids[]"), principal: Principal = Depends(get_current_principal)) -> dict:
    """Return duration and price ranges for provided service ids.

    Response format: { service_id: { min_duration: int|null, max_duration: int|null, min_price_cents: int|null, max_price_cents: int|null } }
    Durations prefer MasterService.duration_minutes when present, falling back to Service.duration_minutes or slot duration.
    """
    try:
        async with get_session() as session:
            from sqlalchemy import select, func
            from bot.app.domain.models import MasterService, Service

            # Default slot duration
            try:
                from bot.app.services.admin_services import SettingsRepo

                default_slot = await SettingsRepo.get_slot_duration()
            except Exception as exc:
                logger.exception("Failed to read default slot duration: %s", exc)
                default_slot = 60

            # Compute durations per master/service using COALESCE(master.duration, service.duration, default_slot)
            # We'll query joined rows and aggregate min/max per service
            stmt = (
                select(
                    MasterService.service_id,
                    func.min(func.coalesce(MasterService.duration_minutes, Service.duration_minutes, default_slot)).label("min_dur"),
                    func.max(func.coalesce(MasterService.duration_minutes, Service.duration_minutes, default_slot)).label("max_dur"),
                    func.min(func.coalesce(Service.price_cents, 0)).label("min_price"),
                    func.max(func.coalesce(Service.price_cents, 0)).label("max_price"),
                )
                .join(Service, Service.id == MasterService.service_id)
                .where(MasterService.service_id.in_(service_ids))
                .group_by(MasterService.service_id)
            )
            res = await session.execute(stmt)
            rows = res.all()

            out: dict = {}
            found = {r[0]: r for r in rows}

            # Fill results for those with master entries
            for sid, min_dur, max_dur, min_price, max_price in rows:
                out[str(sid)] = {
                    "min_duration": int(min_dur) if min_dur is not None else None,
                    "max_duration": int(max_dur) if max_dur is not None else None,
                    "min_price_cents": int(min_price) if min_price is not None else None,
                    "max_price_cents": int(max_price) if max_price is not None else None,
                }

            # For services not present in master_services, fall back to Service table
            missing = [sid for sid in service_ids if sid not in found]
            if missing:
                stmt2 = select(Service.id, Service.duration_minutes, Service.price_cents).where(Service.id.in_(missing))
                res2 = await session.execute(stmt2)
                for sid, dur, price in res2.all():
                    d = int(dur) if dur is not None else default_slot
                    p = int(price) if price is not None else None
                    out[str(sid)] = {
                        "min_duration": d,
                        "max_duration": d,
                        "min_price_cents": p,
                        "max_price_cents": p,
                    }

            return out
    except Exception as exc:
        logger.exception("service_ranges failed: %s", exc)
        raise HTTPException(status_code=500, detail="service_ranges_failed") from exc


@app.post("/api/masters_match", response_model=list[MasterOut])
async def masters_match(payload: MastersMatchRequest, principal: Principal = Depends(get_current_principal)) -> list[MasterOut]:
    # Return masters who provide ALL requested services (intersection) in a single query
    if not payload.service_ids:
        return []
    try:
        async with get_session() as session:
            from sqlalchemy import select, func
            from bot.app.domain.models import Master, MasterService

            stmt = (
                select(Master.id, Master.name)
                .join(MasterService, MasterService.master_id == Master.id)
                .where(MasterService.service_id.in_(payload.service_ids))
                .group_by(Master.id, Master.name)
                .having(func.count(func.distinct(MasterService.service_id)) == len(payload.service_ids))
                .order_by(Master.name)
            )
            rows = (await session.execute(stmt)).all()
            return [MasterOut(id=int(r[0]), name=str(r[1]) if r[1] is not None else "") for r in rows]
    except Exception as exc:
        logger.exception("masters_match failed for %s: %s", payload.service_ids, exc)
        raise HTTPException(status_code=500, detail="masters_unavailable") from exc


@app.get("/api/master_profile", response_model=MasterProfileOut)
async def master_profile(master_id: int, principal: Principal = Depends(get_current_principal)) -> MasterProfileOut:
    data = await MasterRepo.get_master_profile_data(master_id)
    if not data or not data.get("master"):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="master_not_found")

    master_obj = data.get("master")
    services = data.get("services") or []
    specialities: list[str] = []
    services_out: list[dict[str, Any]] = []
    try:
        for svc in services:
            # svc tuple: (id, name, category, price_cents, currency)
            name_val = svc[1] if isinstance(svc, (list, tuple)) and len(svc) > 1 else None
            sid_val = svc[0] if isinstance(svc, (list, tuple)) and len(svc) > 0 else None
            price_val = svc[3] if isinstance(svc, (list, tuple)) and len(svc) > 3 else None
            curr_val = svc[4] if isinstance(svc, (list, tuple)) and len(svc) > 4 else None
            if name_val:
                specialities.append(str(name_val))
            # Pick duration override if present
            try:
                durations_map = data.get("durations_map") or {}
                dur_val = durations_map.get(str(sid_val)) if durations_map else None
            except Exception as exc:
                logger.exception("Failed to read durations_map for master %s: %s", master_id, exc)
                dur_val = None
            services_out.append(
                {
                    "id": sid_val,
                    "name": name_val,
                    "price_cents": int(price_val) if price_val is not None else None,
                    "duration_minutes": int(dur_val) if dur_val is not None else None,
                    "currency": curr_val,
                }
            )
    except Exception:
        specialities = []
        services_out = []

    rating_val = data.get("rating")
    ratings_count_val = data.get("ratings_count")
    completed_orders_val = data.get("completed_orders")

    # Build schedule lines using MasterRepo to avoid inline SQL here
    schedule_lines: list[str] = []
    try:
        schedule = await MasterRepo.get_schedule(master_id) or {}
        weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for i in range(7):
            windows = schedule.get(str(i)) or []
            if not windows:
                schedule_lines.append(f"• {weekdays[i]}: —")
            else:
                joined = ", ".join([f"{w[0]}–{w[1]}" for w in windows])
                schedule_lines.append(f"• {weekdays[i]}: {joined}")
    except Exception as exc:
        logger.exception("Failed to build schedule lines for master %s: %s", master_id, exc)
        schedule_lines = []

    return MasterProfileOut(
        id=int(getattr(master_obj, "id", master_id) or master_id),
        name=str(getattr(master_obj, "name", "")),
        telegram_id=int(getattr(master_obj, "telegram_id", 0) or 0) if getattr(master_obj, "telegram_id", None) is not None else None,
        bio=data.get("about_text") or None,
        rating=float(rating_val) if rating_val is not None else None,
        ratings_count=int(ratings_count_val) if ratings_count_val is not None else None,
        completed_orders=int(completed_orders_val) if completed_orders_val is not None else None,
        title=None,
        experience_years=None,
        specialities=specialities or None,
        avatar_url=getattr(master_obj, "avatar_url", None),
        services=services_out or None,
        schedule_lines=schedule_lines or None,
    )


@app.get("/api/masters_for_service", response_model=list[MasterOut])
async def masters_for_service(service_id: str, principal: Principal = Depends(get_current_principal)) -> list[MasterOut]:
    masters = await MasterRepo.get_masters_for_service(service_id)
    return [MasterOut(id=int(getattr(m, "id", 0)), name=str(getattr(m, "name", ""))) for m in masters]


@app.post("/api/price_quote", response_model=PriceQuoteResponse)
async def price_quote(payload: PriceQuoteRequest, principal: Principal = Depends(get_current_principal)) -> PriceQuoteResponse:
    """Calculate final price on the server to keep Mini App and bot in sync."""
    try:
        base = await get_services_duration_and_price(payload.service_ids, online_payment=False, master_id=payload.master_id)
        online = (payload.payment_method or "cash") == "online"
        final = base
        if online:
            final = await get_services_duration_and_price(payload.service_ids, online_payment=True, master_id=payload.master_id)

        base_price = int(base.get("total_price_cents") or 0)
        final_price = int(final.get("total_price_cents") or 0)
        currency = str(final.get("currency") or base.get("currency") or "UAH")

        discount_percent: float | None = None
        if online and base_price > 0 and final_price < base_price:
            try:
                discount_percent = round(((base_price - final_price) * 100) / base_price, 2)
            except Exception:
                discount_percent = None

        duration = None
        try:
            duration = int(base.get("total_minutes") or 0)
        except Exception:
            duration = None

        return PriceQuoteResponse(
            final_price_cents=final_price,
            original_price_cents=base_price,
            currency=currency,
            discount_percent_applied=discount_percent,
            duration_minutes=duration,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("price_quote failed for %s: %s", payload.service_ids, exc)
        raise HTTPException(status_code=500, detail="price_quote_failed") from exc


@app.get("/api/bookings", response_model=list[BookingItemOut])
async def list_bookings(
    principal: Principal = Depends(get_current_principal),
    mode: str = Query("upcoming", regex="^(upcoming|history)$"),
) -> list[BookingItemOut]:

    # Use repository helper that implements correct SQL-level filtering for
    # active/upcoming bookings (includes RESERVED/PENDING_PAYMENT and
    # performs time comparison in DB to avoid precision races).
    if mode == "upcoming":
        bookings = await BookingRepo.list_active_by_user(int(principal.user_id))
    else:
        bookings = await BookingRepo.recent_by_user(principal.user_id, limit=50)

    # History should include only final states: cancelled, done/completed, and no-shows
    history_statuses = {"cancelled", "canceled", "done", "completed", "no_show"}
    upcoming_allowed_statuses = {"confirmed", "paid"}
    now_utc = datetime.now(UTC)
    result: list[BookingItemOut] = []

    from bot.app.domain.models import normalize_booking_status

    for b in bookings:
        # Normalize enum/string status to canonical lowercase value (e.g. 'paid', 'confirmed')
        raw_status_obj = getattr(b, "status", None)
        try:
            norm_status = normalize_booking_status(raw_status_obj)
            status_raw = norm_status.value if norm_status is not None else (str(raw_status_obj).lower() if raw_status_obj is not None else "")
        except Exception:
            status_raw = str(raw_status_obj).lower() if raw_status_obj is not None else ""
        try:
            status_label_raw = await get_status_label(raw_status_obj, lang=principal.language)
        except Exception:
            status_label_raw = status_raw
        try:
            status_emoji = status_to_emoji(raw_status_obj)
        except Exception:
            status_emoji = ""
        starts_at = getattr(b, "starts_at", None)

        if starts_at and starts_at.tzinfo is None:
            starts_at = starts_at.replace(tzinfo=UTC)

        # Filtering: when using the repository helper for `upcoming` we
        # rely on the DB-level filter; only keep legacy history logic here.
        if mode != "upcoming":  # history
            if starts_at and starts_at > now_utc and status_raw not in history_statuses:
                continue

        # ===== СЕРВИСЫ / ИМЕНА =====
        try:
            from bot.app.services.shared_services import booking_info_from_mapping
            info = booking_info_from_mapping({
                "booking_id": getattr(b, "id", None),
                "service_name": getattr(getattr(b, "service", None), "name", None),
                "master_name": getattr(getattr(b, "master", None), "name", None),
                "status": getattr(b, "status", None),
                "starts_at": getattr(b, "starts_at", None),
                "ends_at": getattr(b, "ends_at", None),
                "final_price_cents": getattr(b, "final_price_cents", None),
                "original_price_cents": getattr(b, "original_price_cents", None),
            })
            service_names = info.service_name if info and info.service_name else None
            # If service_name is missing (composite bookings), fetch composed service names from booking_items
            if not service_names:
                try:
                    async with get_session() as session:
                        from sqlalchemy import select
                        from bot.app.domain.models import BookingItem, Service

                        rows = await session.execute(
                            select(Service.name).join(BookingItem, BookingItem.service_id == Service.id).where(BookingItem.booking_id == int(getattr(b, "id", 0))).order_by(BookingItem.position)
                        )
                        names = [r[0] for r in rows.all() if r and r[0]]
                        if names:
                            service_names = ", ".join([str(n) for n in names])
                except Exception as exc:
                    logger.exception("Failed to load composite service names for booking %s: %s", getattr(b, 'id', None), exc)
        except Exception as exc:
            logger.exception("Failed to derive service names for booking %s: %s", getattr(b, 'id', None), exc)
            service_names = None

        # ===== ПРАВА =====
        # Calculate reschedule and cancel independently so a tight reschedule lock
        # (e.g. 24h) does not hide a looser cancel window (e.g. 30m).
        can_cancel = False
        can_reschedule = False
        lock_r = await _safe_service_repo_call("get_client_reschedule_lock_minutes") or None
        lock_c = await _safe_service_repo_call("get_client_cancel_lock_minutes") or None

        # Authoritative reschedule check (ownership + status + lock).
        try:
            can_res, _reason = await can_client_reschedule(int(getattr(b, "id", 0) or 0), principal.telegram_id)
            can_reschedule = bool(can_res)
        except Exception:
            can_reschedule = False

        # Independent cancel/reschedule calc using lock windows; keeps cancel available
        # even when reschedule is forbidden but cancel window is still open.
        try:
            can_cancel_calc, can_reschedule_calc = await client_services.calculate_booking_permissions(
                b,
                lock_r_minutes=lock_r,
                lock_c_minutes=lock_c,
                settings=None,
            )
            can_cancel = bool(can_cancel_calc)
            if not can_reschedule:
                can_reschedule = bool(can_reschedule_calc)
        except Exception as exc:
            logger.exception("Permission calculation failed for booking %s: %s", getattr(b, 'id', None), exc)
            can_cancel = can_reschedule = False

        ends_at = getattr(b, "ends_at", None)
        master_id = getattr(b, "master_id", None)
        # master name (prefer joined relation if available)
        try:
            master_name_val = getattr(getattr(b, "master", None), "name", None)
            if not master_name_val and master_id is not None:
                try:
                    master_name_val = await MasterRepo.get_master_name(master_id)
                except Exception as exc:
                    logger.exception("Failed to fetch master name for master_id %s: %s", master_id, exc)
                    master_name_val = None
        except Exception as exc:
            logger.exception("Failed to resolve master name for booking %s: %s", getattr(b, 'id', None), exc)
            master_name_val = None

        # price snapshot: prefer final_price_cents then original_price_cents
        try:
            price_val = getattr(b, "final_price_cents", None) or getattr(b, "original_price_cents", None)
            price_val = int(price_val) if price_val is not None else None
        except Exception as exc:
            logger.exception("Failed to parse price for booking %s: %s", getattr(b, 'id', None), exc)
            price_val = None
        try:
            currency_val = getattr(b, "currency", None)
        except Exception as exc:
            logger.exception("Failed to read currency for booking %s: %s", getattr(b, 'id', None), exc)
            currency_val = None
        try:
            price_fmt = format_money_cents(price_val, currency_val) if price_val is not None else None
        except Exception as exc:
            logger.exception("Failed to format price for booking %s: %s", getattr(b, 'id', None), exc)
            price_fmt = None

        # payment method inference: prefer explicit payment_provider/paid_at -> online, else cash
        try:
            pm = None
            if getattr(b, "payment_provider", None) or getattr(b, "paid_at", None) is not None or getattr(b, "payment_id", None):
                pm = "online"
            else:
                pm = "cash"
        except Exception as exc:
            logger.exception("Failed to infer payment method for booking %s: %s", getattr(b, 'id', None), exc)
            pm = None

        result.append(
            BookingItemOut(
                id=int(getattr(b, "id", 0) or 0),
                status=str(getattr(b, "status", "")),
                status_label=(status_label_raw or "") or None,
                status_emoji=status_emoji or None,
                price_cents=price_val,
                price_formatted=price_fmt,
                currency=currency_val or None,
                starts_at=starts_at.isoformat() if starts_at else None,
                ends_at=ends_at.isoformat() if ends_at else None,
                master_id=int(master_id) if master_id is not None else None,
                master_name=master_name_val,
                service_names=service_names,
                payment_method=pm,
                can_cancel=bool(can_cancel),
                can_reschedule=bool(can_reschedule),
            )
        )

    # 🧭 СОРТИРОВКА
    if mode == "upcoming":
        result.sort(key=lambda r: r.starts_at or "")
    else:
        result.sort(key=lambda r: r.starts_at or "", reverse=True)

    return result



@app.post("/api/cancel", response_model=BookingResponse)
@booking_error_handler("cancel_failed")
async def cancel_booking(payload: CancelRequest, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    res: BookingResult = await process_booking_cancellation(principal.user_id, principal.telegram_id, payload.booking_id)
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        status=res.get("status"),
        error=res.get("error"),
    )


@app.post("/api/reschedule", response_model=BookingResponse)
@booking_error_handler("reschedule_failed")
async def reschedule_booking(payload: RescheduleRequest, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    res: BookingResult = await process_booking_reschedule(
        principal.user_id,
        principal.telegram_id,
        payload.booking_id,
        payload.new_slot,
        language=principal.language,
    )
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        status=res.get("status"),
        error=res.get("error"),
    )


@app.post("/api/book", response_model=BookingResponse)
@booking_error_handler("booking_failed")
async def create_booking(
    payload: BookingRequest,
    principal: Principal = Depends(get_current_principal),
) -> BookingResponse:
    payment_method = payload.payment_method or "cash"
    requested_master_id = payload.master_id

    # Ensure the user exists (id embedded in JWT is authoritative)
    user = await UserRepo.get_by_id(principal.user_id)
    if not user:
        user = await UserRepo.get_or_create(
            telegram_id=principal.telegram_id,
            name=principal.first_name or principal.username,
            username=principal.username,
        )

    slot = payload.slot
    # Interpret naive datetimes as salon-local timezone then convert to UTC
    if slot.tzinfo is None:
        try:
            local_tz = get_local_tz() or UTC
        except Exception as exc:
            logger.exception("Failed to determine local timezone for booking: %s", exc)
            local_tz = UTC
        slot = slot.replace(tzinfo=local_tz).astimezone(UTC)
    else:
        slot = slot.astimezone(UTC)

    # Require explicit master selection from the client. Do not auto-pick any master.
    master_id = requested_master_id
    try:
        if master_id is None:
            return BookingResponse(ok=False, error="master_required")
        master_id = int(master_id)
        if master_id < 0:
            return BookingResponse(ok=False, error="master_required")
    except Exception as exc:
        logger.exception("Invalid master_id provided for booking: %s", exc)
        return BookingResponse(ok=False, error="master_required")

    totals = await client_services.get_services_duration_and_price(payload.service_ids, online_payment=False, master_id=master_id)
    total_duration_minutes = int(totals.get("total_minutes") or 0)
    if total_duration_minutes <= 0:
        total_duration_minutes = 60 * max(1, len(payload.service_ids))

    if len(payload.service_ids) == 1:
        booking = await client_services.create_booking(
            client_id=int(user.id),
            master_id=master_id,
            service_id=payload.service_ids[0],
            slot=slot,
            hold_minutes=None,
        )
    else:
        booking = await client_services.create_composite_booking(
            client_id=int(user.id),
            master_id=master_id,
            service_ids=payload.service_ids,
            slot=slot,
            hold_minutes=None,
        )
    booking_id = int(getattr(booking, "id", 0) or 0)
    starts_at_val = getattr(booking, "starts_at", None)
    status_val = str(getattr(booking, "status", ""))

    # Align web flow with bot inline flow:
    # - online: move to pending_payment to block slot and await provider payment
    # - cash: confirm immediately if slot still valid
    invoice_url: str | None = None
    if payment_method == "online":
        ok_paid, reason = await BookingRepo.mark_paid(booking_id)
        if ok_paid:
            status_val = "paid"
        else:
            return BookingResponse(ok=False, error=reason or "booking_failed")
    else:  # cash
        ok_cash, reason = await BookingRepo.confirm_cash(booking_id)
        if ok_cash:
            status_val = "confirmed"
        else:
            return BookingResponse(ok=False, error=reason or "booking_failed")

    # Notify admins + master (if present) about the new booking
    try:
        recipients: list[int] = []
        master_rec = getattr(booking, "master_id", None)
        if master_rec is not None:
            try:
                recipients.append(int(master_rec))
            except Exception as exc:
                logger.exception("Invalid master id %s in recipients for booking %s: %s", master_rec, booking_id, exc)
        try:
            recipients.extend(get_admin_ids())
        except Exception as exc:
            logger.exception("Failed to fetch admin ids for notifications: %s", exc)
        if recipients:
            bot = Bot(BOT_TOKEN)
            event = "paid" if payment_method == "online" else "cash_confirmed"

            # Send notifications to admins/masters (use centralized helper)
            try:
                await send_booking_notification(bot, booking_id, event, recipients)
            except Exception:
                logger.exception("booking notification failed for booking=%s", booking_id)

            # Best-effort: also send a confirmation message to the client to preserve chat history
            try:
                from bot.app.services.client_services import build_booking_details
                from bot.app.services.shared_services import format_booking_details_text, safe_get_locale
            except Exception as exc:
                logger.exception("Failed to import booking detail builders for booking notification: %s", exc)
                build_booking_details = None

            if build_booking_details is not None:
                try:
                    lang = principal.language if getattr(principal, "language", None) else await safe_get_locale(principal.telegram_id)
                    bd = await build_booking_details(booking, user_id=principal.telegram_id, lang=lang)
                    body = format_booking_details_text(bd, lang=lang)
                    try:
                        await bot.send_message(chat_id=principal.telegram_id, text=body, parse_mode="HTML")
                    except Exception as exc:
                        logger.exception("Failed to send booking confirmation to client %s: %s", principal.telegram_id, exc)
                except Exception:
                    pass

            try:
                await bot.session.close()
            except Exception as exc:
                logger.exception("Failed to close bot session after booking notification: %s", exc)
    except Exception:
        logger.exception("booking notification block failed for booking=%s", booking_id)

    try:
        master_name_val = None
        if master_id is not None:
            master_name_val = await MasterRepo.get_master_name(master_id)
        if not master_name_val:
            master_obj = getattr(booking, "master", None)
            master_name_val = getattr(master_obj, "name", None)
    except Exception as exc:
        logger.exception("Failed to resolve master name for booking %s: %s", booking_id, exc)
        master_name_val = None

    return BookingResponse(
        ok=True,
        booking_id=booking_id,
        status=status_val,
        starts_at=starts_at_val.isoformat() if starts_at_val else None,
        master_id=master_id,
        master_name=master_name_val,
        payment_method=payment_method,
        invoice_url=invoice_url,
        duration_minutes=total_duration_minutes,
    )


@app.post("/api/finalize", response_model=BookingResponse)
@booking_error_handler("finalize_failed")
async def finalize_booking(payload: dict, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    """Finalize an existing draft booking created with a hold.

    Expects JSON: { booking_id: int, payment_method: "cash"|"online" }
    """
    try:
        booking_id = int(payload.get("booking_id") or 0)
        payment_method = str(payload.get("payment_method") or "cash")
    except Exception as exc:
        raise ValueError("invalid_payload") from exc

    res: BookingResult = await process_booking_finalization(principal.user_id, principal.telegram_id, booking_id, payment_method)
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        status=res.get("status"),
        starts_at=res.get("starts_at"),
        invoice_url=res.get("invoice_url"),
        error=res.get("error"),
    )


@app.post("/api/create_invoice", response_model=BookingResponse)
@booking_error_handler("invoice_failed")
async def create_invoice(payload: InvoiceRequest, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    """Create a Telegram invoice link for an existing booking and return it to the WebApp.

    Frontend should call `webApp.openInvoice(invoice_url)` with the returned link.
    """
    booking_id = int(payload.booking_id or 0)
    res: BookingResult = await process_invoice_link(principal.user_id, booking_id)
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        invoice_url=res.get("invoice_url"),
        error=res.get("error"),
    )


@app.get("/api/booking_details", response_model=BookingResponse)
@booking_error_handler("details_failed")
async def booking_details(booking_id: int, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    res: BookingResult = await process_booking_details(principal.user_id, booking_id)
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        text=res.get("text"),
        error=res.get("error"),
    )


@app.post("/api/rate", response_model=BookingResponse)
@booking_error_handler("rating_failed")
async def rate_booking(payload: RatingRequest, principal: Principal = Depends(get_current_principal)) -> BookingResponse:
    res: BookingResult = await process_booking_rating(principal.user_id, payload.booking_id, payload.rating)
    return BookingResponse(
        ok=bool(res.get("ok")),
        booking_id=res.get("booking_id"),
        error=res.get("error"),
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def get_app() -> FastAPI:
    """Exported factory for uvicorn or tests."""
    return app

# ---------------------------------------------------------------------------
# Serve WebApp (production build)
# ---------------------------------------------------------------------------

WEB_DIR = os.getenv("TWA_WEB_DIR", "/app/web")

if os.path.isdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
else:
    logger.warning("Web directory not found: %s", WEB_DIR)
