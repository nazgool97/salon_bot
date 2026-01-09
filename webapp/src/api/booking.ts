// src/api/booking.ts
import api from "./client";

// --- Types matching your Python Pydantic Models ---

export type ServiceOut = {
  id: string;
  name: string;
  description?: string | null;
  duration_minutes?: number | null;
  price_cents?: number | null;
};

export type MasterOut = {
  id: number;
  name: string;
};

export type MasterProfile = {
  id: number;
  name: string;
  telegram_id?: number | null;
  bio?: string | null;
  rating?: number | null;
  completed_orders?: number | null;
  experience_years?: number | null;
  title?: string | null;
  specialities?: string[];
  avatar_url?: string | null;
  services?: {
    id?: string | number | null;
    name?: string | null;
    price_cents?: number | null;
    duration_minutes?: number | null;
    currency?: string | null;
  }[];
  schedule_lines?: string[] | null;
};

export type SlotsResponse = {
  slots: string[];
  timezone?: string | null;
};

export type AvailableDaysResponse = {
  days: number[];
  timezone?: string | null;
};

export type PaymentMethod = "cash" | "online";

export type BookingRequest = {
  service_ids: string[]; // Pydantic: list[str], min_length=1
  // ISO datetime string accepted by the backend (FastAPI parses into datetime)
  slot: string;
  master_id?: number | null;
  payment_method?: PaymentMethod | null;
};

export type BookingResponse = {
  ok: boolean;
  booking_id?: number | null;
  status?: string | null;
  starts_at?: string | null;
  cash_hold_expires_at?: string | null;
  original_price_cents?: number | null;
  final_price_cents?: number | null;
  discount_amount_cents?: number | null;
  currency?: string | null;
  master_id?: number | null;
  master_name?: string | null;
  payment_method?: PaymentMethod | null;
  invoice_url?: string | null;
  duration_minutes?: number | null;
  text?: string | null;
  error?: string | null;
};

export type BookingItem = {
  id: number;
  status: string;
  status_label?: string | null;
  status_emoji?: string | null;
  // Server-provided combined status text (may include emoji) and formatted date/time
  display_text?: string | null;
  formatted_date?: string | null;
  formatted_time_range?: string | null;
  // Optional server-provided human-friendly datetime (e.g. "14:00, 7 Jan")
  starts_at_formatted?: string | null;
  starts_at?: string | null;
  ends_at?: string | null;
  master_id?: number | null;
  service_names?: string | null;
  can_cancel: boolean;
  can_reschedule: boolean;
  rated?: boolean;
  master_name?: string | null;
  price_cents?: number | null; // Keep this line for backward compatibility
  price_formatted?: string | null;
  currency?: string | null;
  payment_method?: PaymentMethod | null;
};

// Backend-aligned booking terminal statuses — keep in sync with API enums
// NOTE: status labels, emojis and permissions (`can_cancel`/`can_reschedule`)
// are provided by the backend in `/api/bookings`. Avoid duplicating
// status enums here — use server-provided `status_label` / `status_emoji`
// and `can_cancel`/`can_reschedule` as the source of truth.

export type PriceQuoteRequest = {
  service_ids: string[];
  payment_method?: PaymentMethod;
  master_id?: number | null;
};

export type PriceQuoteResponse = {
  final_price_cents: number;
  original_price_cents?: number | null;
  discount_amount_cents?: number | null;
  currency: string;
  discount_percent_applied?: number | null;
  duration_minutes?: number | null;
};

// --- API Calls ---

export async function fetchServices(): Promise<ServiceOut[]> {
  const { data } = await api.get<ServiceOut[]>("/api/services");
  return data;
}

export async function fetchMastersForService(service_id: string): Promise<MasterOut[]> {
  const { data } = await api.get<MasterOut[]>("/api/masters_for_service", {
    params: { service_id },
  });
  return data;
}

export async function fetchMasters(): Promise<MasterOut[]> {
  const { data } = await api.get<MasterOut[]>('/api/masters');
  return data;
}

export type ServiceRange = {
  min_duration: number | null;
  max_duration: number | null;
  min_price_cents: number | null;
  max_price_cents: number | null;
};

export async function fetchServiceRanges(service_ids: string[]): Promise<Record<string, ServiceRange>> {
  const { data } = await api.get<Record<string, ServiceRange>>('/api/service_ranges', {
    params: { 'service_ids[]': service_ids },
  });
  return data;
}

export async function fetchMastersMatch(service_ids: string[]): Promise<MasterOut[]> {
  const { data } = await api.post<MasterOut[]>("/api/masters_match", { service_ids });
  return data;
}

export async function fetchMasterProfile(master_id: number): Promise<MasterProfile> {
  const { data } = await api.get<MasterProfile>("/api/master_profile", {
    params: { master_id },
  });
  return data;
}

export async function fetchSlots(params: {
  master_id?: number | null;
  date_param: string; // YYYY-MM-DD
  service_ids: string[];
}): Promise<SlotsResponse> {
  const { data } = await api.get<SlotsResponse>("/api/slots", {
    params: {
      // Server expects query param name `date` (YYYY-MM-DD)
      date: params.date_param,
      ...(params.master_id != null ? { master_id: params.master_id } : {}),
      // `GET /api/slots` in the backend expects a comma-separated string like "1,2,3"
      service_ids: params.service_ids.join(","),
    },
  });
  return data;
}

export async function fetchAvailableDays(params: {
  master_id?: number | null;
  year: number;
  month: number; // 1-12
  service_ids: string[];
}): Promise<AvailableDaysResponse> {
  const { data } = await api.get<AvailableDaysResponse>("/api/available_days", {
    params: {
      year: params.year,
      month: params.month,
      ...(params.master_id != null ? { master_id: params.master_id } : {}),
      "service_ids[]": params.service_ids,
    },
  });
  return data;
}

// -------------------------
// Booking status constants
// -------------------------
// Booking status enum mirrors backend values. Do NOT make client-side
// decisions about terminal vs active statuses here; rely on backend
// `can_cancel` and `can_reschedule` flags instead.
export const BookingStatus = {
  RESERVED: "reserved",
  PENDING_PAYMENT: "pending_payment",
  CONFIRMED: "confirmed",
  PAID: "paid",
  CANCELLED: "cancelled",
  DONE: "done",
  NO_SHOW: "no_show",
  EXPIRED: "expired",
} as const;

export type BookingStatusValue = typeof BookingStatus[keyof typeof BookingStatus];

export async function checkSlot(params: { master_id?: number | null; slot: string; service_ids: string[] }): Promise<{ available: boolean; conflict?: string | null }> {
  const { data } = await api.get<{ available: boolean; conflict?: string | null }>("/api/check_slot", {
    params: {
      slot: params.slot,
      ...(params.master_id != null ? { master_id: params.master_id } : {}),
      "service_ids[]": params.service_ids,
    },
  });
  return data;
}

export async function createHold(payload: BookingRequest): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/hold", payload);
  return data;
}

export async function finalizeBooking(params: { booking_id: number; payment_method: "cash" | "online" }): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/finalize", params);
  return data;
}

export async function fetchBookings(mode: "upcoming" | "history" = "upcoming"): Promise<BookingItem[]> {
  const { data } = await api.get<BookingItem[]>("/api/bookings", { params: { mode } });
  return data;
}

export async function cancelBooking(booking_id: number): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/cancel", { booking_id });
  return data;
}

export async function rescheduleBooking(params: { booking_id: number; new_slot: string }): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/reschedule", {
    booking_id: params.booking_id,
    new_slot: params.new_slot,
  });
  return data;
}

export async function rateBooking(params: { booking_id: number; rating: number }): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/rate", {
    booking_id: params.booking_id,
    rating: params.rating,
  });
  return data;
}

export async function fetchPriceQuote(payload: PriceQuoteRequest): Promise<PriceQuoteResponse> {
  const { data } = await api.post<PriceQuoteResponse>("/api/price_quote", payload);
  return data;
}