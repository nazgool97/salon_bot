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

export type BookingRequest = {
  master_id?: number | null; // optional for "any master"
  service_ids: string[]; // Python expects List[str]
  slot: string;          // ISO Format: YYYY-MM-DDTHH:mm:ss
  payment_method?: "cash" | "online";
};

export type BookingResponse = {
  ok: boolean;
  booking_id?: number;
  status?: string;
  starts_at?: string;
  cash_hold_expires_at?: string | null;
  original_price_cents?: number | null;
  final_price_cents?: number | null;
  discount_amount_cents?: number | null;
  currency?: string | null;
  master_id?: number | null;
  master_name?: string | null;
  payment_method?: "cash" | "online";
  invoice_url?: string;
   duration_minutes?: number;
  error?: string;
};

export type BookingItem = {
  id: number;
  status: string;
  status_label?: string | null;
  status_emoji?: string | null;
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
  payment_method?: "cash" | "online" | null;
};

export type PriceQuoteRequest = {
  service_ids: string[];
  payment_method?: "cash" | "online";
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

export async function createBooking(payload: BookingRequest): Promise<BookingResponse> {
  const { data } = await api.post<BookingResponse>("/api/book", payload);
  return data;
}

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