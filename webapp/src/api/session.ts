import api from "./client";

export type SessionPayload = { initData: string };
export type SessionResponse = {
  token: string;
  user: { id: number; username?: string };
  currency?: string;
  locale?: string;
  webapp_title?: string;
  online_payments_available?: boolean | null;
  online_payment_discount_percent?: number | null;
  reminder_lead_minutes?: number | null;
  // optional admin-provided address fields
  address?: string | null;
  webapp_address?: string | null;
  location_address?: string | null;
};

export async function createSession(payload: SessionPayload): Promise<SessionResponse> {
  const { data } = await api.post<SessionResponse>("/api/session", payload);
  return data;
}
