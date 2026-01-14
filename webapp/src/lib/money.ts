import { getAppLocale } from "./timezone";

export const getAppCurrency = (): string => {
  const win = window as any;
  if (win && win.__APP_CURRENCY) return String(win.__APP_CURRENCY);
  return "UAH";
};

export const createMoneyFormatter = (currency?: string | null, maximumFractionDigits?: number) => {
  const locale = getAppLocale();
  // Enforce use of server-provided currency. If `currency` is not provided,
  // fall back to a plain number formatter (no currency symbol) to avoid
  // silently using an app-level currency override. Server should supply currency.
  if (!currency) return new Intl.NumberFormat(locale, { maximumFractionDigits: maximumFractionDigits ?? 2 });
  return new Intl.NumberFormat(locale, { style: "currency", currency: String(currency), maximumFractionDigits: maximumFractionDigits ?? 2 });
};

export const formatMoneyFromCents = (cents: number | null | undefined, currency?: string | null, maximumFractionDigits?: number): string => {
  if (cents == null) return "";
  const num = Number(cents) / 100;
  return createMoneyFormatter(currency, maximumFractionDigits).format(num as number);
};

export const formatMoney = (amount: number | null | undefined, currency?: string | null, maximumFractionDigits?: number): string => {
  if (amount == null) return "";
  return createMoneyFormatter(currency, maximumFractionDigits).format(amount as number);
};

// Prefer server-provided formatted money when available to avoid
// duplicating formatting logic on the client. If `serverFormatted`
// is provided and non-empty it is returned, otherwise we format
// using cents+currency as a fallback.
export const formatMoneyPreferServer = (
  serverFormatted: string | null | undefined,
  cents: number | null | undefined,
  currency?: string | null,
  maximumFractionDigits?: number
): string => {
  if (serverFormatted) return String(serverFormatted);
  return formatMoneyFromCents(cents, currency, maximumFractionDigits);
};
