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

// Unified money formatter: prefer server-provided string, otherwise format cents locally.
export const formatMoney = (
  cents: number | null | undefined,
  currency: string | null,
  serverFormatted?: string | null
): string => {
  if (serverFormatted) return serverFormatted;
  if (cents == null) return "—";

  // Use Intl only when server did not supply ready-made formatting.
  return createMoneyFormatter(currency).format(cents / 100);
};
