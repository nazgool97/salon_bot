export const getDefaultLocalTimezone = (): string => {
  try {
    const win = window as any;
    if (win && win.__SERVER_TZ) return String(win.__SERVER_TZ);
    const resolved = Intl.DateTimeFormat().resolvedOptions().timeZone;
    if (resolved) return resolved;
  } catch (err) {
    // ignore
  }
  return "UTC";
};

export const getAppLocale = (): string => {
  try {
    const win = window as any;
    if (win && win.__APP_LOCALE) return String(win.__APP_LOCALE);
  } catch (err) {
    // ignore
  }
  return "ru-RU";
};

export function formatInTimezone(iso: string | Date | number | null | undefined, opts?: Intl.DateTimeFormatOptions): string {
  if (!iso) return "";
  const tz = getDefaultLocalTimezone();
  const locale = getAppLocale();
  let d: Date;
  try {
    d = typeof iso === "string" || typeof iso === "number" ? new Date(iso) : (iso as Date);
  } catch (err) {
    return String(iso);
  }
  if (Number.isNaN(d.getTime())) return String(iso);
  try {
    return new Intl.DateTimeFormat(locale, { timeZone: tz, ...opts }).format(d);
  } catch (err) {
    return d.toLocaleString();
  }
}

export const formatYMD = (year: number, month: number, day: number): string => `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;

const todayIso = () => {
  const d = new Date();
  return formatYMD(d.getFullYear(), d.getMonth() + 1, d.getDate());
};

export const normalizeSlotString = (raw: string, dateHint?: string): string => {
  if (!raw) return "";
  const trimmed = raw.trim();
  const hasDate = trimmed.includes("T") || trimmed.includes(" ");
  const base = hasDate ? trimmed.replace(" ", "T") : `${dateHint || todayIso()}T${trimmed}`;
  if (/Z$|[+-]\d{2}:?\d{2}$/.test(base)) return base;
  if (/T\d{2}:\d{2}$/.test(base)) return `${base}:00`;
  return base;
};

export const friendlyDate = (value: string): string => {
  if (!value) return "";
  const parts = value.split("-");
  if (parts.length !== 3) return value;
  const [y, m, d] = parts.map((p) => Number(p));
  const date = new Date(y, m - 1, d);
  if (Number.isNaN(date.getTime())) return value;
  return formatInTimezone(date, { day: "numeric", month: "long" });
};

export const friendlyTime = (raw: string, fallbackDate?: string): string => {
  if (!raw) return "";
  const normalized = normalizeSlotString(raw, fallbackDate);
  return formatInTimezone(normalized, { hour: "2-digit", minute: "2-digit" });
};

export const formatDateTimeLabel = (dateIso: string, timeStr: string): string => {
  if (!dateIso && !timeStr) return "";
  const normalized = normalizeSlotString(timeStr || "", dateIso || todayIso());
  const d = new Date(normalized);
  if (!Number.isNaN(d.getTime())) {
    return formatInTimezone(d, { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  if (dateIso && timeStr) return `${dateIso} ${timeStr}`;
  return dateIso || timeStr || "";
};

export const friendlyDateTime = (iso?: string | null): string => {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso as string;
  return formatInTimezone(d, { day: "numeric", month: "long", hour: "2-digit", minute: "2-digit" });
};

// Money formatting helpers (use app locale & global currency when available)
export const getAppCurrency = (): string => {
  try {
    const win = window as any;
    if (win && win.__APP_CURRENCY) return String(win.__APP_CURRENCY);
  } catch (err) {
    // ignore
  }
  return "UAH";
};

export const createMoneyFormatter = (currency?: string | null, maximumFractionDigits?: number) => {
  const curr = currency || getAppCurrency();
  const locale = getAppLocale();
  try {
    return new Intl.NumberFormat(locale, { style: "currency", currency: curr, maximumFractionDigits: maximumFractionDigits ?? 2 });
  } catch (err) {
    return { format: (n: number) => String(n) } as Intl.NumberFormat;
  }
};

export const formatMoneyFromCents = (cents: number | null | undefined, currency?: string | null, maximumFractionDigits?: number): string => {
  if (cents == null) return "";
  const num = Number(cents) / 100;
  try {
    return createMoneyFormatter(currency, maximumFractionDigits).format(num as number);
  } catch (err) {
    return String(num);
  }
};

export const formatMoney = (amount: number | null | undefined, currency?: string | null, maximumFractionDigits?: number): string => {
  if (amount == null) return "";
  try {
    return createMoneyFormatter(currency, maximumFractionDigits).format(amount as number);
  } catch (err) {
    return String(amount);
  }
};
