export const getDefaultLocalTimezone = (): string => {
  if (typeof window !== "undefined" && (window as any).__SERVER_TZ) {
    return String((window as any).__SERVER_TZ);
  }
  return "UTC";
};

export const getAppLocale = (): string => {
  const win = window as any;
  if (win && win.__APP_LOCALE) return String(win.__APP_LOCALE);
  return "ru-RU";
};

export function formatInTimezone(iso: string | Date | number | null | undefined, opts?: Intl.DateTimeFormatOptions): string {
  if (!iso) return "";
  const tz = getDefaultLocalTimezone();
  const locale = getAppLocale();
  const d = typeof iso === "string" || typeof iso === "number" ? new Date(iso) : (iso as Date);
  if (Number.isNaN(d.getTime())) return String(iso);
  return new Intl.DateTimeFormat(locale, { timeZone: tz, ...opts }).format(d);
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

type Granularity = "short" | "long";

export const formatDate = (iso?: string | Date | number | null, opts?: { granularity?: Granularity }): string => {
  return formatISO(iso, opts?.granularity === "short" ? "date-short" : "date-long");
};

export const formatTime = (value?: string | Date | number | null, fallbackDate?: string, opts?: { granularity?: Granularity }): string => {
  if (!value) return "";
  const normalized = typeof value === "string" ? normalizeSlotString(value, fallbackDate) : (value as Date | number);
  return formatISO(normalized, opts?.granularity === "short" ? "time-short" : "time-long");
};

export const formatDateTime = (iso?: string | Date | number | null, opts?: { granularity?: Granularity }): string => {
  return formatISO(iso, opts?.granularity === "short" ? "datetime-short" : "datetime-long");
};

/**
 * Generic ISO formatter. `pattern` controls which parts are rendered.
 * Supported patterns: date-short, date-long, time-short, time-long, datetime-short, datetime-long
 */
export function formatISO(iso: string | Date | number | null | undefined, pattern: string): string {
  if (!iso) return "";
  const d = typeof iso === "string" || typeof iso === "number" ? new Date(iso) : (iso as Date);
  if (Number.isNaN(d.getTime())) return String(iso);
  const map: Record<string, Intl.DateTimeFormatOptions> = {
    "date-short": { day: "2-digit", month: "2-digit", year: "numeric" },
    "date-long": { day: "numeric", month: "long", year: "numeric" },
    "time-short": { hour: "2-digit", minute: "2-digit" },
    "time-long": { hour: "2-digit", minute: "2-digit", second: "2-digit" },
    "datetime-short": { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" },
    "datetime-long": { day: "numeric", month: "long", year: "numeric", hour: "2-digit", minute: "2-digit" },
  };
  const opts = map[pattern] || map["datetime-long"];
  return formatInTimezone(d, opts);
}

// Backwards-compatible thin wrappers for existing call sites. Prefer `formatDate`, `formatTime`, `formatDateTime`.
export const friendlyDate = (value: string) => formatISO(value, "date-long");
export const friendlyTime = (raw: string, fallbackDate?: string) => formatISO(normalizeSlotString(raw, fallbackDate), "time-short");
export const friendlyDateTime = (iso?: string | null) => formatISO(iso, "datetime-long");
export const formatDateTimeShort = (iso?: string | null) => formatISO(iso, "datetime-short");

export const formatDateTimeLabel = (dateIso: string, timeStr: string): string => {
  if (!dateIso && !timeStr) return "";
  const normalized = normalizeSlotString(timeStr || "", dateIso || todayIso());
  const d = new Date(normalized);
  if (!Number.isNaN(d.getTime())) {
    return formatDateTime(d, { granularity: "short" });
  }
  return dateIso && timeStr ? `${dateIso} ${timeStr}` : dateIso || timeStr || "";
};

export const formatDurationMinutes = (minutes: number | null | undefined, unit: string = "min"): string => {
  if (minutes == null) return "";
  const m = Number(minutes);
  if (!Number.isFinite(m)) return "";
  return `${m} ${unit}`;
};

// Prefer server-provided human-friendly datetime when present.
export const formatDateTimePreferServer = (serverFormatted: string | null | undefined, iso?: string | null): string => {
  if (serverFormatted) return String(serverFormatted);
  return formatDateTime(iso, { granularity: "long" });
};

export const formatDateTimeShortPreferServer = (serverFormatted: string | null | undefined, iso?: string | null): string => {
  if (serverFormatted) return String(serverFormatted);
  return formatDateTime(iso, { granularity: "short" });
};

// Currency helpers were moved to lib/money.ts to keep timezone.ts single-purpose.
