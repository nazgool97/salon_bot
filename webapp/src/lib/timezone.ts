export const getDefaultLocalTimezone = (): string => {
  const win = window as any;
  if (win && win.__SERVER_TZ) return String(win.__SERVER_TZ);
  const resolved = Intl.DateTimeFormat().resolvedOptions().timeZone;
  return resolved || "UTC";
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
  return dateIso && timeStr ? `${dateIso} ${timeStr}` : dateIso || timeStr || "";
};

export const friendlyDateTime = (iso?: string | null): string => {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return formatInTimezone(d, { day: "numeric", month: "long", hour: "2-digit", minute: "2-digit" });
};

export const formatDateTimeShort = (iso?: string | null): string => {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return formatInTimezone(d, {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
};

export const formatDurationMinutes = (minutes: number | null | undefined, unit: string = "min"): string => {
  if (minutes == null) return "";
  const m = Number(minutes);
  if (!Number.isFinite(m)) return "";
  return `${m} ${unit}`;
};

// Currency helpers were moved to lib/money.ts to keep timezone.ts single-purpose.
