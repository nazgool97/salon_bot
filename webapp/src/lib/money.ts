export const getAppCurrency = (): string => {
  const win = window as any;
  if (win && win.__APP_CURRENCY) return String(win.__APP_CURRENCY);
  return "UAH";
};

export const getAppLocale = (): string => {
  const win = window as any;
  if (win && win.__APP_LOCALE) return String(win.__APP_LOCALE);
  return "ru-RU";
};

export const createMoneyFormatter = (currency?: string | null, maximumFractionDigits?: number) => {
  const curr = currency || getAppCurrency();
  const locale = getAppLocale();
  return new Intl.NumberFormat(locale, { style: "currency", currency: curr, maximumFractionDigits: maximumFractionDigits ?? 2 });
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
