import WebApp from "@twa-dev/sdk";

export const tg = WebApp;

export function initWebApp(): void {
  if (!tg) return;
  tg.ready();
  tg.expand();
  tg.disableVerticalSwipes();

  // Align Telegram header with app background to avoid visible seams
  const style = getComputedStyle(document.documentElement);
  const appBg = style.getPropertyValue("--t-bg")?.trim();
  const tgBg = style.getPropertyValue("--tg-theme-bg-color")?.trim();
  const headerColor = appBg || tgBg;
  if (headerColor) {
    tg.setHeaderColor(headerColor as "bg_color" | `#${string}`);
    tg.setBackgroundColor?.(headerColor as `#${string}`);
  }

  const url = new URL(window.location.href);
  const serverTz = url.searchParams.get("server_tz") || url.searchParams.get("server_tz_name");
  if (serverTz) {
    (window as any).__SERVER_TZ = serverTz;
  }

  const userLang = getUserLanguage();
  if (userLang) {
    (window as any).__TWA_LANG = userLang;
  }

  tg.MainButton.hide();
}

export function getInitData(): string {
  const url = new URL(window.location.href);
  const mock = url.searchParams.get("initData") || url.searchParams.get("twa_initdata");
  if (mock) return mock;
  return tg?.initData || "";
}

export function getUserLanguage(): string | null {
  const win = window as any;
  if (win && win.__TWA_LANG) return String(win.__TWA_LANG);
  const init = getInitData();
  if (!init) return null;
  const params = new URLSearchParams(init);
  const user = params.get("user");
  if (!user) return null;
  try {
    const parsed = JSON.parse(user);
    if (parsed && parsed.language_code) return String(parsed.language_code);
  } catch {
    return null;
  }
  return null;
}

export function setMainButton(label: string, onClick: () => void, enabled: boolean) {
  if (!tg?.MainButton) return;
  tg.MainButton.setText(label);
  tg.MainButton.offClick(onClick);
  if (enabled) {
    tg.MainButton.show();
    tg.MainButton.onClick(onClick);
  } else {
    tg.MainButton.hide();
  }
}

export function notifySuccess(): void {
  try {
    tg?.HapticFeedback?.notificationOccurred("success");
  } catch (err) {
    // ignore
  }
}
