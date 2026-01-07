import WebApp from "@twa-dev/sdk";

export const tg = WebApp;
const isDev = Boolean(import.meta.env?.DEV);

export function initWebApp(): void {
  if (!tg) return;
  try {
    tg.ready();
    tg.expand();
    tg.disableVerticalSwipes();
    // Align Telegram header with app background to avoid visible seams
    try {
      const style = getComputedStyle(document.documentElement);
      const appBg = style.getPropertyValue("--t-bg")?.trim();
      const tgBg = style.getPropertyValue("--tg-theme-bg-color")?.trim();
      const headerColor = appBg || tgBg;
      if (headerColor) {
        tg.setHeaderColor(headerColor);
        tg.setBackgroundColor?.(headerColor);
      }
    } catch (err) {
      if (isDev) console.debug("header color fallback", err);
    }
    // Allow server to pass preferred display timezone via query param `server_tz`.
    try {
      const url = new URL(window.location.href);
      const serverTz = url.searchParams.get("server_tz") || url.searchParams.get("server_tz_name");
      if (serverTz) {
        // expose globally for the app to consume
        (window as any).__SERVER_TZ = serverTz;
      }
      // Extract user language from initData if present and expose as __TWA_LANG
      try {
        const init = getInitData();
        if (init) {
          const params = new URLSearchParams(init);
          const user = params.get("user");
          if (user) {
            try {
              const u = JSON.parse(user);
              if (u && u.language_code) (window as any).__TWA_LANG = String(u.language_code);
            } catch (err) {
              // ignore
            }
          }
        }
      } catch (err) {
        // ignore
      }
    } catch (err) {
      // ignore
    }
    tg.MainButton.hide();
  } catch (err) {
    if (isDev) console.debug("TWA init fallback", err);
  }
}

export function getInitData(): string {
  const url = new URL(window.location.href);
  const mock = url.searchParams.get("initData") || url.searchParams.get("twa_initdata");
  if (mock) return mock;
  return tg?.initData || "";
}

export function getUserLanguage(): string | null {
  try {
    const win = window as any;
    if (win && win.__TWA_LANG) return String(win.__TWA_LANG);
    const init = getInitData();
    if (!init) return null;
    const params = new URLSearchParams(init);
    const user = params.get("user");
    if (!user) return null;
    try {
      const u = JSON.parse(user);
      if (u && u.language_code) return String(u.language_code);
    } catch (err) {
      return null;
    }
  } catch (err) {
    // ignore
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
    if (isDev) console.debug("haptic fallback", err);
  }
}
