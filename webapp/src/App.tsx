// src/App.tsx
import { useEffect, useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";

import { setAuthToken, setLang } from "./api/client";
import api from "./api/client";
import { createSession } from "./api/session";
import BookingWizard from "./features/booking/BookingWizard"; // <-- IMPORT NEW WIZARD
import MyVisits from "./features/booking/MyVisits";
import Hub from "./features/hub/Hub";
import { t } from "./i18n";
import { getInitData, initWebApp, tg, getUserLanguage } from "./lib/twa";

export default function App() {
  const [token, setToken] = useState<string | null>(null);
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [tab, setTab] = useState<"hub" | "booking" | "visits">("hub");
  const [bookingStep, setBookingStep] = useState<string | null>(null);
  const authAppliedRef = useRef(false);
  const isDev = Boolean(import.meta.env?.DEV);

  useEffect(() => {
    initWebApp();
  }, []);

  useEffect(() => {
    // Centralized MainButton control: hide outside booking wizard
    try {
      if (tab !== "booking") {
        tg?.MainButton?.hide();
      }
      // booking tab is managed by BookingWizard
    } catch (err) {
      if (isDev) console.debug("main button toggle", err);
    }
  }, [tab, isDev]);

  const sessionMutation = useMutation({
    mutationFn: (initData: string) => createSession({ initData }),
    onSuccess: async (data) => {
      setToken(data.token);
      if (!authAppliedRef.current) {
        setAuthToken(data.token);
        authAppliedRef.current = true;
      }

        try {
          // Use values returned by /api/session (authoritative)
        const cfg: any = data || {};
        try {
          (window as any).__APP_CURRENCY = cfg.currency ?? null;
          (window as any).__APP_LOCALE = cfg.locale ?? null;
            (window as any).__APP_TITLE = cfg.webapp_title ?? "Telegram Mini App • Beauty";
          (window as any).__APP_ONLINE_PAYMENTS_AVAILABLE = cfg.online_payments_available;
            (window as any).__APP_REMINDER_LEAD_MINUTES = cfg.reminder_lead_minutes ?? null;
          // support admin-provided address (prefer explicit keys)
          (window as any).__APP_ADDRESS = cfg.address ?? cfg.webapp_address ?? cfg.location_address ?? null;
        } catch (err) {}

        if (cfg.locale) {
          try {
            setLang(cfg.locale);
          } catch (err) {}
        }
      } catch (err) {
        console.warn("apply session config failed", err);
      }
    },
    onError: () => setSessionError("Authorization failed. Please restart the bot.")
  });

  useEffect(() => {
    const initData = getInitData();
    const lang = getUserLanguage();
    if (lang) setLang(lang);
    if (initData) {
      sessionMutation.mutate(initData);
    } else {
      // For development in browser without Telegram
      if (isDev) {
        console.debug("dev: initData missing; showing auth error");
      }
      setSessionError("Please open this app from Telegram.");
    }
  }, []);

  useEffect(() => {
    const switchToBooking = () => {
      try {
        setTab("booking");
      } catch (err) {}
    };

    const openVisits = () => { try { setTab("visits"); } catch (e) {} };
    const openHub = () => { try { setTab("hub"); } catch (e) {} };

    window.addEventListener("tma:repeat-booking", switchToBooking as EventListener);
    window.addEventListener("tma:reschedule-start", switchToBooking as EventListener);
    window.addEventListener("tma:open-visits", openVisits as EventListener);
    window.addEventListener("tma:open-hub", openHub as EventListener);
    const stepHandler = (e: Event) => {
      try {
        const d: any = (e as CustomEvent).detail;
        setBookingStep(d ?? null);
      } catch (err) {}
    };
    window.addEventListener("tma:booking-step", stepHandler as EventListener);

    return () => {
      window.removeEventListener("tma:repeat-booking", switchToBooking as EventListener);
      window.removeEventListener("tma:reschedule-start", switchToBooking as EventListener);
      window.removeEventListener("tma:open-visits", openVisits as EventListener);
      window.removeEventListener("tma:open-hub", openHub as EventListener);
      window.removeEventListener("tma:booking-step", stepHandler as EventListener);
    };
  }, []);

  if (sessionError) {
    return (
      <div className="flex items-center justify-center h-screen text-red-500 p-4 text-center">
        {sessionError}
      </div>
    );
  }

  if (!token) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="tma-card tma-skeleton-card">
          <div className="tma-skeleton tma-skeleton--lg" />
          <div className="u-h-12" />
          <div className="tma-skeleton u-w-200 u-h-16" />
        </div>
      </div>
    );
  }

  return (
    <div className="tma-app-frame">
      {/* Global header with top-right "My Visits" icon */}
      <header className="tma-global-header">
        <div className="tma-container u-flex u-justify-between u-items-center">
          {tab !== "hub" && !(tab === "booking" && bookingStep === "SUCCESS") ? (
            <>
              <button
                type="button"
                className="tma-icon-btn tma-icon-btn--prominent"
                aria-label={t("hub_home") || "Home"}
                onClick={() => {
                  setTab("hub");
                }}
              >
                <span style={{ fontSize: 18 }}>🏠</span>
                <span className="tma-header-label">{t("home") || "Главная"}</span>
              </button>

              <div />

              <button
                type="button"
                className="tma-icon-btn tma-icon-btn--prominent"
                aria-label={t("tab_visits")}
                onClick={() => {
                  setTab("visits");
                }}
              >
                <span style={{ fontSize: 18 }}>📅</span>
                <span className="tma-header-label">{t("my_visits") || "Мои записи"}</span>
              </button>
            </>
          ) : (
            <div />
          )}
        </div>
      </header>

      {tab === "hub" && <Hub onNavigate={(to: "booking" | "visits") => setTab(to)} />}
      {tab === "booking" && <BookingWizard />}
      {tab === "visits" && <MyVisits />}

      {/* No bottom tabbar — bottom area is reserved for Telegram MainButton */}
    </div>
  );
}