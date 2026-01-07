import React, { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useQuery } from "@tanstack/react-query";
import { fetchBookings, BookingItem } from "../../api/booking";
import { t } from "../../i18n";
import { friendlyDateTime, formatMoneyFromCents } from "../../lib/timezone";
import { setMainButton } from "../../lib/twa";

export default function Hub({ onNavigate }: { onNavigate: (to: "booking" | "visits") => void }) {
  const { data, isLoading } = useQuery<BookingItem[]>({ queryKey: ["hub:nearest"], queryFn: () => fetchBookings("upcoming") });

  const nearest = Array.isArray(data) && data.length > 0 ? data[0] : null;
  const [modalBooking, setModalBooking] = useState<BookingItem | null>(null);

  useEffect(() => {
    // Reserve bottom area for native MainButton and configure it to start booking
    try {
      // don't show Telegram MainButton on the hub; keep it hidden
      setMainButton("", () => {}, false);
    } catch (err) {}
    return () => {
      try { setMainButton("", () => {}, false); } catch (err) {}
    };
  }, [onNavigate]);

  return (
    <div className="tma-shell">
      <div className="tma-bg" />
      <div className="tma-container">
        <header className="tma-hero">
          <div className="tma-chip tma-chip-ghost">{(typeof window !== "undefined" && (window as any).__APP_TITLE) || t("hub_welcome_chip") || "Хаб"}</div>
          <h1 className="tma-hero-title">{t("hero_title") || "Запись без чатов и звонков"}</h1>
        </header>

        <section className="tma-card tma-stack" style={{ alignItems: "center" }}>
          <div style={{ width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14 }}>
            <button
              className="tma-primary"
              type="button"
              onClick={() => onNavigate("booking")}
              style={{ width: '100%', maxWidth: 360, padding: '14px 20px', fontSize: 16, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10 }}
            >
              <span style={{ fontSize: 22 }}></span>
              <span>{t("hub_book_btn") || "Записаться"}</span>
            </button>

            <button
              className="tma-primary"
              type="button"
              onClick={() => onNavigate("visits")}
              style={{ width: '100%', maxWidth: 360, padding: '14px 20px', fontSize: 16, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 10 }}
            >
              <span style={{ fontSize: 22 }}></span>
              <span>{t("hub_my_visits_btn") || "Мои записи"}</span>
            </button>
          </div>

          <div style={{ height: 18 }} />

          {isLoading ? (
            <div className="tma-skeleton tma-skeleton--lg" />
          ) : nearest ? (
            <>
              <div
                className="tma-card tma-card--compact u-w-100"
                style={{ cursor: 'pointer' }}
                onClick={() => setModalBooking(nearest)}
              >
                <div className="tma-field-label">{t("nearest_visit_label")}</div>
                <div className="tma-field-value">{nearest.service_names || "—"}</div>
                <div className="tma-field-label">{friendlyDateTime(nearest.starts_at)}</div>
              </div>

              {modalBooking && createPortal(
                <div className="tma-modal-backdrop" onClick={() => setModalBooking(null)}>
                  <div className="tma-centered-modal" onClick={(e) => e.stopPropagation()}>
                    <div className="tma-modal-header-fixed">
                      <h3 className="u-mb-0">{`${t("booking_label") || "Запись"} №${modalBooking.id}`}</h3>
                      <button className="tma-close-btn" onClick={() => setModalBooking(null)}>✕</button>
                    </div>

                    <div className="tma-modal-scroll-content">
                      <div className="tma-card tma-card--compact tma-card--spaced">
                        <div className="tma-field-label">{t("service_label")}</div>
                        <div className="tma-field-value">{modalBooking.service_names}</div>

                        <div className="tma-field-label">{t("master_label")}</div>
                        <div className="tma-field-value">{modalBooking.master_name || "—"}</div>

                        <div className="tma-field-label">{t("date_label")}</div>
                        <div className="tma-field-value">{friendlyDateTime(modalBooking.starts_at)}</div>

                        <div className="tma-field-label">{t("duration_label")}</div>
                        <div className="tma-field-value">{(() => {
                          const dm = (modalBooking as any).duration_minutes;
                          const unit = t("minutes_short") as string || "min";
                          if (dm != null) return `${dm} ${unit}`;
                          if (modalBooking.starts_at && (modalBooking as any).ends_at) {
                            try {
                              const s = new Date(modalBooking.starts_at);
                              const e = new Date((modalBooking as any).ends_at);
                              if (!Number.isNaN(s.getTime()) && !Number.isNaN(e.getTime())) {
                                const mins = Math.round((e.getTime() - s.getTime()) / 60000);
                                return `${mins} ${unit}`;
                              }
                            } catch (err) {}
                          }
                          return "—";
                        })()}</div>

                        <div className="tma-field-label">{modalBooking.payment_method === "online" ? (t("paid_label") || "Paid") : (t("to_be_paid") || "To be paid:")}</div>
                        <div className="tma-field-value">{(() => {
                          return modalBooking.price_cents != null ? formatMoneyFromCents(modalBooking.price_cents, modalBooking.currency || undefined) : "—";
                        })()}</div>
                      </div>
                    </div>

                    {/* no action buttons in hub modal */}
                  </div>
                </div>,
                document.body
              )}
            </>
          ) : null}
        </section>
      </div>
    </div>
  );
}
