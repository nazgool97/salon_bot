import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchBookings, BookingItem } from "../../api/booking";
import { t } from "../../i18n";
import { friendlyDateTime } from "../../lib/timezone";
import { BookingDetailsModal } from "../booking/MyVisits";

export default function Hub({ onNavigate }: { onNavigate: (to: "booking" | "visits") => void }) {
  const { data, isLoading } = useQuery<BookingItem[]>({ queryKey: ["bookings", "upcoming"], queryFn: () => fetchBookings("upcoming") });

  const nearest = Array.isArray(data) && data.length > 0 ? data[0] : null;
  const [modalBooking, setModalBooking] = useState<BookingItem | null>(null);

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

              <BookingDetailsModal
                booking={modalBooking}
                onClose={() => setModalBooking(null)}
              />
            </>
          ) : null}
        </section>
      </div>
    </div>
  );
}
