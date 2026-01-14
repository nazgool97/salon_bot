import React, { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchBookings, BookingItem } from "../../api/booking";
import { t } from "../../i18n";
import { BookingDetailsModal, BookingCard, useBookingActions } from "../booking/MyVisits";
import { parseInstagram } from "../../lib/twa";

export default function Hub({ onNavigate }: { onNavigate: (to: "booking" | "visits") => void }) {
  const { data, isLoading } = useQuery<BookingItem[]>({ queryKey: ["bookings", "upcoming"], queryFn: () => fetchBookings("upcoming") });

  const nearest = Array.isArray(data) && data.length > 0 ? data[0] : null;
  const [modalBooking, setModalBooking] = useState<BookingItem | null>(null);

  const { cancel, startReschedule } = useBookingActions({
    onCancelSuccess: () => {},
    onError: () => {},
  });

  const address = (typeof window !== "undefined" && (window as any).__APP_ADDRESS) || null;
  const phone = (typeof window !== "undefined" && (window as any).__APP_PHONE) || null;
  const instagram = (typeof window !== "undefined" && (window as any).__APP_INSTAGRAM) || null;
  const hasContacts = Boolean(address || phone || instagram);

  const parsedInsta = parseInstagram(instagram);

  const contactRows = [
    address
      ? {
          icon: "📍",
          label: t("contacts_address") || "Адрес",
          value: address,
          display: address,
          href: `https://maps.google.com/?q=${encodeURIComponent(address)}`,
        }
      : null,
    phone
      ? {
          icon: "📞",
          label: t("contacts_phone") || "Телефон",
          value: phone,
          display: phone,
          href: `tel:${phone}`,
        }
      : null,
    parsedInsta.url
      ? {
          icon: "📸",
          label: t("contacts_instagram") || "Instagram",
          value: instagram || parsedInsta.url || "",
          display: parsedInsta.handle || instagram || parsedInsta.url,
          href: parsedInsta.url,
        }
      : null,
  ].filter(Boolean) as Array<{ icon: string; label: string; value: string; display: string; href: string | null }>;

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
              <div className="tma-field-label">{t("nearest_visit_label")}</div>
              <BookingCard
                booking={nearest}
                masterName={nearest.master_name || undefined}
                onOpen={() => setModalBooking(nearest)}
                onCancel={nearest.can_cancel ? () => cancel(nearest.id) : undefined}
                onReschedule={nearest.can_reschedule ? () => startReschedule(nearest) : undefined}
              />

              <BookingDetailsModal
                booking={modalBooking}
                onClose={() => setModalBooking(null)}
                allowReschedule={!!modalBooking?.can_reschedule}
                allowCancel={!!modalBooking?.can_cancel}
                onRescheduleClick={modalBooking ? () => { startReschedule(modalBooking); setModalBooking(null); } : undefined}
                onCancelClick={modalBooking ? () => { if (modalBooking) cancel(modalBooking.id); setModalBooking(null); } : undefined}
              />
            </>
          ) : null}

          {hasContacts ? (
            <div className="tma-card u-w-100 hub-contacts-card">
              <div className="hub-contacts-head">
                <span className="hub-contacts-title">{t("contacts_title") || "Контакты"}</span>
                <span className="hub-contacts-emoji">🤝</span>
              </div>

              <div className="hub-contacts-rows">
                {contactRows.map((row) => (
                  <div className="hub-contacts-row" key={row.label + row.value}>
                    <div className="hub-contacts-left">
                      <span className="hub-contacts-icon">{row.icon}</span>
                      <span className="hub-contacts-label">{row.label}</span>
                    </div>
                    <div className="hub-contacts-right">
                      {row.href ? (
                        <a
                          className="hub-contacts-link"
                          href={row.href}
                          target={row.href.startsWith("http") ? "_blank" : undefined}
                          rel={row.href.startsWith("http") ? "noopener noreferrer" : undefined}
                        >
                          {row.display}
                        </a>
                      ) : (
                        <span className="hub-contacts-value">{row.display}</span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </div>
  );
}
