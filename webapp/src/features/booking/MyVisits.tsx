import { useState, useEffect, useMemo, useCallback } from "react";
import { createPortal } from "react-dom";
import { useMutation, useQueries, useQuery } from "@tanstack/react-query";
import { Toast } from "./BookingWizard";

import {
  fetchBookings,
  cancelBooking,
  rescheduleBooking,
  rateBooking,
  BookingItem,
  fetchMasters,
} from "../../api/booking";
import { t } from "../../i18n";
import type { MasterOut } from "../../api/booking";
import { tg, haptic } from "../../lib/twa";
import { formatDurationMinutes, formatDateTime } from "../../lib/timezone";
import { formatMoney } from "../../lib/money";

// Status chip styles are now derived from server `status` directly via
// CSS class names like `status-paid`, `status-cancelled`, etc.

export function BookingDetailsModal({
  booking,
  masterMap,
  onClose,
  allowReschedule,
  allowCancel,
  onRescheduleClick,
  onCancelClick,
}: {
  booking: BookingItem | null;
  masterMap?: Record<number, string>;
  onClose: () => void;
  allowReschedule?: boolean;
  allowCancel?: boolean;
  onRescheduleClick?: () => void;
  onCancelClick?: () => void;
}) {
  if (!booking) return null;

  const durationLabel = formatDurationMinutes(booking.duration_minutes) || "—";

  const masterLabel = masterMap?.[booking.master_id || 0] || booking.master_name || (t("master_default") as string);
  const priceFinal = booking.final_price_cents ?? booking.price_cents ?? booking.original_price_cents ?? null;
  const priceOriginal = booking.original_price_cents;
  const priceDiscount = booking.discount_amount_cents;
  const hasDiscount = priceOriginal != null && priceDiscount != null && priceDiscount > 0 && priceFinal != null;

  const finalLabel = formatMoney(priceFinal, booking.currency || null, booking.final_price_formatted || booking.price_formatted);
  const originalLabel = hasDiscount
    ? formatMoney(priceOriginal, booking.currency || null, booking.original_price_formatted)
    : null;
  const discountLabel = hasDiscount
    ? formatMoney(priceDiscount, booking.currency || null, booking.discount_amount_formatted)
    : null;

  return createPortal(
    <div className="tma-modal-backdrop" onClick={onClose}>
      <div className="tma-centered-modal" onClick={(e) => e.stopPropagation()}>
        <div className="tma-modal-header-fixed">
          <h3 className="u-mb-0">{`${t("booking_label") || "Booking"} №${booking.id}`}</h3>
          <button className="tma-close-btn" onClick={onClose}>✕</button>
        </div>

        <div className="tma-modal-scroll-content">
          <div className="tma-card tma-card--compact tma-card--spaced">
            <div className="tma-field-label">{t("service_label")}</div>
            <div className="tma-field-value">{booking.service_names}</div>

            <div className="tma-field-label">{t("master_label")}</div>
            <div className="tma-field-value">{masterLabel}</div>

            <div className="tma-field-label">{t("date_label")}</div>
            <div className="tma-field-value">
              {formatDateTime(booking.starts_at, { granularity: "long" })}
            </div>

            <div className="tma-field-label">{t("duration_label")}</div>
            <div className="tma-field-value">{durationLabel}</div>

            <div className="tma-field-label">{booking.payment_method === "online" ? t("paid_label") : t("to_be_paid")}</div>
            <div className="tma-field-value">
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                <strong>{finalLabel}</strong>
                {hasDiscount && originalLabel ? <span className="tma-price-strike tma-subtle">{originalLabel}</span> : null}
              </div>
              {hasDiscount && discountLabel ? (
                <div className="tma-subtle">{t("online_discount") || "Скидка"} {discountLabel}</div>
              ) : null}
            </div>
          </div>
        </div>

        {(allowReschedule || allowCancel) ? (
          <div className="tma-modal-actions">
            {allowReschedule ? (
              <button className="tma-primary u-w-100" type="button" onClick={onRescheduleClick}>
                {t("reschedule_label") || "Перенести"}
              </button>
            ) : null}
            {allowCancel ? (
              <button className="tma-danger tma-danger-ghost" type="button" onClick={onCancelClick}>
                {t("cancel_btn")}
              </button>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>,
    document.body
  );
}

// Shared booking actions hook (kept in this file to avoid adding new files).
export function useBookingActions(opts?: { onCancelSuccess?: () => void; onError?: (msg: string) => void }) {
  const cancelMut = useMutation({
    mutationFn: cancelBooking,
    onSuccess: (resp: any) => {
      if (resp && resp.ok) {
        try { haptic.notify("success"); } catch (e) {}
        try { window.dispatchEvent(new CustomEvent("tma:bookings-changed")); } catch (e) {}
        opts?.onCancelSuccess?.();
      } else {
        opts?.onError?.(resp?.error || "");
      }
    },
    onError: () => opts?.onError?.("network_unavailable"),
  });

  function startReschedule(b: BookingItem | null) {
    if (!b) return;
    try {
      const detail = { booking_id: b.id, master_id: b.master_id, service_names: b.service_names, starts_at: b.starts_at };
      try { (window as any).__RESCHEDULE_BOOKING = detail; } catch (e) {}
      try { window.dispatchEvent(new CustomEvent("tma:reschedule-start", { detail })); } catch (e) {}
      try { window.dispatchEvent(new CustomEvent("tma:repeat-booking")); } catch (e) {}
    } catch (err) {
      console.warn("startReschedule failed", err);
    }
  }

  function cancel(id: number) {
    cancelMut.mutate(id);
  }

  return { cancelMut, cancel, startReschedule };
}

export default function MyVisits() {
  const [toast, setToast] = useState<{ message: string; tone?: "error" | "success" } | null>(null);
  const [tab, setTab] = useState<"upcoming" | "history">("upcoming");

  const [upcomingQuery, historyQuery] = useQueries({
    queries: [
      { queryKey: ["bookings", "upcoming"] as const, queryFn: () => fetchBookings("upcoming") },
      { queryKey: ["bookings", "history"] as const, queryFn: () => fetchBookings("history") },
    ],
  });

  const dedupe = (list: BookingItem[] | undefined) => {
    const arr = Array.isArray(list) ? list : [];
    return Array.from(new Map(arr.map((x) => [x.id, x])).values());
  };

  const upcomingList = useMemo(
    () => dedupe(upcomingQuery.data as BookingItem[] | undefined),
    [upcomingQuery.data]
  );
  const historyList = useMemo(
    () => dedupe(historyQuery.data as BookingItem[] | undefined),
    [historyQuery.data]
  );
  const activeList = tab === "upcoming" ? upcomingList : historyList;
  const upcomingCount = upcomingList.length;
  const historyCount = historyList.length;
  const activeQuery = tab === "upcoming" ? upcomingQuery : historyQuery;
  const isLoading = activeQuery.isLoading;

  // Log query results for debugging why upcoming bookings may be empty
  useEffect(() => {
    try {
      if (!isLoading && import.meta.env?.DEV) {
        const payload = tab === "upcoming" ? upcomingList : historyList;
        console.debug("fetchBookings result", tab, payload.length, payload);
      }
    } catch (e) {}
  }, [historyList, isLoading, tab, upcomingList]);
  const { data: masters } = useQuery<MasterOut[]>({ queryKey: ["masters"], queryFn: fetchMasters });
  const masterMap = (masters || []).reduce<Record<number, string>>((acc, m) => {
    acc[m.id] = m.name;
    return acc;
  }, {});
  const [rescheduleTarget, setRescheduleTarget] = useState<BookingItem | null>(null);
  const [newSlot, setNewSlot] = useState<string>("");

  const refetchAll = useCallback(() => {
    try { upcomingQuery.refetch(); } catch (e) {}
    try { historyQuery.refetch(); } catch (e) {}
  }, [historyQuery, upcomingQuery]);

  const { cancel, startReschedule } = useBookingActions({
    onCancelSuccess: () => { setRescheduleTarget(null); refetchAll(); },
    onError: (msg) => setToast({ message: msg || (t("network_unavailable") as string), tone: "error" }),
  });

  const rateMut = useMutation({
    mutationFn: rateBooking,
    onSuccess: (resp) => {
      if (resp.ok) {
        haptic.notify("success");
        refetchAll();
      } else {
        setToast({ message: resp.error || (t("rate_failed") as string), tone: "error" });
      }
    },
    onError: () => setToast({ message: (t("network_unavailable") as string), tone: "error" }),
  });

  const repeatBooking = (b: BookingItem) => {
    try {
      const win = window as any;
      // Try to provide service names (best-effort) and master id.
      const service_names = b.service_names ? b.service_names.split(",").map((s) => s.trim()).filter(Boolean) : [];
      win.__REPEAT_BOOKING = {
        master_id: b.master_id ?? null,
        service_names,
      };
      // Notify App to switch to booking tab and BookingWizard to consume payload
      window.dispatchEvent(new CustomEvent("tma:repeat-booking"));
    } catch (err) {
      console.warn("repeat booking failed", err);
    }
  };

  const closeModal = () => setRescheduleTarget(null);

  // Booking list is kept fresh via React Query invalidation from booking flows.



  const allowReschedule = tab === "upcoming" && !!rescheduleTarget?.can_reschedule;
  const allowCancel = tab === "upcoming" && !!rescheduleTarget?.can_cancel;

  const rescheduleModal = (
    <BookingDetailsModal
      booking={rescheduleTarget}
      masterMap={masterMap}
      onClose={closeModal}
      allowReschedule={tab === "upcoming" && allowReschedule}
      allowCancel={tab === "upcoming" && allowCancel}
      onRescheduleClick={rescheduleTarget ? () => {
        startReschedule(rescheduleTarget);
        closeModal();
      } : undefined}
      onCancelClick={rescheduleTarget ? () => cancel(rescheduleTarget.id) : undefined}
    />
  );

  return (
    <div className="tma-shell">
      <div className="tma-bg" />
      <div className="tma-container">
        <header className="tma-hero">
          <div className="tma-chip tma-chip-ghost">{t("my_visits_title")}</div>
          <p className="tma-subtle tma-hero-subtle">{t("my_visits_sub")}</p>
        </header>

        <div className="tma-tabs" style={{ margin: '12px 0' }}>
          <button type="button" className={`tma-tab ${tab === "upcoming" ? "active" : ""}`} onClick={() => setTab("upcoming")}>
            {`${t("tab_upcoming")}${upcomingCount ? ` (${upcomingCount})` : ""}`}
          </button>
          <button type="button" className={`tma-tab ${tab === "history" ? "active" : ""}`} onClick={() => setTab("history")}>
            {`${t("tab_history")}${historyCount ? ` (${historyCount})` : ""}`}
          </button>
        </div>

        <section className="tma-card tma-stack">

          {isLoading && <div className="tma-skeleton tma-skeleton--lg" />}
          {!isLoading && activeList.length === 0 && (
            <div className="tma-empty">{tab === "upcoming" ? t("no_upcoming") : t("no_history")}</div>
          )}
            {!isLoading && activeList.map((b: BookingItem) => (
              <BookingCard
                key={b.id}
                booking={b}
                masterName={b.master_id != null ? masterMap[b.master_id] : undefined}
                onOpen={() => {
                  setRescheduleTarget(b);
                  setNewSlot(b.starts_at || "");
                }}
                onCancel={tab === "upcoming" && b.can_cancel ? () => cancel(b.id) : undefined}
                onReschedule={tab === "upcoming" && b.can_reschedule ? () => {
                  setRescheduleTarget(b);
                  setNewSlot(b.starts_at || "");
                } : undefined}
                onRate={tab === "history" ? (rating) => rateMut.mutate({ booking_id: b.id, rating }) : undefined}
                onRepeat={tab === "history" ? undefined : () => repeatBooking(b)}
                disabledActions={tab === "history"}
              />
            ))}
        </section>
      </div>

      {rescheduleModal}
      <Toast message={toast?.message} tone={toast?.tone} onClose={() => setToast(null)} />
    </div>
  );
}

    

function toLocalInput(iso?: string) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const offset = d.getTimezoneOffset();
  const local = new Date(d.getTime() - offset * 60000);
  return local.toISOString().slice(0, 16);
}

export function BookingCard({ booking, masterName, onOpen, onCancel, onReschedule, onRate, onRepeat, disabledActions }: {
  booking: BookingItem;
  masterName?: string | undefined;
  onOpen?: () => void;
  onCancel?: () => void;
  onReschedule?: () => void;
  onRate?: (rating: number) => void;
  onRepeat?: () => void;
  disabledActions?: boolean;
}) {
  const canCancel = booking.can_cancel && !disabledActions;
  const canReschedule = booking.can_reschedule && !disabledActions;
  const isPast = !!booking.starts_at && new Date(booking.starts_at).getTime() < Date.now();
  const showRating = isPast && !booking.rated && !!onRate;
  const rawStatus = (booking.status || "").toString();
  const status = rawStatus.toLowerCase().replace(/-/g, "_").trim();
  const historyStatusEmoji = booking.status_emoji || ( /cancel/.test(status) || status === "canceled"
    ? "❌"
    : /no_show|no_show|no_show/.test(status) || /no.?show/.test(status)
      ? "🚫"
      : /done|completed|finished/.test(status)
        ? "✅"
        : "" );
  const compactDateTime = (booking.formatted_date ? `${booking.formatted_date}${booking.formatted_time_range ? ' • ' + booking.formatted_time_range : ''}` : "") || "";

  const {
    topLine,
    bottomLine,
    servicesArr,
    pricePart,
    statusEmoji: statusEmojiRaw,
    statusLabelText,
    statusClassName,
    svc,
  } = formatBookingCardData(booking, masterName);

  // Do not render inline emoji to avoid duplicates; icon-box shows single emoji
  const showEmoji = "";

  return (
    <div className="tma-mini-card tma-booking-card" onClick={() => onOpen && onOpen()} role="button" tabIndex={0}>
      <div className="tma-booking-head">
        <div className="tma-booking-row">
          <div className="tma-icon-box" style={{ marginLeft: -6 }}>
            {disabledActions ? (
              historyStatusEmoji ? <span style={{ fontSize: 15, lineHeight: 1, marginRight: 2 }}>{historyStatusEmoji}</span> : null
            ) : (
              <span style={{ fontSize: 15, lineHeight: 1, marginRight: 2 }}>{booking.payment_method === "online" ? "💳" : "💵"}</span>
            )}
          </div>
          <div className="tma-flex-1">
                <div className="u-flex u-items-center u-gap-1 u-justify-between">
                    <div className="u-flex u-items-center u-gap-1 u-minw-0">
                    {showEmoji ? (
                      <div style={{ fontSize: 22, lineHeight: 1 }}>{showEmoji}</div>
                    ) : (
                      <div style={{ width: 0 }} />
                    )}
                    <div className="u-flex-col u-gap-4 u-minw-0">
                      <div className="u-font-13 u-fw-700 u-ellipsis">{topLine}</div>
                      {/* Render each service on its own line (dynamically expand). Price shown as final line. */}
                      {servicesArr.length > 0 ? (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                          {servicesArr.map((ss, idx) => {
                            const isLast = idx === servicesArr.length - 1;
                            if (isLast) {
                              return (
                                <div key={idx} className="u-flex u-justify-between u-items-center">
                                  <div className="u-fw-700 u-font-14 u-ellipsis" style={{ minWidth: 0 }}>{ss}</div>
                                  <div className="u-fw-700 u-font-14">{`• ${pricePart}`.replace(/^•\s*/, '• ')}</div>
                                </div>
                              );
                            }
                            return (
                              <div key={idx} className="u-fw-700 u-font-14 u-ellipsis">{ss}</div>
                            );
                          })}
                          {/* If there are no services but price exists, show price */}
                          {servicesArr.length === 0 && bottomLine && <div className="u-fw-700 u-font-14">{`• ${bottomLine}`}</div>}
                        </div>
                      ) : (
                        <div className="u-fw-700 u-font-14 u-ellipsis">{(svc ? svc + (bottomLine ? ' • ' + bottomLine : '') : bottomLine)}</div>
                      )}
                    </div>
                  </div>
                  {false ? <div className={statusClassName + " tma-chip--compact"} style={{ marginLeft: 8 }}>{""}</div> : null}
                </div>
          </div>
        </div>
      </div>
      {/* Action buttons moved to modal — keep preview clean */}
    </div>
  );
}

function RatingStars({ onRate }: { onRate: (rating: number) => void }) {
  return (
    <div className="tma-rating">
      {[1, 2, 3, 4, 5].map((n) => (
            <button key={n} type="button" onClick={() => onRate(n)} aria-label={`${t("rating_label_prefix")} ${n}`}>
              ★
            </button>
          ))}
    </div>
  );
}

function formatBookingCardData(booking: BookingItem, masterName?: string) {
  // Prefer structured short label provided by backend; do not render bot-oriented `display_text`.
  const statusLabelText = booking.status_label || booking.status || "";
  const statusClassName = `tma-chip status-${(booking.status || "unknown").toString().replace(/\s+/g, "-")}`;

  const svc = booking.service_names || (t("service_default") as string);
  const servicesArr = (booking.service_names || "").split(",").map((s) => s.trim()).filter(Boolean);

  const dateTimeLabel = booking.formatted_date ? `${booking.formatted_date}${booking.formatted_time_range ? ' • ' + booking.formatted_time_range : ''}` : (booking.starts_at_formatted || "");
  const masterPart = masterName || (booking.master_id != null ? `Мастер #${booking.master_id}` : "");
  const pricePart = formatMoney(booking.price_cents, booking.currency || null, booking.price_formatted);

  const topParts: string[] = [];
  if (dateTimeLabel) topParts.push(dateTimeLabel);
  if (masterPart) topParts.push(masterPart);
  const topLine = topParts.join(" • ");

  const bottomParts: string[] = [];
  if (pricePart) bottomParts.push(pricePart);
  const bottomLine = bottomParts.join(" • ");

  // Prefer backend-provided emoji; frontend should not synthesize status emoji.
  const statusEmoji = booking.status_emoji || "";

  return { topLine, bottomLine, servicesArr, pricePart, statusEmoji, statusLabelText, statusClassName, svc };
}
