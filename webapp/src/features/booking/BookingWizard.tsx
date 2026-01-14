// src/features/booking/BookingWizard.tsx
import { useCallback, useEffect, useMemo, useRef, useState, useReducer } from "react";
import { createPortal } from "react-dom";
import { AnimatePresence, motion } from "framer-motion";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { tg, setMainButton, notifySuccess, haptic } from "../../lib/twa";
import { t, detectLang } from "../../i18n";
import {
  fetchServices,
  fetchMastersForService,
  fetchMastersMatch,
  fetchSlots,
  fetchAvailableDays,
  fetchMasterProfile,
  fetchServiceRanges,
  fetchPriceQuote,
  fetchBookings,
  rescheduleBooking,
  checkSlot,
  BookingRequest,
  BookingResponse,
  createHold,
  finalizeBooking,
  cancelBooking,
  ServiceOut,
  MasterOut,
  MasterProfile,
} from "../../api/booking";
import { formatDateTimeLabel, formatDate, formatTime, formatYMD, normalizeSlotString } from "../../lib/timezone";
import { formatMoneyFromCents } from "../../lib/money";

type Step = "SERVICE" | "MASTER" | "DATE" | "TIME" | "CONFIRM" | "SUCCESS";

type HoldDetails = {
  bookingId: number | null;
  expiresAt: string | null;
  originalPriceCents: number | null;
  finalPriceCents: number | null;
  discountAmountCents: number | null;
  discountPercent: number | null;
  currency: string | null;
  paymentMethod: "cash" | "online" | null;
};

type BookingDraft = {
  services: ServiceOut[];
  master: MasterOut | null;
  date: string;
  time: string;
  slot: string;
  customerInfo?: {
    name?: string;
    phone?: string;
    email?: string;
  } | null;
};

const steps: { id: Step; label: string; hint: string }[] = [
  { id: "SERVICE", label: "SERVICE", hint: "" },
  { id: "MASTER", label: "MASTER", hint: "" },
  { id: "DATE", label: "DATE", hint: "" },
  { id: "TIME", label: "TIME", hint: "" },
  { id: "CONFIRM", label: "CONFIRM", hint: "" },
];
const STEP_ORDER: Step[] = ["SERVICE", "MASTER", "DATE", "TIME", "CONFIRM", "SUCCESS"];
const DEFAULT_CURRENCY = "UAH";

const today = (() => {
  const d = new Date();
  return formatYMD(d.getFullYear(), d.getMonth() + 1, d.getDate());
})();

const SLIDE_PX = 50; // reduced slide distance for cheaper painting on mobile
const slideVariants = {
  enter: (direction: number) => ({ x: direction > 0 ? SLIDE_PX : -SLIDE_PX, opacity: 0 }),
  center: { x: 0, opacity: 1 },
  exit: (direction: number) => ({ x: direction > 0 ? -SLIDE_PX : SLIDE_PX, opacity: 0 }),
};

function useBookingFlow(initialStep: Step = "SERVICE") {
  const [step, setStep] = useState<Step>(initialStep);
  const [direction, setDirection] = useState(0);

  const goToStep = useCallback((next: Step) => {
    setStep((prev) => {
      const prevIdx = STEP_ORDER.indexOf(prev);
      const nextIdx = STEP_ORDER.indexOf(next);
      setDirection(nextIdx > prevIdx ? 1 : nextIdx < prevIdx ? -1 : 0);
      return next;
    });
  }, []);

  const goBack = useCallback(() => {
    setStep((prev) => {
      const prevIdx = STEP_ORDER.indexOf(prev);
      const targetIdx = Math.max(0, prevIdx - 1);
      const target = STEP_ORDER[targetIdx] ?? prev;
      setDirection(targetIdx < prevIdx ? -1 : 0);
      return target;
    });
  }, []);

  return { step, goToStep, goBack, direction } as const;
}

// Visual progress header (thin stepper)
function ProgressHeader({ currentStep }: { currentStep: Step }) {
  const stepIndex = STEP_ORDER.indexOf(currentStep);
  const visibleSteps = STEP_ORDER.filter((s) => s !== "SUCCESS");
  return (
    <div className="tma-progress-header">
      {visibleSteps.map((_, idx) => (
        <div
          key={idx}
          className={`tma-progress-bar ${idx <= stepIndex ? "tma-progress-bar--active" : ""}`}
        />
      ))}
    </div>
  );
}

// TimeWheel: iOS-like wheel picker for time slots
function TimeWheel({
  slots,
  selectedSlot,
  selectedDate,
  onCenterChange,
  onPick,
}: {
  slots: string[];
  selectedSlot: string;
  selectedDate: string;
  onCenterChange: (h: string, m: string) => void;
  onPick: (origSlot: string, display: string) => void;
}) {
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const ITEM_HEIGHT = 50;

  // Show slots exactly as provided by the backend.
  // Keep original `orig` string so onPick can send server-provided value.
  const entries = useMemo(() => {
    const out: { orig: string; norm: string; display: string }[] = [];
    for (const s of slots) {
      try {
        const norm = normalizeSlotString(s, selectedDate);
        const d = new Date(norm);
        if (Number.isNaN(d.getTime())) continue;
        // do not filter by minute grid here — show backend-provided slots verbatim
        out.push({ orig: s, norm, display: formatTime(s, selectedDate) });
      } catch (e) {
        // ignore
      }
    }
    // If no 15-min aligned slots found, fallback to all slots preserving original strings
    if (out.length === 0) {
      return slots.map((s) => ({ orig: s, norm: normalizeSlotString(s, selectedDate), display: formatTime(s, selectedDate) }));
    }
    return out;
  }, [slots, selectedDate]);

  // Scroll to the currently selected slot (if present) on mount / when slots change
  useEffect(() => {
    if (!scrollerRef.current) return;
    if (selectedSlot) {
      const idx = entries.findIndex((entry) => {
        try {
          const a = entry.norm || normalizeSlotString(entry.orig, selectedDate);
          const b = selectedSlot;
          return a === b || entry.orig === b;
        } catch (err) {
          return entry.orig === selectedSlot;
        }
      });
      if (idx !== -1) scrollerRef.current.scrollTop = idx * ITEM_HEIGHT;
    } else {
      scrollerRef.current.scrollTop = 0;
    }
  }, [scrollerRef, slots, selectedSlot, selectedDate]);

  const handleScroll = () => {
    if (!scrollerRef.current) return;
    const scrollTop = scrollerRef.current.scrollTop;
    const index = Math.round(scrollTop / ITEM_HEIGHT);
    if (index >= 0 && index < entries.length) {
      const entry = entries[index];
      const slot = entry.orig;
      const display = entry.display;
      const parts = (display || "").split(":");
      const h = parts[0] || "";
      const m = parts[1] || "00";
      try {
        onCenterChange(h, m);
        haptic.impact("selection");
      } catch (e) {}
    }
  };

  return (
    <div className="ios-picker-container">
      <div className="ios-picker-highlight" />
      <div
        ref={scrollerRef}
        className="ios-picker-scroller"
        onScroll={handleScroll}
      >
        {entries.map((entry) => {
          const slotOrig = entry.orig;
          const display = entry.display;
          let isSelected = false;
          try {
            const a = entry.norm || normalizeSlotString(slotOrig, selectedDate);
            isSelected = a === selectedSlot || slotOrig === selectedSlot || display === (selectedSlot ? formatTime(selectedSlot, selectedDate) : "");
          } catch (e) {
            isSelected = slotOrig === selectedSlot;
          }

          return (
            <div
              key={slotOrig}
              className={`ios-picker-item ${isSelected ? "selected" : ""}`}
              onClick={() => {
                onPick(slotOrig, display);
                const index = entries.findIndex((x) => x.orig === slotOrig);
                scrollerRef.current?.scrollTo({ top: index * ITEM_HEIGHT, behavior: "smooth" });
              }}
            >
              {display}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default function BookingWizard() {
  const { step, goToStep, direction } = useBookingFlow("SERVICE");
  const prevStepRef = useRef<Step | null>(null);

  // Notify outer app about current booking step so header can react (hide buttons on SUCCESS)
  useEffect(() => {
    try {
      window.dispatchEvent(new CustomEvent("tma:booking-step", { detail: step }));
    } catch (err) {}
    prevStepRef.current = step;
  }, [step]);
  const initialDraft: BookingDraft = { services: [], master: null, date: today, time: "", slot: "", customerInfo: null };
  const draftReducer = (state: BookingDraft, action: { type: "patch"; patch?: Partial<BookingDraft> } | { type: "reset" } | { type: "toggleService"; service: ServiceOut }) => {
    if (action.type === "patch") return { ...state, ...(action.patch || {}) };
    if (action.type === "toggleService") {
      const s = action.service;
      const exists = state.services.find((p) => p.id === s.id);
      const services = exists ? state.services.filter((p) => p.id !== s.id) : [...state.services, s];
      return { ...state, services };
    }
    return initialDraft;
  };

  const [bookingDraft, dispatchBookingDraft] = useReducer(draftReducer, initialDraft);
  const updateDraft = useCallback((patch: Partial<BookingDraft>) => dispatchBookingDraft({ type: "patch", patch }), []);
  const { services: selectedServices, master: selectedMaster, date: selectedDate, time: selectedTime, slot: selectedSlot } = bookingDraft;
  const [onlinePaymentsAvailable] = useState<boolean>(() => {
    try {
      const win: any = typeof window !== "undefined" ? window : {};
      const flag = win.__APP_ONLINE_PAYMENTS_AVAILABLE;
      if (typeof flag === "boolean") return flag;
    } catch (err) {}
    return true;
  });
  const [onlineDiscountPercentSetting] = useState<number | null>(() => {
    try {
      const win: any = typeof window !== "undefined" ? window : {};
      const raw = win.__APP_ONLINE_PAYMENT_DISCOUNT_PERCENT ?? win.__APP_ONLINE_DISCOUNT_PERCENT;
      const num = raw != null ? Number(raw) : null;
      if (typeof num === "number" && !Number.isNaN(num) && num > 0) return num;
    } catch (err) {}
    return null;
  });
  const [paymentMethod, setPaymentMethod] = useState<"cash" | "online">("cash");
  const [paymentStatus, setPaymentStatus] = useState<"idle" | "opening" | "pending" | "polling" | "paid" | "failed" | "cancelled">("idle");
  const [bookingId, setBookingId] = useState<number | null>(null);
  const [rescheduleBookingId, setRescheduleBookingId] = useState<number | null>(null);
  const [holdDetails, setHoldDetails] = useState<HoldDetails>({ bookingId: null, expiresAt: null, originalPriceCents: null, finalPriceCents: null, discountAmountCents: null, discountPercent: null, currency: null, paymentMethod: null });
  const {
    bookingId: holdBookingId,
    expiresAt: holdExpiresAt,
    originalPriceCents: holdOriginalPriceCents,
    finalPriceCents: holdFinalPriceCents,
    discountAmountCents: holdDiscountAmountCents,
    discountPercent: holdDiscountPercent,
    currency: holdCurrency,
    paymentMethod: holdPaymentMethod,
  } = holdDetails;
  const resetHold = useCallback(() => setHoldDetails({ bookingId: null, expiresAt: null, originalPriceCents: null, finalPriceCents: null, discountAmountCents: null, discountPercent: null, currency: null, paymentMethod: null }), []);
  const updateHold = useCallback((partial: Partial<HoldDetails>) => setHoldDetails((prev) => ({ ...prev, ...partial })), []);
  const [bookingDurationMinutes, setBookingDurationMinutes] = useState<number | null>(null);
  const [viewYearMonth, setViewYearMonth] = useState<{ year: number; month: number }>(() => {
    const d = new Date();
    return { year: d.getFullYear(), month: d.getMonth() + 1 };
  });
  // Prefetch selected master profile so we can show per-master duration overrides in the preview
  const { data: selectedMasterProfile } = useQuery({
    queryKey: ["selected-master-profile", selectedMaster?.id],
    enabled: Boolean(selectedMaster?.id),
    queryFn: () => fetchMasterProfile(selectedMaster!.id),
    staleTime: 5 * 60 * 1000,
  });
  const activeIndex = useMemo(() => {
    const idx = steps.findIndex((s: { id: Step }) => s.id === step);
    return idx >= 0 ? idx : 0;
  }, [step]);
  const selectedService = useMemo(() => selectedServices[0] || null, [selectedServices]);
  const selectedServiceIds = useMemo(() => selectedServices.map((s: ServiceOut) => s.id), [selectedServices]);
  const { data: priceQuote, isFetching: priceQuoteLoading } = useQuery({
    queryKey: ["price-quote", selectedServiceIds, paymentMethod, selectedMaster?.id],
    enabled: selectedServiceIds.length > 0,
    queryFn: () =>
      fetchPriceQuote({
        service_ids: selectedServiceIds,
        payment_method: paymentMethod,
        master_id: selectedMaster?.id ?? null,
      }),
    staleTime: 30 * 1000,
  });

  useEffect(() => {
    if (prevStepRef.current && prevStepRef.current !== step) {
      haptic.impact("selection");
    }
    prevStepRef.current = step;
  }, [step]);

  useEffect(() => {
    if (!onlinePaymentsAvailable && paymentMethod === "online") {
      setPaymentMethod("cash");
    }
  }, [onlinePaymentsAvailable, paymentMethod]);

  const currencyCode = (priceQuote as any)?.currency || (typeof window !== "undefined" && (window as any).__APP_CURRENCY) || DEFAULT_CURRENCY;
  const [serviceAvailability, setServiceAvailability] = useState<Record<string, boolean>>({});
  // `prevStepRef` is declared above to track previous step for haptics/holds
  const mainButtonHandlerRef = useRef<(() => void) | null>(null);
  const selectedServicesNames = useMemo(() => selectedServices.map((s: ServiceOut) => s.name).join(", "), [selectedServices]);
  // Do NOT compute prices on the client. Price must come from server `priceQuote`.
  // Duration must be provided by the server via `priceQuote.duration_minutes` or
  // from the created booking (`bookingDurationMinutes`). Do not sum service durations
  // on the client to avoid divergence from backend logic.
  const durationLabel = useMemo(() => {
    const fromBooking = bookingDurationMinutes;
    const fromQuote = (priceQuote as any)?.duration_minutes ?? null;
    if (fromBooking && fromBooking > 0) return `${fromBooking} ${t("minutes_short")}`;
    if (fromQuote && fromQuote > 0) return `${fromQuote} ${t("minutes_short")}`;
    return `60 ${t("minutes_short")}`;
  }, [bookingDurationMinutes, priceQuote]);
  const isRescheduling = useMemo(() => Boolean(rescheduleBookingId), [rescheduleBookingId]);
  const [toast, setToast] = useState<{ message: string; tone?: "error" | "success" } | null>(null);
  const [tick, setTick] = useState<number>(0);
  const paymentMessage = useMemo(() => {
    if (paymentStatus === "opening") return t("awaiting_payment") || "Opening payment window…";
    if (paymentStatus === "polling") return t("awaiting_payment") || "Awaiting payment confirmation…";
    if (paymentStatus === "pending") return t("awaiting_payment") || "Awaiting payment confirmation…";
    if (paymentStatus === "paid") return t("payment_success") || "Payment succeeded.";
    if (paymentStatus === "failed") return t("payment_failed") || "Payment failed. Please try again.";
    if (paymentStatus === "cancelled") return t("payment_cancelled") || "Payment cancelled.";
    return "";
  }, [paymentStatus]);

  const { data: services, isLoading: servicesLoading } = useQuery({
    queryKey: ["services"],
    queryFn: fetchServices,
  });

  
  const [serviceRanges, setServiceRanges] = useState<Record<string, { min_duration: number | null; max_duration: number | null; min_price_cents: number | null; max_price_cents: number | null }>>({});

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!services || services.length === 0) return;
        const ids = services.map((s) => s.id);
        const ranges = await fetchServiceRanges(ids);
        if (!cancelled) setServiceRanges(ranges || {});
      } catch (e) {
        console.warn("fetchServiceRanges failed", e);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [services]);

  useEffect(() => {
    if (!services || services.length === 0) return;
    let cancelled = false;
    (async () => {
      const entries = await Promise.all(
        services.map(async (s) => {
          try {
            const mastersForService = await fetchMastersForService(s.id);
            return [s.id, (mastersForService || []).length > 0] as const;
          } catch (err) {
            console.warn("masters check failed", s.id, err);
            return [s.id, true] as const; // fallback: show if check failed
          }
        })
      );
      if (!cancelled) {
        setServiceAvailability(Object.fromEntries(entries));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [services]);

  const { data: masters, isLoading: mastersLoading } = useQuery({
    queryKey: ["masters", selectedServiceIds],
    enabled: selectedServiceIds.length > 0,
    queryFn: async () => {
      const matched = await fetchMastersMatch(selectedServiceIds);
      const list = matched || [];
      return list;
    },
    staleTime: 5 * 60 * 1000,
  });

  // Prefetch lightweight master profiles so we can show rating/orders in the list
  const { data: masterProfilesMap } = useQuery({
    queryKey: ["master-profiles", masters?.map((m) => m.id)],
    enabled: Boolean(masters && masters.length > 0),
    queryFn: async () => {
      const arr = await Promise.all(
        (masters || []).map(async (m) => {
          try {
            const p = await fetchMasterProfile(m.id);
            return [m.id, p] as const;
          } catch (err) {
            return [m.id, null] as const;
          }
        })
      );
      return Object.fromEntries(arr);
    },
    staleTime: 5 * 60 * 1000,
  });

  const mastersWithMeta = useMemo(() => {
    if (!masters) return [] as (MasterOut & Partial<Pick<MasterProfile, "rating" | "completed_orders">>)[];
    return masters.map((m) => ({
      ...m,
      rating: masterProfilesMap?.[m.id]?.rating,
      completed_orders: masterProfilesMap?.[m.id]?.completed_orders,
    }));
  }, [masters, masterProfilesMap]);

  const applyReschedulePayload = useCallback((payload: any) => {
    if (!payload) return;
    if (!services || services.length === 0) return;

    resetHold();
    setPaymentStatus("idle");
    if (payload.booking_id) {
      setRescheduleBookingId(Number(payload.booking_id));
      setBookingId(Number(payload.booking_id));
    } else {
      setRescheduleBookingId(null);
    }

    const service_ids: string[] = [];
    if (Array.isArray(payload.service_ids) && payload.service_ids.length > 0) {
      for (const sid of payload.service_ids) {
        const svc = services.find((s) => s.id === String(sid));
        if (svc) service_ids.push(svc.id);
      }
    } else if (Array.isArray(payload.service_names) && payload.service_names.length > 0) {
      const wanted = payload.service_names.map((n: string) => String(n).trim().toLowerCase());
      for (const s of services) {
        if (wanted.includes((s.name || "").toLowerCase())) service_ids.push(s.id);
      }
    } else if (typeof payload.service_names === "string" && payload.service_names.trim().length > 0) {
      const wanted = payload.service_names.split(",").map((n: string) => String(n).trim().toLowerCase()).filter(Boolean);
      for (const s of services) {
        if (wanted.includes((s.name || "").toLowerCase())) service_ids.push(s.id);
      }
    }

    if (service_ids.length > 0) {
      const selected = services.filter((s) => service_ids.includes(s.id));
      if (selected.length > 0) updateDraft({ services: selected });
    }

    if (typeof payload.master_id === "number") {
      const m = (masters || []).find((mm) => mm.id === payload.master_id) || null;
      if (m) updateDraft({ master: m });
      else updateDraft({ master: { id: payload.master_id, name: payload.master_name || (t("master_default") as string) } as MasterOut });
    }

    if (payload.starts_at) {
      try {
        const d = new Date(payload.starts_at);
        if (!Number.isNaN(d.getTime())) {
          const iso = formatYMD(d.getFullYear(), d.getMonth() + 1, d.getDate());
          updateDraft({ date: iso, time: "", slot: "" });
          setViewYearMonth({ year: d.getFullYear(), month: d.getMonth() + 1 });
        }
      } catch (err) {}
    }

    goToStep("DATE");
  }, [services, masters, resetHold, goToStep, updateDraft]);

  // Consolidated initialization effect: handle __REPEAT_BOOKING, __RESCHEDULE_BOOKING
  // and `tma:reschedule-start` events in a single place to avoid race
  // conditions and repeated state-resetting across multiple effects.
  useEffect(() => {
    const win = window as any;

    const consumeRepeat = () => {
      try {
        const payload = win.__REPEAT_BOOKING;
        if (!payload) return false;
        if (!services || services.length === 0) return false;

        const service_ids: string[] = [];
        if (Array.isArray(payload.service_ids) && payload.service_ids.length > 0) {
          for (const sid of payload.service_ids) {
            const svc = services.find((s) => s.id === String(sid));
            if (svc) service_ids.push(svc.id);
          }
        } else if (Array.isArray(payload.service_names) && payload.service_names.length > 0) {
          const wanted = payload.service_names.map((n: string) => String(n).trim().toLowerCase());
          for (const s of services) {
            if (wanted.includes((s.name || "").toLowerCase())) service_ids.push(s.id);
          }
        }

        if (service_ids.length > 0) {
          const selected = services.filter((s) => service_ids.includes(s.id));
          if (selected.length > 0) updateDraft({ services: selected });
        }

        if (typeof payload.master_id === "number") {
          const m = (masters || []).find((mm) => mm.id === payload.master_id) || null;
          if (m) updateDraft({ master: m });
          else updateDraft({ master: { id: payload.master_id, name: payload.master_name || (t("master_default") as string) } as MasterOut });
        }

        goToStep("DATE");
        try { delete win.__REPEAT_BOOKING; } catch (e) {}
        return true;
      } catch (e) {
        return false;
      }
    };

    const consumeReschedule = () => {
      try {
        const payload = win.__RESCHEDULE_BOOKING;
        if (!payload) return false;
        applyReschedulePayload(payload);
        try { delete win.__RESCHEDULE_BOOKING; } catch (e) {}
        return true;
      } catch (e) {
        return false;
      }
    };

    // Attempt to consume both repeat and reschedule payloads once on mount.
    consumeRepeat();
    consumeReschedule();

    // Event listener for explicit reschedule-start events
    const handler = (evt: Event) => {
      const payload = (evt as CustomEvent)?.detail || (window as any).__RESCHEDULE_BOOKING;
      try { (window as any).__RESCHEDULE_BOOKING = payload; } catch (e) {}
      applyReschedulePayload(payload);
    };
    window.addEventListener("tma:reschedule-start", handler as EventListener);
    return () => window.removeEventListener("tma:reschedule-start", handler as EventListener);
  }, [services, masters, applyReschedulePayload, goToStep, updateDraft]);

  useEffect(() => {
    if (!selectedMaster) return;
    if (masters && !masters.find((m) => m.id === selectedMaster.id)) {
      updateDraft({ master: null });
    }
  }, [masters, selectedMaster, updateDraft]);

  // Prefer hold-provided pricing when present (authoritative server-side values)
  const displayCurrency = holdCurrency || priceQuote?.currency || DEFAULT_CURRENCY;

  // Price comes only from server responses: hold/finalize for fact, priceQuote for preview.
  const { finalPriceCents, originalPriceCents, discountAmountCents } = useMemo(() => {
    const quoteBase = priceQuote?.original_price_cents ?? null;
    const quoteFinal = priceQuote?.final_price_cents ?? null;
    const quoteDiscount = priceQuote?.discount_amount_cents ?? null;
    const holdMatchesMethod = holdPaymentMethod ? holdPaymentMethod === paymentMethod : false;

    const base = holdMatchesMethod ? (holdOriginalPriceCents ?? quoteBase) : (quoteBase ?? holdOriginalPriceCents);
    const final = holdMatchesMethod ? (holdFinalPriceCents ?? quoteFinal) : (quoteFinal ?? holdFinalPriceCents ?? quoteFinal);
    const discount = holdMatchesMethod ? (holdDiscountAmountCents ?? quoteDiscount) : (quoteDiscount ?? holdDiscountAmountCents);

    return { finalPriceCents: final ?? null, originalPriceCents: base ?? null, discountAmountCents: discount ?? null };
  }, [holdOriginalPriceCents, holdFinalPriceCents, holdDiscountAmountCents, holdDiscountPercent, holdPaymentMethod, priceQuote, paymentMethod]);

  const onlineDiscountPercent = useMemo(() => {
    const fromQuote = (priceQuote as any)?.discount_percent ?? (priceQuote as any)?.discount_percent_applied;
    if (typeof fromQuote === "number" && !Number.isNaN(fromQuote) && fromQuote > 0) return fromQuote;
    if (typeof holdDiscountPercent === "number" && !Number.isNaN(holdDiscountPercent) && holdDiscountPercent > 0) return holdDiscountPercent;
    if (typeof onlineDiscountPercentSetting === "number" && onlineDiscountPercentSetting > 0) return onlineDiscountPercentSetting;
    return null;
  }, [priceQuote, holdDiscountPercent, onlineDiscountPercentSetting]);
  const { data: availableDaysData } = useQuery({
    queryKey: ["available-days", selectedMaster?.id, viewYearMonth.year, viewYearMonth.month, selectedServiceIds],
    // Allow fetching availability for a master even if no services are selected
    enabled: Boolean(selectedMaster),
    queryFn: () =>
      fetchAvailableDays({
        master_id: selectedMaster!.id,
        year: viewYearMonth.year,
        month: viewYearMonth.month,
        service_ids: selectedServiceIds,
      }),
    // Keep availability fresh — don't cache longer than ~2 minutes
    staleTime: 120_000,
    refetchOnWindowFocus: true,
    refetchOnMount: "always",
  });

  const { data: slotsData, isLoading: slotsLoading, refetch: refetchSlots } = useQuery({
    queryKey: ["slots", selectedMaster?.id, selectedDate, selectedServiceIds],
    queryFn: () =>
      fetchSlots({
        master_id: selectedMaster!.id,
        date_param: selectedDate,
        service_ids: selectedServiceIds,
      }),
    enabled: Boolean(selectedDate) && selectedServiceIds.length > 0 && Boolean(selectedMaster),
    // Keep slots fresh — stale after 60s so UI asks server frequently
    staleTime: 60_000,
    refetchOnWindowFocus: true,
    refetchOnMount: "always",
  });
  const queryClient = useQueryClient();
  const handlePaymentMethodChange = useCallback((method: "cash" | "online") => {
    if (paymentMethod === method) return;
    setPaymentMethod(method);
    try {
      queryClient.invalidateQueries({ queryKey: ["price-quote"] });
    } catch (err) {}
  }, [paymentMethod, queryClient]);

  // Prefer server-provided timezone: force UI to use salon timezone when available
  useEffect(() => {
    try {
      const win = window as any;
      if ((slotsData as any)?.timezone) win.__SERVER_TZ = (slotsData as any).timezone;
      else if ((availableDaysData as any)?.timezone) win.__SERVER_TZ = (availableDaysData as any).timezone;
    } catch (err) {
      // ignore
    }
  }, [slotsData?.timezone, availableDaysData?.timezone]);

  const bookingMutation = useMutation<BookingResponse, unknown, BookingRequest>({
    mutationFn: createHold,
    onSuccess: (data) => {
      if (data.ok) {
        if (data.booking_id) {
          setBookingId(data.booking_id);
        }
        if (data.master_id || data.master_name) {
          const current = bookingDraft.master;
          let next = current;
          if (current && data.master_id && current.id === data.master_id) {
            next = { ...current, name: data.master_name || current.name } as MasterOut;
          } else if (data.master_id) {
            next = { id: data.master_id, name: data.master_name || current?.name || (t("master_default") as string) } as MasterOut;
          } else if (data.master_name && current) {
            next = { ...current, name: data.master_name } as MasterOut;
          }
          updateDraft({ master: next });
        }
        if (data.duration_minutes) {
          setBookingDurationMinutes(data.duration_minutes);
        }
        if (data.starts_at) {
          const d = new Date(data.starts_at);
            if (!Number.isNaN(d.getTime())) {
              const isoDate = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
              const timeLabel = formatTime(data.starts_at, isoDate);
              updateDraft({ date: isoDate, time: timeLabel });
            }
        }
          if (data.invoice_url) {
            handlePayment(data.invoice_url);
          } else {
          // Если сервер сразу подтвердил/поставил pending, считаем шаг успешным
          if (data.status === "pending_payment") {
            setPaymentStatus("pending");
            setToast({ message: t("awaiting_payment") as string, tone: "success" });
          } else if (data.status === "confirmed" || data.status === "paid") {
            if (data.status === "paid") setPaymentStatus("paid");
            notifySuccess();
          }
          goToStep("SUCCESS");
          tg.MainButton.hide();
        }
      } else {
        if (data.error === "slot_unavailable") {
          setToast({ message: t("slot_taken") as string, tone: "error" });
          goToStep("DATE");
          refetchSlots();
          haptic.notify("error");
        } else {
          setToast({ message: data.error || (t("booking_failed") as string), tone: "error" });
          refetchSlots();
        }
      }
    },
    onError: (err: unknown) => {
      const res = (err as any)?.response;
      const detail = res?.data?.detail || res?.data?.error || res?.status;
      console.error("booking error", err);
      setToast({ message: detail ? `Ошибка: ${detail}` : (t("network_unavailable") as string), tone: "error" });
      refetchSlots();
      setPaymentStatus("failed");
    },
  });

  const rescheduleMutation = useMutation({
    mutationFn: rescheduleBooking,
    onSuccess: (resp) => {
      if (resp.ok) {
        notifySuccess();
        if (resp.booking_id) setBookingId(resp.booking_id);
        setRescheduleBookingId(null);
        setPaymentStatus("idle");
        resetHold();

        // Update UI with confirmed slot if backend echoed it back
        if (resp.starts_at) {
          try {
            const d = new Date(resp.starts_at);
              if (!Number.isNaN(d.getTime())) {
              const isoDate = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
              const timeLabel = formatTime(resp.starts_at, isoDate);
              updateDraft({ date: isoDate, time: timeLabel });
            }
          } catch (err) {}
        }

        try { queryClient.invalidateQueries({ queryKey: ["bookings"] }); } catch (e) {}
        goToStep("SUCCESS");
      } else {
        setToast({ message: resp.error || (t("reschedule_failed") as string), tone: "error" });
        refetchSlots();
        if (resp.error === "slot_unavailable") {
          goToStep("DATE");
          haptic.notify("error");
        }
      }
    },
    onError: () => setToast({ message: (t("network_unavailable") as string), tone: "error" }),
  });

  const bookingSubmitting = bookingMutation.isPending;

  const handlePayment = useCallback((invoiceUrl: string, opts?: { resetHoldAfter?: boolean }) => {
    if (!invoiceUrl) return;
    if (!tg?.openInvoice) {
      setPaymentStatus("failed");
      setToast({ message: t("payment_unavailable_version") as string, tone: "error" });
      return;
    }

    setPaymentStatus("opening");

    try {
      (tg as any).openInvoice(invoiceUrl, (status: any) => {
        const s = String(status).toLowerCase();
        if (s === "paid") {
          setPaymentStatus("paid");
          notifySuccess();
          try { queryClient.invalidateQueries({ queryKey: ["bookings"] }); } catch (e) {}
          if (opts?.resetHoldAfter) {
            try { resetHold(); } catch (e) {}
          }
          goToStep("SUCCESS");
          try { tg?.MainButton?.hide(); } catch (e) {}
        } else if (s === "cancelled") {
          setPaymentStatus("cancelled");
          setToast({ message: t("payment_cancelled") as string, tone: "error" });
        } else if (s === "failed") {
          setPaymentStatus("failed");
          setToast({ message: t("payment_failed") as string, tone: "error" });
        } else {
          setPaymentStatus("pending");
        }
      });
    } catch (err) {
      console.warn("openInvoice callback failed", err);
      setPaymentStatus("pending");
    }
  }, [goToStep, notifySuccess, queryClient, resetHold]);

  const handleContinueFromService = useCallback(() => {
    if (selectedServices.length === 0) return;
    updateDraft({ master: null, date: today, time: "", slot: "" });
    setPaymentMethod("cash");
    setBookingId(null);
    setBookingDurationMinutes(null);
    goToStep("MASTER");
    // lightweight impact when user proceeds from service selection
    haptic.impact("light");
  }, [selectedServices, goToStep, updateDraft]);

  const goToTimeClearingHold = useCallback(async () => {
    if (holdBookingId) {
      try { await cancelBooking(holdBookingId); } catch (err) {}
      resetHold();
      setPaymentStatus("idle");
    }
    goToStep("TIME");
  }, [holdBookingId, resetHold, goToStep]);

  const handleBack = useCallback(async () => {
    try {
      if (step === "CONFIRM") {
        await goToTimeClearingHold();
        return;
      }
    } catch (err) {
      // ignore
    }

    if (step === "MASTER") goToStep("SERVICE");
    else if (step === "DATE") goToStep("MASTER");
    else if (step === "TIME") goToStep("DATE");
  }, [step, goToStep, goToTimeClearingHold]);

  useEffect(() => {
    const backHandler = async () => {
      await handleBack();
    };
    tg.BackButton.offClick(backHandler);

    if (step === "SERVICE" || step === "SUCCESS") {
      tg.BackButton.hide();
      return () => tg.BackButton.offClick(backHandler);
    }

    tg.BackButton.show();
    tg.BackButton.onClick(backHandler);

    return () => tg.BackButton.offClick(backHandler);
  }, [step, handleBack]);

  useEffect(() => {
    const detachHandler = () => {
      if (mainButtonHandlerRef.current && tg?.MainButton?.offClick) {
        try {
          tg.MainButton.offClick(mainButtonHandlerRef.current);
        } catch (err) {
          // ignore
        }
      }
      mainButtonHandlerRef.current = null;
    };

    try {
      detachHandler();
      if (!tg?.MainButton) return;

      let label: string | null = null;
      let enabled = false;
      let handler: (() => void) | null = null;

      switch (step) {
        case "SERVICE": {
          if (selectedServices.length > 0) {
            const priceLabel = priceQuoteLoading ? "…" : finalPriceCents != null ? formatMoneyFromCents(finalPriceCents, displayCurrency, 0) : "—";
            // Show total count, total duration and total price on the MainButton
            label = `${t("continue")} (${selectedServices.length}) ${durationLabel} • ${priceLabel}`;
            enabled = true;
            handler = () => handleContinueFromService();
          }
          break;
        }
        case "TIME": {
          if (selectedSlot || selectedTime) {
            label = String(t("next")) || "Next";
            enabled = true;
            handler = () => goToStep("CONFIRM");
          }
          break;
        }
        case "CONFIRM": {
          if (((selectedServiceIds.length > 0) || isRescheduling) && selectedDate && (selectedSlot || selectedTime)) {
            if (isRescheduling && rescheduleBookingId) {
              label = String(t("reschedule_label") || "Перенести");
              enabled = !rescheduleMutation.isPending;
              handler = async () => {
                try {
                  let finalSlot: string;
                  if (selectedSlot) {
                    finalSlot = selectedSlot;
                  } else {
                    const normalized = normalizeSlotString(selectedTime, selectedDate);
                    const parsed = new Date(normalized);
                    finalSlot = Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();
                  }

                  if (!selectedMaster) {
                    setToast({ message: t("choose_master") as string, tone: "error" });
                    goToStep("MASTER");
                    return;
                  }

                  try {
                    if (selectedServiceIds.length > 0) {
                      const chk = await checkSlot({ master_id: selectedMaster.id, slot: finalSlot, service_ids: selectedServiceIds });
                      if (!chk.available) {
                        setToast({ message: t("slot_taken") as string, tone: "error" });
                        refetchSlots();
                        goToStep("DATE");
                        return;
                      }
                    }
                  } catch (err) {
                    // if checkSlot fails, still attempt reschedule and surface backend error
                  }
                  await rescheduleMutation.mutateAsync({ booking_id: rescheduleBookingId, new_slot: finalSlot });
                } catch (err) {
                  setToast({ message: (t("reschedule_failed") as string), tone: "error" });
                  refetchSlots();
                }
              };
            } else {
              if (paymentStatus === "pending" || paymentStatus === "opening") {
                label = String(t("awaiting_payment"));
              } else if (paymentMethod === "online") {
                label = String(t("pay_label")) || "Оплатить";
              } else {
                label = String(t("confirm")) || "Записаться";
              }
              enabled = paymentStatus !== "pending" && paymentStatus !== "opening" && !bookingSubmitting;
              handler = async () => {
                if (bookingSubmitting) return;
                try {
                  let finalSlot: string;
                  if (selectedSlot) {
                    finalSlot = selectedSlot; // server-provided slot string (preserves timezone)
                  } else {
                    const normalized = normalizeSlotString(selectedTime, selectedDate);
                    const parsed = new Date(normalized);
                    finalSlot = Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();
                  }

                  if (holdBookingId) {
                    try {
                      const resp = await finalizeBooking({ booking_id: holdBookingId, payment_method: paymentMethod });
                      if (resp.ok) {
                        if (resp.invoice_url) {
                          if (resp.booking_id) setBookingId(resp.booking_id);
                          if (resp.duration_minutes) setBookingDurationMinutes(resp.duration_minutes);
                          updateHold({
                            originalPriceCents: resp.original_price_cents ?? holdOriginalPriceCents,
                            finalPriceCents: resp.final_price_cents ?? holdFinalPriceCents,
                            discountAmountCents: resp.discount_amount_cents ?? holdDiscountAmountCents,
                            discountPercent: (resp as any)?.discount_percent ?? (resp as any)?.discount_percent_applied ?? holdDiscountPercent ?? null,
                            currency: resp.currency ?? holdCurrency,
                            paymentMethod: resp.payment_method ?? holdPaymentMethod ?? paymentMethod,
                          });

                          if (!tg?.openInvoice) {
                            setPaymentStatus("failed");
                            setToast({ message: t("payment_unavailable_version") as string, tone: "error" });
                            return;
                          }

                          setPaymentStatus("opening");

                          try {
                            handlePayment(resp.invoice_url, { resetHoldAfter: true });
                          } catch (err) {
                            console.warn("openInvoice callback attempt failed", err);
                            setPaymentStatus("pending");
                          }

                          return;
                        }

                        if (resp.booking_id) setBookingId(resp.booking_id);
                        if (resp.duration_minutes) setBookingDurationMinutes(resp.duration_minutes);
                        if (resp.starts_at) {
                          const d = new Date(resp.starts_at);
                          if (!Number.isNaN(d.getTime())) {
                            const isoDate = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
                            const timeLabel = formatTime(resp.starts_at, isoDate);
                            updateDraft({ date: isoDate, time: timeLabel });
                          }
                        }
                        resetHold();
                        try { queryClient.invalidateQueries({ queryKey: ["bookings"] }); } catch (e) {}
                        notifySuccess();
                        goToStep("SUCCESS");
                      } else {
                        if (resp.error === "slot_unavailable") {
                          setToast({ message: t("slot_taken") as string, tone: "error" });
                          resetHold();
                          refetchSlots();
                          haptic.notify("error");
                          goToStep("DATE");
                        } else {
                          setToast({ message: resp.error || (t("booking_failed") as string), tone: "error" });
                          refetchSlots();
                        }
                      }
                    } catch (err) {
                      setToast({ message: (t("booking_failed") as string), tone: "error" });
                      refetchSlots();
                    }
                  } else {
                    if (!selectedMaster) {
                      setToast({ message: t("choose_master") as string, tone: "error" });
                      goToStep("MASTER");
                      return;
                    }

                    const chk = await checkSlot({ master_id: selectedMaster.id, slot: finalSlot, service_ids: selectedServiceIds });
                    if (!chk.available) {
                      setToast({ message: t("slot_taken") as string, tone: "error" });
                      refetchSlots();
                      return;
                    }

                    bookingMutation.mutate({
                      master_id: selectedMaster.id,
                      service_ids: selectedServiceIds,
                      slot: finalSlot,
                      payment_method: paymentMethod,
                    });
                  }
                } catch (err) {
                  setToast({ message: (t("reserve_failed") as string), tone: "error" });
                  refetchSlots();
                }
              };
            }
          }
          break;
        }
        case "SUCCESS": {
          label = String(t("close"));
          enabled = true;
          handler = () => tg.close();
          break;
        }
        default:
          break;
      }

      if (label && handler) {
        mainButtonHandlerRef.current = handler;
        try {
          // Try to set a brighter accent color for the MainButton when available
          try {
            const accent = getComputedStyle(document.documentElement).getPropertyValue("--t-accent")?.trim();
                if (accent) {
              try {
                if (typeof tg.MainButton.setParams === "function") {
                  tg.MainButton.setParams({ color: accent });
                } else if (typeof (tg.MainButton as any).setColor === "function") {
                  // older sdk variants
                  (tg.MainButton as any).setColor(accent);
                }
              } catch (e) {
                // ignore color set errors
              }
            }
          } catch (err) {}

          tg.MainButton.setText(label);
          if (tg.MainButton.offClick) tg.MainButton.offClick(handler);
          if (enabled) {
            tg.MainButton.show();
            tg.MainButton.onClick(handler);
          } else {
            tg.MainButton.hide();
          }
        } catch (err) {
          // ignore twa errors
        }
      } else {
        try {
          tg.MainButton.hide();
        } catch (err) {}
      }
    } catch (err) {
      console.warn("MainButton control error", err);
    }

    return () => {
      try {
        detachHandler();
        if (tg?.MainButton) tg.MainButton.hide();
      } catch (err) {}
    };
  }, [
    step,
    selectedMaster,
    selectedServiceIds,
    selectedDate,
    selectedTime,
    selectedSlot,
    bookingMutation,
    paymentStatus,
    selectedServices.length,
    finalPriceCents,
    priceQuoteLoading,
    handleContinueFromService,
    paymentMethod,
    resetHold,
    updateHold,
    holdOriginalPriceCents,
    holdFinalPriceCents,
    refetchSlots,
    goToStep,
    bookingSubmitting,
    isRescheduling,
    rescheduleBookingId,
    rescheduleMutation,
  ]);

  useEffect(() => {
    if (selectedMaster && selectedDate && selectedServiceIds.length > 0) {
      refetchSlots();
    }
  }, [selectedMaster, selectedDate, selectedServiceIds, refetchSlots]);

  

  // When user returns to TIME, refresh availability to keep wheel current.
  useEffect(() => {
    if (step === "TIME") {
      try {
        // Invalidate slots cache so re-fetch uses latest availability
        queryClient.invalidateQueries({ queryKey: ["slots"] });
      } catch (err) {}
      try {
        // Also trigger an immediate refetch so UI updates right away
        refetchSlots();
      } catch (err) {}
    }
  }, [step, queryClient, refetchSlots]);

  // Best-effort: cancel hold when Mini App is hidden/closed
  useEffect(() => {
    const onHidden = () => {
      if (holdBookingId) {
        cancelBooking(holdBookingId).catch(() => {});
        resetHold();
      }
    };
    const onBeforeUnload = () => {
      if (holdBookingId) {
        try {
          const url = "/api/cancel";
          const payload = JSON.stringify({ booking_id: holdBookingId });
          if (navigator.sendBeacon) {
            navigator.sendBeacon(url, payload);
          } else {
            fetch(url, { method: "POST", body: payload, credentials: "same-origin", headers: { "Content-Type": "application/json" } });
          }
        } catch (err) {}
      }
    };
    document.addEventListener("visibilitychange", onHidden);
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => {
      document.removeEventListener("visibilitychange", onHidden);
      window.removeEventListener("beforeunload", onBeforeUnload);
    };
  }, [holdBookingId, resetHold]);

  // Countdown for hold expiry — forces re-render each second and auto-refreshes slots when expired
  useEffect(() => {
    if (!holdExpiresAt) return undefined;
    let interval: ReturnType<typeof setInterval> | null = setInterval(() => {
      const remain = Math.floor((new Date(holdExpiresAt).getTime() - Date.now()) / 1000);
      if (remain <= 0) {
        if (interval) clearInterval(interval);
        resetHold();
        setToast({ message: t("hold_expired") as string, tone: "error" });
        refetchSlots();
        goToTimeClearingHold();
      } else {
        setTick((t) => t + 1);
      }
    }, 1000);
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [holdExpiresAt, refetchSlots, resetHold]);

  useEffect(() => {
    if (!toast) return undefined;
    const timer = setTimeout(() => setToast(null), 2800);
    return () => clearTimeout(timer);
  }, [toast]);

  const heading = (() => {
    if (isRescheduling && (step === "CONFIRM" || step === "TIME" || step === "DATE")) {
      return (t("reschedule_title") as string) || "Перенос записи";
    }
    if (step === "SERVICE") return t("heading_service") as string;
    if (step === "MASTER") return t("heading_master") as string;
    if (step === "DATE") return t("heading_date") as string;
    if (step === "TIME") return `${t("heading_time_prefix")} ${formatDate(selectedDate)}`;
    if (step === "CONFIRM") return t("heading_confirm") as string;
    return t("heading_done") as string;
  })();
  const heroTitle = isRescheduling
    ? ((t("reschedule_title") as string) || "Перенос записи")
    : (t("hero_sub") as string);
  const heroSub = isRescheduling
    ? ((t("reschedule_sub") as string) || (t("hero_sub") as string))
    : "";

  const holdRemainingSeconds = holdExpiresAt ? Math.max(0, Math.floor((new Date(holdExpiresAt).getTime() - Date.now()) / 1000)) : null;
  const holdRemainingLabel = holdRemainingSeconds !== null ? `${String(Math.floor(holdRemainingSeconds / 60)).padStart(2, "0")}:${String(holdRemainingSeconds % 60).padStart(2, "0")}` : null;

  const [profileMaster, setProfileMaster] = useState<{ master: MasterOut; anchorEl?: HTMLElement | null } | null>(null);
  const { data: profileData, isFetching: profileLoading } = useQuery({
    queryKey: ["master-profile", profileMaster?.master?.id],
    enabled: Boolean(profileMaster?.master?.id),
    queryFn: () => fetchMasterProfile(profileMaster!.master.id),
    staleTime: 5 * 60 * 1000,
  });

  const closeProfile = () => setProfileMaster(null);

  const toggleService = useCallback((service: ServiceOut) => {
    dispatchBookingDraft({ type: "toggleService", service });
    haptic.impact("light");
  }, [dispatchBookingDraft]);

  useEffect(() => {
    // Reset payment status when leaving confirmation, but preserve it
    // while on the SUCCESS screen so paid/pending states remain visible.
    if (step !== "CONFIRM" && step !== "SUCCESS") {
      setPaymentStatus("idle");
    }
  }, [step]);

  // Haptic: success notification when entering SUCCESS screen
  useEffect(() => {
    if (step === "SUCCESS") {
      haptic.notify("success");
    }
  }, [step]);

  // Ensure paymentStatus reflects server-side booking state when landing on SUCCESS.
  useEffect(() => {
    const ensurePaid = async () => {
      try {
        const id = bookingId ?? holdBookingId;
        if (!id) return;
        const list = await fetchBookings("upcoming");
        const found = (list || []).find((b) => b.id === id);
        if (found) {
          const st = (found.status || "").toString().toLowerCase();
          if (st === "paid") {
            setPaymentStatus("paid");
          }
        }
      } catch (err) {
        // ignore
      }
    };
    if (step === "SUCCESS") ensurePaid();
  }, [step, bookingId, holdBookingId]);

  const handleSlotCenterChange = useCallback((h: string, m: string) => {
    updateDraft({ time: `${h}:${m}` });
  }, [updateDraft]);

  const handleSlotPick = useCallback(async (origSlot: string, display: string) => {
    try {
      if (!selectedMaster) {
        setToast({ message: t("choose_master") as string, tone: "error" });
        goToStep("MASTER");
        return;
      }

      if (holdBookingId) {
        try {
          await cancelBooking(holdBookingId);
        } catch (err) {}
        resetHold();
      }

      const normalized = normalizeSlotString(origSlot, selectedDate);
      const parsed = new Date(normalized);
      const finalSlot = Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();

      if (isRescheduling && rescheduleBookingId) {
        updateDraft({ slot: finalSlot, time: display });
        goToStep("CONFIRM");
        haptic.impact("light");
        return;
      }

      const payload = {
        master_id: selectedMaster.id,
        service_ids: selectedServiceIds,
        slot: finalSlot,
        payment_method: paymentMethod,
      };
      const resp = await createHold(payload as any);
      if (resp && resp.ok && resp.booking_id) {
        updateHold({
          bookingId: resp.booking_id ?? null,
          expiresAt: resp.cash_hold_expires_at ?? null,
          originalPriceCents: resp.original_price_cents ?? null,
          finalPriceCents: resp.final_price_cents ?? null,
          discountAmountCents: resp.discount_amount_cents ?? null,
          discountPercent: (resp as any)?.discount_percent ?? (resp as any)?.discount_percent_applied ?? null,
          currency: resp.currency ?? null,
          paymentMethod: resp.payment_method ?? paymentMethod ?? null,
        });
          updateDraft({ slot: finalSlot, time: display });
        goToStep("CONFIRM");
        haptic.impact("light");
        setToast({ message: t("slot_reserved") as string, tone: "success" });
      } else {
        setToast({ message: resp?.error || (t("reserve_failed") as string), tone: "error" });
        haptic.notify("error");
        refetchSlots();
        goToTimeClearingHold();
      }
    } catch (err) {
      console.warn("hold on pick failed", err);
      setToast({ message: (t("reserve_failed") as string), tone: "error" });
      refetchSlots();
      goToTimeClearingHold();
    }
  }, [selectedMaster, holdBookingId, resetHold, selectedDate, isRescheduling, rescheduleBookingId, selectedServiceIds, paymentMethod, updateHold, goToStep, updateDraft, setToast, refetchSlots, goToTimeClearingHold]);

  const handleMasterSelect = useCallback((m: MasterOut) => {
    updateDraft({ master: m });
    goToStep("DATE");
    haptic.impact("light");
  }, [goToStep, updateDraft]);

  const handleProfileOpen = useCallback((m: MasterOut, anchorEl?: HTMLElement | null) => {
    setProfileMaster({ master: m, anchorEl });
  }, []);

  const handleDateSelect = useCallback(async (iso: string) => {
    updateDraft({ date: iso, time: "", slot: "" });
    await goToTimeClearingHold();
    haptic.impact("light");
  }, [goToTimeClearingHold, updateDraft]);

  const renderServiceSelection = () => (
    <StepServiceSelect
      services={services || []}
      servicesLoading={servicesLoading}
      serviceAvailability={serviceAvailability}
      serviceRanges={serviceRanges}
      selectedServices={selectedServices}
      currencyCode={currencyCode}
      onToggle={toggleService}
    />
  );

  const renderMasterSelection = () => (
    <StepMasterSelect
      masters={mastersWithMeta || []}
      mastersLoading={mastersLoading}
      selectedMaster={selectedMaster}
      onSelect={handleMasterSelect}
      onOpenProfile={handleProfileOpen}
    />
  );

  const renderTimeSelection = () => (
    <StepTimeSelect
      slotsLoading={slotsLoading}
      slots={(slotsData as any)?.slots || []}
      selectedSlot={selectedSlot}
      selectedDate={selectedDate}
      onCenterChange={handleSlotCenterChange}
      onPick={handleSlotPick}
    />
  );

  const stepContent = (() => {
    switch (step) {
      case "SERVICE":
        return renderServiceSelection();
      case "MASTER":
        return renderMasterSelection();
      case "DATE":
        return (
          <StepDateSelect
            viewYearMonth={viewYearMonth}
            availableDays={(availableDaysData as any)?.days || []}
            selectedDate={selectedDate}
            onPrev={() =>
                setViewYearMonth((v) => {
                const prev = new Date(v.year, v.month - 2, 1);
                haptic.impact("light");
                return { year: prev.getFullYear(), month: prev.getMonth() + 1 };
              })
            }
            onNext={() =>
              setViewYearMonth((v) => {
                const next = new Date(v.year, v.month, 1);
                haptic.impact("light");
                return { year: next.getFullYear(), month: next.getMonth() + 1 };
              })
            }
            onSelect={handleDateSelect}
          />
        );
      case "TIME":
        return renderTimeSelection();
      case "CONFIRM":
        return (
          <StepConfirm
            bookingId={bookingId}
            holdBookingId={holdBookingId}
            selectedServicesNames={selectedServicesNames}
            selectedMaster={selectedMaster}
            selectedDate={selectedDate}
            selectedTime={selectedTime}
            durationLabel={durationLabel}
            isRescheduling={isRescheduling}
            priceQuoteLoading={priceQuoteLoading}
            finalPriceCents={finalPriceCents}
            displayCurrency={displayCurrency}
            discountAmountCents={discountAmountCents}
            originalPriceCents={originalPriceCents}
            holdExpiresAt={holdExpiresAt}
            holdRemainingLabel={holdRemainingLabel}
            onlinePaymentsAvailable={onlinePaymentsAvailable}
            onlineDiscountPercent={onlineDiscountPercent}
            paymentMethod={paymentMethod}
            setPaymentMethod={handlePaymentMethodChange}
            paymentStatus={paymentStatus}
            paymentMessage={paymentMessage}
          />
        );
      default:
        return null;
    }
  })();

  if (step === "SUCCESS") {
    const successDate = formatDateTimeLabel(selectedDate, selectedTime);
    const derivedOriginal = originalPriceCents ?? (discountAmountCents != null && finalPriceCents != null ? finalPriceCents + discountAmountCents : null);
    const derivedFinal = finalPriceCents ?? (originalPriceCents != null && discountAmountCents != null ? originalPriceCents - discountAmountCents : originalPriceCents ?? null);
    const derivedDiscount = discountAmountCents ?? (derivedOriginal != null && derivedFinal != null && derivedOriginal > derivedFinal ? derivedOriginal - derivedFinal : null);
    const hasDiscount = derivedDiscount != null && derivedDiscount > 0 && derivedOriginal != null && derivedFinal != null && derivedOriginal > derivedFinal;
    const leadMinutesRaw = (typeof window !== "undefined" && (window as any).__APP_REMINDER_LEAD_MINUTES) || null;
    const leadMinutes = typeof leadMinutesRaw === "number" ? leadMinutesRaw : null;

    function pluralFormRu(n: number) {
      const mod10 = n % 10;
      const mod100 = n % 100;
      if (mod10 === 1 && mod100 !== 11) return "one";
      if (mod10 >= 2 && mod10 <= 4 && !(mod100 >= 12 && mod100 <= 14)) return "few";
      return "many";
    }

    function pluralForm(lang: string, n: number) {
      if (lang.startsWith("ru")) return pluralFormRu(n);
      return n === 1 ? "one" : "many";
    }

    function unitKey(unit: "hour" | "minute", form: string) {
      return `${unit}_${form}`;
    }

    function formatLead(minutes: number | null) {
      const lang = detectLang();
      if (minutes == null) {
        const key = unitKey("hour", pluralForm(lang, 1));
        return `1 ${t(key)}` as string;
      }
      if (minutes < 60) {
        const form = pluralForm(lang, minutes);
        const key = unitKey("minute", form);
        return `${minutes} ${t(key)}` as string;
      }
      const hours = Math.floor(minutes / 60);
      const rem = minutes % 60;
      if (rem === 0) {
        const form = pluralForm(lang, hours);
        const key = unitKey("hour", form);
        return `${hours} ${t(key)}` as string;
      }
      const formH = pluralForm(lang, hours);
      const formM = pluralForm(lang, rem);
      const keyH = unitKey("hour", formH);
      const keyM = unitKey("minute", formM);
      return `${hours} ${t(keyH)} ${rem} ${t(keyM)}` as string;
    }

    const remindersEnabled = typeof leadMinutesRaw === 'number' && leadMinutesRaw > 0;
    const leadLabel = remindersEnabled ? formatLead(leadMinutes) : null;

    return (
      <div className="tma-shell">
        <div className="tma-bg"></div>
        <div className="tma-container" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', paddingTop: 24 }}>
          <div className="tma-card tma-pop tma-success" style={{ width: '100%', maxWidth: 720, minHeight: '64vh', display: 'flex', flexDirection: 'column', justifyContent: 'space-between', padding: 28 }}>
            <div>
              <div className="tma-checkmark" aria-hidden="true" style={{ margin: '8px auto 14px' }}>
                <span className="tma-checkmark-circle" />
                <span className="tma-checkmark-stem" />
                <span className="tma-checkmark-kick" />
              </div>

              <h2 style={{ marginTop: 6, marginBottom: 8, fontSize: 22, textAlign: 'center' }}>{t("success_heading")}</h2>
              {remindersEnabled && (
                <p className="tma-subtle" style={{ marginBottom: 12, textAlign: 'center' }}>
                  {String(t("reminder_message")).replace("%s", String(leadLabel))}
                </p>
              )}

              <div style={{ marginTop: 6 }}>
                <div className="tma-pay-card">
                  <div className="tma-pay-row">
                    <span>{t('tab_booking') as string}</span>
                    <strong>{(bookingId ?? holdBookingId) ? `№${bookingId ?? holdBookingId}` : ""}</strong>
                  </div>
                  <div className="tma-pay-row"><span>{t("service_label")}</span> <strong>{selectedServicesNames || "—"}</strong></div>
                  <div className="tma-pay-row"><span>{t("master_label")}</span> <strong>{selectedMaster?.name || "—"}</strong></div>
                  <div className="tma-pay-row"><span>{t("date_label")}</span> <strong>{successDate}</strong></div>
                  <div className="tma-pay-row"><span>{t("duration_label")}</span> <strong>{durationLabel}</strong></div>
                  <div className="tma-pay-row">
                    <span>{paymentStatus === "paid" ? t("paid_label") : t("to_be_paid")}</span>
                    <strong>{priceQuoteLoading ? "…" : derivedFinal != null ? formatMoneyFromCents(derivedFinal, displayCurrency) : "—"}</strong>
                    {hasDiscount && derivedOriginal != null && (
                      <span className="tma-subtle tma-price-strike">{formatMoneyFromCents(derivedOriginal, displayCurrency)}</span>
                    )}
                  </div>
                  {hasDiscount && derivedDiscount != null && (
                    <div className="tma-subtle">{t("online_discount")} {formatMoneyFromCents(derivedDiscount, displayCurrency)}</div>
                  )}
                </div>
                {/* Адрес салона под карточкой брони (если задан админом) */}
                {typeof window !== "undefined" && (window as any).__APP_ADDRESS ? (
                  <div style={{ marginTop: 12, textAlign: 'center' }} className="tma-subtle">
                    {String(t("success_address_hint")).replace("%s", String((window as any).__APP_ADDRESS))}
                  </div>
                ) : null}
              </div>
            </div>

            <div style={{ marginTop: 18, display: 'flex', gap: 12, flexDirection: 'column', alignItems: 'center' }}>
              <button
                type="button"
                className="tma-primary"
                style={{ width: '100%', maxWidth: 360, padding: '14px 20px', fontSize: 16 }}
                onClick={() => { try { window.dispatchEvent(new CustomEvent('tma:open-visits')); } catch (e) {} }}
              >
                {t("success_my_visits")}
              </button>

              <button
                type="button"
                className="tma-primary"
                style={{ width: '100%', maxWidth: 360, padding: '14px 20px', fontSize: 16 }}
                onClick={() => { try { window.dispatchEvent(new CustomEvent('tma:open-hub')); } catch (e) {} }}
              >
                {t("success_home")}
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="tma-shell">
      <div className="tma-bg"></div>
      
      <div className="tma-container">
        <header className="tma-hero">
          <div className="tma-chip tma-chip-ghost">{(typeof window !== "undefined" && (window as any).__APP_TITLE) || "Telegram Mini App • Beauty"}</div>
          <h1 style={{ fontSize: '1.0em' }}>{heroTitle}</h1>
          <p className="tma-subtle">{heroSub}</p>
          {/* progress steps removed per request */}
        </header>

        <section className="tma-card tma-stack">
          <ProgressHeader currentStep={step} />
            <div className="tma-section-head">
            <div>
              <h2>{heading}</h2>
            </div>
            {step !== "CONFIRM" && (
              <div className="tma-summary">
                {selectedServices.length > 0 && selectedServices.map((s) => <span key={s.id}>{s.name}</span>)}
                {selectedMaster && <span>{selectedMaster.name}</span>}
                {selectedTime && <span>{selectedTime}</span>}
              </div>
            )}
          </div>

          <div className="tma-step-viewport">
            <AnimatePresence mode="wait" custom={direction}>
              <motion.div
                key={step}
                custom={direction}
                variants={slideVariants}
                initial="enter"
                animate="center"
                exit="exit"
                transition={{ type: "tween", ease: "circOut", duration: 0.28 }}
                className="tma-step-pane"
              >
                {stepContent}
              </motion.div>
            </AnimatePresence>
          </div>
        </section>
      </div>
      <MasterProfileSheet
        open={Boolean(profileMaster)}
        loading={profileLoading}
        master={profileMaster?.master ?? null}
        profile={profileData || null}
        onClose={closeProfile}
          onBook={(m) => {
          updateDraft({ master: m });
          goToStep("DATE");
          closeProfile();
          haptic.impact("medium");
        }}
        anchorEl={profileMaster?.anchorEl}
      />
      <Toast message={toast?.message} tone={toast?.tone} onClose={() => setToast(null)} />
    </div>
  );
}

function SkeletonGroup({ count, compact }: { count: number; compact?: boolean }) {
  return (
    <div className={`tma-skeleton-group ${compact ? "compact" : ""}`}>
      {Array.from({ length: count }).map((_, idx) => (
        <div className="tma-skeleton" key={idx} />
      ))}
    </div>
  );
}

function SkeletonChips({ count = 8 }: { count?: number }) {
  const widths = [68, 82, 74, 96, 70, 88, 78, 84];

  return (
    <div className="tma-skeleton-row">
      {Array.from({ length: count }).map((_, idx) => (
        <div key={idx} className="tma-skeleton-pill" style={{ width: `${widths[idx % widths.length]}px` }} />
      ))}
    </div>
  );
}

type CalendarProps = {
  year: number;
  month: number;
  availableDays: number[];
  selectedDate: string;
  onPrev?: () => void;
  onNext?: () => void;
  onSelect: (iso: string) => void;
};

type CalCell = { day: number; iso: string } | null;

function buildCalendarDays(year: number, month: number): CalCell[] {
  const first = new Date(year, month - 1, 1);
  const startOffset = (first.getDay() + 6) % 7; // Monday = 0
  const daysInMonth = new Date(year, month, 0).getDate();

  const cells: CalCell[] = [];

  for (let i = 0; i < startOffset; i++) cells.push(null);

  for (let d = 1; d <= daysInMonth; d++) {
    const iso = formatYMD(year, month, d);
    cells.push({ day: d, iso });
  }

  return cells;
}

function monthName(month: number) {
  const arr = (t("month_names") as string[]) || [];
  return arr[month - 1] || new Date(2020, month - 1, 1).toLocaleString(undefined, { month: "long" });
}

function Calendar({ year, month, availableDays, selectedDate, onPrev, onNext, onSelect }: CalendarProps) {
  const cells = buildCalendarDays(year, month);
  const selected = selectedDate ? new Date(selectedDate) : null;

  return (
    <div className="tma-calendar">
      <div className="tma-cal-head">
        <button className="tma-cal-nav" onClick={() => onPrev?.()} type="button">‹</button>
        <div className="tma-cal-title">{monthName(month)} {year}</div>
        <button className="tma-cal-nav" onClick={() => onNext?.()} type="button">›</button>
      </div>

      <div className="tma-cal-grid">
        {((t("dow_short") as string[]) || ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]).map((d) => (
            <div key={d} className="tma-cal-dow">{d}</div>
          ))}
        {cells.map((cell, idx) => {
          if (!cell) return <div key={idx} className="tma-cal-cell" />;

          const isAvailable = availableDays.includes(cell.day);
          const isSelected =
            selected &&
            selected.getDate() === cell.day &&
            selected.getMonth() === month - 1 &&
            selected.getFullYear() === year;

          return (
            <button
              key={idx}
              className={`tma-cal-cell ${isSelected ? "sel" : ""}`}
              disabled={!isAvailable}
              onClick={() => onSelect(cell.iso)}
              type="button"
            >
              <span>{cell.day}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}


type StepServiceSelectProps = {
  services: ServiceOut[];
  servicesLoading: boolean;
  serviceAvailability: Record<string, boolean>;
  serviceRanges: Record<string, { min_duration: number | null; max_duration: number | null; min_price_cents: number | null; max_price_cents: number | null }>;
  selectedServices: ServiceOut[];
  currencyCode: string;
  onToggle: (s: ServiceOut) => void;
};

const StepServiceSelect = ({ services, servicesLoading, serviceAvailability, serviceRanges, selectedServices, currencyCode, onToggle }: StepServiceSelectProps) => {
  return (
    <div className="tma-grid">
      {servicesLoading && <SkeletonGroup count={3} />}
      {services
        .filter((s) => serviceAvailability[s.id] !== false)
        .map((s) => {
          const r = serviceRanges[s.id];
          const minutesLabel = (t("minutes_short") as string) || "min";
          const durLabel = r
            ? r.min_duration && r.max_duration
              ? r.min_duration === r.max_duration
                ? `${r.min_duration} ${minutesLabel}`
                : `${r.min_duration}–${r.max_duration} ${minutesLabel}`
              : s.duration_minutes
              ? `${s.duration_minutes} ${minutesLabel}`
              : "—"
            : s.duration_minutes
            ? `${s.duration_minutes} ${minutesLabel}`
            : "—";

          const priceLabel = r && (r.min_price_cents != null || r.max_price_cents != null)
            ? (r.min_price_cents != null && r.max_price_cents != null
              ? (r.min_price_cents === r.max_price_cents
                ? formatMoneyFromCents(r.min_price_cents, currencyCode)
                : `${formatMoneyFromCents(r.min_price_cents, currencyCode)}–${formatMoneyFromCents(r.max_price_cents, currencyCode)}`)
              : (r.min_price_cents != null ? formatMoneyFromCents(r.min_price_cents, currencyCode) : "—"))
            : (s.price_cents != null ? formatMoneyFromCents(s.price_cents, currencyCode) : "—");

          const isSelected = selectedServices.some((sel) => sel.id === s.id);

          return (
            <button
              key={s.id}
              className={`tma-tile ${isSelected ? "active" : ""}`}
              onClick={() => onToggle(s)}
              type="button"
            >
              <div className="tma-tile-head">
                <input
                  aria-label={`Выбрать услугу ${s.name}`}
                  type="checkbox"
                  className="tma-service-checkbox"
                  checked={isSelected}
                  readOnly
                  tabIndex={-1}
                />
              </div>
              <div className="tma-tile-body tma-tile-row">
                <div className="tma-tile-row-left">
                  <h3>{s.name}</h3>
                  {s.description && <p className="tma-subtle">{s.description}</p>}
                </div>
                <div className="tma-tile-row-right">
                  <span className="tma-duration-inline">{durLabel}</span>
                  <span className="tma-price-inline">{priceLabel}</span>
                </div>
              </div>
            </button>
          );
        })}
    </div>
  );
};

type StepMasterSelectProps = {
  masters: MasterOut[];
  mastersLoading: boolean;
  selectedMaster: MasterOut | null;
  onSelect: (m: MasterOut) => void;
  onOpenProfile: (m: MasterOut, anchorEl?: HTMLElement | null) => void;
};

const StepMasterSelect = ({ masters, mastersLoading, selectedMaster, onSelect, onOpenProfile }: StepMasterSelectProps) => (
  <div className="tma-grid">
    {mastersLoading && <SkeletonGroup count={2} />}
    {masters.length === 0 && !mastersLoading && <div className="tma-empty">{t("no_masters")}</div>}
    {masters.map((m) => (
      <button
        key={m.id}
        className={`tma-tile tma-master-card ${selectedMaster?.id === m.id ? "active" : ""}`}
        onClick={() => onSelect(m)}
        type="button"
      >
        <div className="tma-master-card__main">
          <div className="tma-avatar" aria-hidden="true" />
          <div className="tma-master-card__info">
            <div className="tma-master-name-badge" aria-hidden>{m.name}</div>

            <button
              type="button"
              className="tma-master-cta"
              onClick={(e) => {
                e.stopPropagation();
                onSelect(m);
              }}
            >
              {t("book_with_master") || "Записатися"}
            </button>
          </div>

          <div className="tma-meta-column">
            {typeof (m as any).rating === "number" && (
              <div className="tma-meta-chip tma-chip--rating">⭐ {String((m as any).rating.toFixed ? (m as any).rating.toFixed(1) : (m as any).rating)}</div>
            )}

            {typeof (m as any).completed_orders === "number" && (
              <div className="tma-meta-chip tma-chip--orders">{String((m as any).completed_orders)} {String(t("master_orders_short") || "orders")}</div>
            )}
          </div>
        </div>

        <div
          className="tma-master-card__profile"
          role="button"
          tabIndex={0}
          aria-label={t("master_profile") || "Profile"}
          onClick={(e) => {
            e.stopPropagation();
            onOpenProfile(m, e.currentTarget as HTMLElement);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              e.stopPropagation();
              onOpenProfile(m, e.currentTarget as HTMLElement);
            }
          }}
        >
          <span className="tma-master-card__profile-label">{t("master_profile") || "Profile"}</span>
        </div>
      </button>
    ))}
  </div>
);

type StepDateSelectProps = {
  viewYearMonth: { year: number; month: number };
  availableDays: number[];
  selectedDate: string;
  onPrev: () => void;
  onNext: () => void;
  onSelect: (iso: string) => void;
};

const StepDateSelect = ({ viewYearMonth, availableDays, selectedDate, onPrev, onNext, onSelect }: StepDateSelectProps) => (
  <div className="tma-stack">
    <Calendar
      year={viewYearMonth.year}
      month={viewYearMonth.month}
      availableDays={availableDays}
      selectedDate={selectedDate}
      onPrev={onPrev}
      onNext={onNext}
      onSelect={onSelect}
    />
    <p className="tma-subtle">{t("unavailable_days_note") || "Unavailable days are disabled per bot data."}</p>
  </div>
);

type StepTimeSelectProps = {
  slotsLoading: boolean;
  slots: string[];
  selectedSlot: string;
  selectedDate: string;
  onCenterChange: (h: string, m: string) => void;
  onPick: (origSlot: string, display: string) => void;
};

const StepTimeSelect = ({ slotsLoading, slots, selectedSlot, selectedDate, onCenterChange, onPick }: StepTimeSelectProps) => (
  <div className="tma-stack">
    {slotsLoading && <SkeletonChips count={10} />}
    {!slotsLoading && (slots?.length || 0) === 0 && (
      <div className="tma-empty">{t("no_slots")}</div>
    )}
    {!slotsLoading && (slots?.length || 0) > 0 && (
      <TimeWheel
        slots={slots || []}
        selectedSlot={selectedSlot}
        selectedDate={selectedDate}
        onCenterChange={onCenterChange}
        onPick={onPick}
      />
    )}
  </div>
);

type StepConfirmProps = {
  bookingId: number | null;
  holdBookingId: number | null;
  selectedServicesNames: string;
  selectedMaster: MasterOut | null;
  selectedDate: string;
  selectedTime: string;
  durationLabel: string;
  isRescheduling: boolean;
  priceQuoteLoading: boolean;
  finalPriceCents: number | null;
  displayCurrency: string;
  discountAmountCents: number | null;
  originalPriceCents: number | null;
  holdExpiresAt: string | null;
  holdRemainingLabel: string | null;
  onlinePaymentsAvailable: boolean;
  onlineDiscountPercent: number | null;
  paymentMethod: "cash" | "online";
  setPaymentMethod: (method: "cash" | "online") => void;
  paymentStatus: "idle" | "opening" | "pending" | "polling" | "paid" | "failed" | "cancelled";
  paymentMessage: string;
};

const StepConfirm = ({
  bookingId,
  holdBookingId,
  selectedServicesNames,
  selectedMaster,
  selectedDate,
  selectedTime,
  durationLabel,
  isRescheduling,
  priceQuoteLoading,
  finalPriceCents,
  displayCurrency,
  discountAmountCents,
  originalPriceCents,
  holdExpiresAt,
  holdRemainingLabel,
  onlinePaymentsAvailable,
  onlineDiscountPercent,
  paymentMethod,
  setPaymentMethod,
  paymentStatus,
  paymentMessage,
}: StepConfirmProps) => {
  const hasOnlineDiscount = onlineDiscountPercent != null && onlineDiscountPercent > 0;
  const discountPercentRounded = hasOnlineDiscount ? Math.round(onlineDiscountPercent as number) : null;
  const onlineSaveTemplate = t("online_save_percent") || "Save %s%";
  const onlineSubLabel = hasOnlineDiscount
    ? String(onlineSaveTemplate).replace("%s", String(discountPercentRounded))
    : t("online_sub");

  // Derive a complete price breakdown so UI shows strike-through + discount even when server omits one of the fields
  const derivedOriginal = originalPriceCents ?? (discountAmountCents != null && finalPriceCents != null ? finalPriceCents + discountAmountCents : null);
  const derivedFinal = finalPriceCents ?? (originalPriceCents != null && discountAmountCents != null ? originalPriceCents - discountAmountCents : originalPriceCents ?? null);
  const derivedDiscount = discountAmountCents ?? (derivedOriginal != null && derivedFinal != null && derivedOriginal > derivedFinal ? derivedOriginal - derivedFinal : null);
  const hasDiscount = derivedDiscount != null && derivedDiscount > 0 && derivedOriginal != null && derivedFinal != null && derivedOriginal > derivedFinal;

  return (
    <div className="tma-stack">
      <div className="tma-pay-card">
        <div className="tma-pay-row">
          <span>{t('tab_booking') as string}</span>
          <strong>{(bookingId ?? holdBookingId) ? `№${bookingId ?? holdBookingId}` : ""}</strong>
        </div>
        <div className="tma-pay-row"><span>{t("service_label")}</span> <strong>{selectedServicesNames || "—"}</strong></div>
        <div className="tma-pay-row"><span>{t("master_label")}</span> <strong>{selectedMaster?.name || "—"}</strong></div>
        <div className="tma-pay-row"><span>{t("date_label")}</span> <strong>{formatDateTimeLabel(selectedDate, selectedTime)}</strong></div>
        <div className="tma-pay-row"><span>{t("duration_label")}</span> <strong>{durationLabel}</strong></div>
        {isRescheduling ? (
          <div className="tma-pay-row">
            <span>{(t("reschedule_title") as string) || "Перенос записи"}</span>
            <strong>{(t("reschedule_free") as string) || "Оплата не требуется"}</strong>
          </div>
        ) : (
          <>
            <div className="tma-pay-row">
              <span>{t("to_be_paid")}</span>
              <strong>{priceQuoteLoading ? "…" : derivedFinal != null ? formatMoneyFromCents(derivedFinal, displayCurrency) : "—"}</strong>
              {hasDiscount && derivedOriginal != null && (
                <span className="tma-subtle tma-price-strike">
                  {formatMoneyFromCents(derivedOriginal, displayCurrency)}
                </span>
              )}
            </div>
            {hasDiscount && derivedDiscount != null && (
              <div className="tma-subtle">{t("online_discount")} {formatMoneyFromCents(derivedDiscount, displayCurrency)}</div>
            )}
            {holdExpiresAt && (
              <div className="tma-subtle tma-pay-countdown">{t("hold_countdown")} {holdRemainingLabel}</div>
            )}
          </>
        )}
      </div>

      {!isRescheduling && (
        <>
          <div className="tma-pay-method">
            <div className="tma-pay-label">{t("choose_payment_method") || "Choose a payment method"}</div>
            <div className="tma-pay-options">
              <button
                type="button"
                className={`tma-pay-option ${paymentMethod === "cash" ? "active" : ""}`}
                onClick={() => setPaymentMethod("cash")}
              >
                <span className="tma-pay-icon">💵</span>
                <div>
                  <strong>{t("cash")}</strong>
                  <div className="tma-subtle">{t("cash_sub")}</div>
                </div>
              </button>
              {onlinePaymentsAvailable && (
                <button
                  type="button"
                  className={`tma-pay-option ${paymentMethod === "online" ? "active" : ""}`}
                  onClick={() => { setPaymentMethod("online"); haptic.impact("light"); }}
                >
                  <span className="tma-pay-icon">💳</span>
                  <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <strong>{t("online")}</strong>
                      {hasOnlineDiscount && discountPercentRounded !== null && (
                        <span className="tma-chip tma-chip-soft" aria-label={onlineSubLabel || undefined}>
                          -{discountPercentRounded}%
                        </span>
                      )}
                    </div>
                    <div className="tma-subtle">{onlineSubLabel || t("online_sub")}</div>
                  </div>
                </button>
              )}
            </div>
          </div>

          {paymentStatus !== "idle" && (
            <div className="tma-pay-status">
              <strong>{t("pay_label")}</strong>
              <p className="tma-subtle">{paymentMessage}</p>
            </div>
          )}
        </>
      )}
      <p className="tma-subtle">{isRescheduling ? (t("reschedule_confirm_hint") as string) || (t("press_confirm") as string) : t("press_confirm")}</p>
    </div>
  );
};


type MasterProfileSheetProps = {
  open: boolean;
  loading: boolean;
  master: MasterOut | null;
  profile: MasterProfile | null;
  onClose: () => void;
  onBook: (master: MasterOut) => void;
  anchorEl?: HTMLElement | null;
};


function MasterProfileSheet({
  open,
  loading,
  master,
  profile,
  onClose,
  onBook,
  anchorEl,
}: MasterProfileSheetProps) {
  // Эффект для прокрутки к карточке при открытии
  useEffect(() => {
    if (open && anchorEl) {
      // Плавно скроллим страницу, чтобы карточка мастера была по центру
      setTimeout(() => {
        anchorEl.scrollIntoView({ behavior: "smooth", block: "center" });
      }, 100);
    }
  }, [open, anchorEl]);

  if (!open || !master) return null;

  // Localize schedule lines coming from server — replace English day names
  const localeDow = (t("dow_short") as string[]) || ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const engShort = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const engFull = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
  function localizeScheduleLine(line: string | null | undefined): string {
    if (!line) return "";
    let out = String(line);
    // Replace full names first
    engFull.forEach((en, idx) => {
      try {
        const re = new RegExp("\\b" + en + "\\b", "g");
        out = out.replace(re, localeDow[idx]);
      } catch (err) {}
    });
    // Then replace short names
    engShort.forEach((en, idx) => {
      try {
        const re = new RegExp("\\b" + en + "\\b", "g");
        out = out.replace(re, localeDow[idx]);
      } catch (err) {}
    });
    return out;
  }

  const content = (
    <AnimatePresence>
      {open && (
        <motion.div
          className="tma-modal-backdrop"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          onClick={onClose}
        >
          <motion.div
            className="tma-centered-modal"
            initial={{ scale: 0.9, opacity: 0, y: 20 }}
            animate={{ scale: 1, opacity: 1, y: 0 }}
            exit={{ scale: 0.9, opacity: 0, y: 20 }}
            transition={{ type: "spring", damping: 25, stiffness: 300 }}
            onClick={(e) => e.stopPropagation()}
          >
            {/* Header */}
            <div className="tma-modal-header-fixed">
              <div className="tma-profile-row">
                <div className="tma-avatar lg">{master.name.charAt(0)}</div>
                <div>
                  <h3 className="tma-profile-title">{master.name}</h3>
                  <p className="tma-subtle tma-profile-subtle">
                    {profile?.title || (t("master_default") as string)}
                  </p>
                </div>
              </div>
              <button className="tma-close-btn" onClick={onClose}>
                ✕
              </button>
            </div>

            {/* Scrollable Content */}
            <div className="tma-modal-scroll-content">
              {loading && <SkeletonGroup count={2} compact />}
              {!loading && (
                <>
                  <div className="tma-chip-row tma-chip-row--spaced">
                    <span className="tma-chip">
                      <span style={{ fontSize: 14, lineHeight: 1 }}>⭐</span>
                      <span>{profile?.rating ? profile.rating.toFixed(1) : "—"}</span>
                    </span>

                    <span className="tma-chip">
                      <span>{String(t("master_orders_label")) || "Orders:"}</span>
                      <span>{profile?.completed_orders ?? "0"}</span>
                    </span>
                  </div>

                  {profile?.bio && (
                    <div className="tma-card tma-card--compact u-mb-12">
                      <p className="tma-subtle tma-pre-wrap">{profile.bio}</p>
                    </div>
                  )}

                  {/* Services List */}
                  {profile?.services?.length ? (
                    <div className="tma-card tma-card--compact">
                      <strong className="tma-block-title">{t("services_label") || "Services"}</strong>
                      <ul className="tma-list tma-plain-list">
                        {profile.services.map((s, idx) => {
                          const price = s.price_cents != null ? formatMoneyFromCents(s.price_cents, s.currency || undefined) : "";
                          const dur = s.duration_minutes != null ? `${s.duration_minutes} ${t("minutes_short") as string || "min"}` : null;
                          return (
                            <li key={idx} className="tma-list-item">
                              <span className="tma-list-bullet">•</span>{" "}
                              {s.name}
                              {dur ? <span className="tma-subtle"> ({dur})</span> : null}
                              {price ? <span className="tma-subtle"> — {price}</span> : null}
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  ) : null}
                  {/* Schedule List */}
                  {profile?.schedule_lines && profile.schedule_lines.length > 0 ? (
                    <div className="tma-card tma-card--compact tma-card--spaced-top">
                      <strong className="tma-block-title">{t("schedule_label") || "Schedule"}</strong>
                      <ul className="tma-schedule-list tma-plain-list">
                        {profile.schedule_lines.map((line, idx) => (
                          <li key={idx} className="tma-schedule-item">{
                            (() => {
                              const raw = localizeScheduleLine(line) || "";
                              const t = String(raw).trim();
                              // If server already provides a bullet/marker, don't add another
                              if (/^[•\-\*]/.test(t)) return t;
                              return `• ${t}`;
                            })()
                          }</li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                </>
              )}
            </div>

            {/* Actions Footer */}
            <div className="tma-modal-actions">
              <button
                onClick={() => onBook(master)}
                type="button"
                className="tma-primary u-w-100"
              >
                {t("book_with_master") || "Записаться к мастеру"}
              </button>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );

  return createPortal(content, document.body);
}

export function Toast({ message, tone = "success", onClose }: { message?: string | null; tone?: "error" | "success"; onClose: () => void }) {
  if (!message) return null;
  return (
    <AnimatePresence>
      <motion.div
        className={`tma-toast ${tone}`}
        initial={{ y: 40, opacity: 0 }}
        animate={{ y: 0, opacity: 1 }}
        exit={{ y: 40, opacity: 0 }}
        transition={{ type: "spring", stiffness: 260, damping: 24, mass: 0.8 }}
        onClick={onClose}
      >
        {message}
      </motion.div>
    </AnimatePresence>
  );
}
