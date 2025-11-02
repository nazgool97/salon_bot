# Salon Bot — Ready-to-Sell SaaS Telegram Template for Appointments

Salon Bot is a production-ready Telegram bot template designed for salons, clinics, studios, and any service business that takes appointments. It includes separate UIs for Clients, Masters (staff), and Admins, plus analytics, payments, notifications, and robust scheduling out of the box.

This repository ships with Dockerized deployment, database migrations, demo bootstrap, and a minimal i18n layer (uk/ru/en). Configure it in minutes and start selling it to your customers or running it for your own business.

## What buyers get

- A full, self-hostable Telegram bot with a clear appointment lifecycle and background workers (holds/expirations)
- Three roles with tailored UX: Client, Master, Admin
- Built-in payments (Telegram Payments) and cash workflow
- Analytics for revenue, retention, no-shows, LTV, and status breakdowns
- Master schedule editor and real-time availability with calendar and time slots
- Localization (uk/ru/en), default language via env or per-user
- Docker + Compose deployment with health check, migrations, and optional bootstrap data
- Clean architecture, typed Python (3.12), SQLAlchemy 2.x, Alembic, Aiogram 3.x

---

## Features at a glance

### Client experience
- Browse services and prices (with service categories)
- Pick a master and a date/time from only valid, available slots
- Book in a few taps; confirm payment either online or at visit (cash)
- See “My bookings” with filters: Upcoming and Completed
- Reschedule with a configurable lock window (e.g., 3 hours before start)
- Cancel with confirmation; immediate updates to master/admins
- Rate the visit after completion (star rating UI)
- All messages localized to the user’s language when available

### Master experience
- Inline schedule editor (working days/time windows)
- Calendar and list views; “today” and “next 7 days” screens
- Only shows relevant, paid/confirmed upcoming bookings
- Booking details with actions: mark done, no-show, reschedule, cancel
- Client history and notes per client
- Personal stats summary (last 30 days, next booking)

### Admin experience
- Service catalog management with prices and currency (now includes Service.category)
- Price quick-edit steppers (+/- 5/20/50)
- Masters management and linking to services
- Business analytics (revenue, retention, no-show rate, LTV)
- Monthly range shortcuts (This Month, Last Month) for quick filtering
- Settings: hold duration, cash/online payment toggle, provider token
- CSV export for bookings (XLSX optional as an add-on)

### Booking lifecycle and availability
- Hold-based reservations with auto-expiration in background worker
- Unified statuses: RESERVED, PENDING_PAYMENT, CONFIRMED, PAID, DONE, CANCELLED, NO_SHOW, EXPIRED
- Availability ignores expired reservations
- Reschedule lock window to prevent last-minute changes

### Payments
- Telegram Payments integration (provider token from env)
- Cash confirmation flow for offline payments
- Admin toggle to enable/disable online payments at runtime

### Localization (i18n)
- Minimal dictionary-based i18n in `bot/app/translations.py`
- Languages included: Ukrainian (uk), Russian (ru), English (en)
- Default language via env `BOT_LANGUAGE`
- Per-user locale from DB (if present)

---

## Tech stack

- Python 3.12, Aiogram 3.x (Telegram bot)
- Postgres, SQLAlchemy 2.x, Alembic
- Docker, Docker Compose
- Pytest (tests), background worker for expirations

---

## Project structure (high-level)

```
salon_bot/
├── bot/
│   ├── app/
│   │   ├── core/          # config, DB, startup
│   │   ├── domain/        # SQLAlchemy models, enums
│   │   ├── services/      # business logic (client/master/admin/shared)
│   │   └── telegram/      # routers, keyboards, callbacks, navigation
│   └── ...
├── docker-compose.yml
├── Dockerfile
└── migrations/
```

---

## Quick start

Prerequisites:
- Docker & Docker Compose

1) Clone the repo
```bash
git clone https://github.com/nazgool97/salon_bot.git
cd salon_bot
```

2) Copy env and configure
```bash
cp .env.example .env
# edit .env and set at least:
# BOT_TOKEN, DATABASE_URL, ADMIN_IDS
```

Important variables:
- BOT_TOKEN — Telegram bot token from BotFather
- DATABASE_URL — Postgres URL (e.g., postgres://user:pass@db:5432/salon)
- ADMIN_IDS — comma-separated Telegram IDs with admin rights
- TELEGRAM_PAYMENT_PROVIDER_TOKEN — provider token for online payments (optional)
- BOT_LANGUAGE — default language: uk | ru | en
- CONTACT_* — contact info shown in the “Contacts” screen
- RESERVATION_HOLD_MINUTES — reservation hold duration
- CLIENT_RESCHEDULE_LOCK_HOURS — min hours before start when reschedule is blocked

3) Optional: preload demo data on first run
```bash
export RUN_BOOTSTRAP=1
```

4) Build & run
```bash
docker compose up --build
```

Compose will launch Postgres, run migrations, (optionally) bootstrap demo data, and start the bot with a health check.

---

## Non‑technical setup

If you’re not a developer, follow the step‑by‑step guide with screenshots:

- docs/NonTechnical-Setup-Guide.md — Bot token, Supabase DB, .env, and Docker run
- docs/FAQ.md — Payments/integrations/licensing
- docs/Customization.md — Add a language, payment provider, or change terminology

---

## Configuration & branding

- Default language via `.env` `BOT_LANGUAGE`; per-user locale from DB when available
- Update `CONTACT_*` and (optionally) `COMPANY_NAME` in `.env` to brand the bot
- Translations live in `bot/app/translations.py`; add or override keys as needed
- Payment toggle and provider token are configurable without restart

---

## Analytics & exports

- Admin analytics include revenue, retention, no-shows, and LTV
- Month range buttons: “This Month”, “Last Month”
- CSV export out-of-the-box; XLSX export can be enabled as an optional add-on

---

## Quality & maintainability

- Clear separation between domain models, services, and telegram layers
- Background worker handles expirations reliably
- Type hints throughout; SQLAlchemy 2.x patterns
- Optional pre-commit hooks for formatting/linting (Black, Ruff, isort)

Install hooks locally:
```bash
pip install pre-commit
pre-commit install
```

---

## Support & licensing

- License: choose MIT/Apache-2.0 or provide your own commercial EULA — the template is structured for resale
- Optional support plan: add your contact or Telegram group/link for buyers

---

## Roadmap (optional add-ons)

- XLSX export via openpyxl with basic formatting
- Global quick search by client/master name
- Expanded i18n coverage for all admin/master screens (already partially implemented)

---

## FAQ

Q: Can I disable online payments?
A: Yes, there’s a runtime toggle in Admin settings. You can also omit the provider token.

Q: Can I add a new language?
A: Add keys to `bot/app/translations.py` and set `BOT_LANGUAGE` to your new code; users with a stored locale will see their own language.

Q: How do I seed demo services and a master?
A: Set `RUN_BOOTSTRAP=1` in `.env` for the first run.

Q: Is the calendar restricted to the master’s working hours?
A: Yes, availability strictly follows the master’s schedule windows and ignores expired holds.

---

Happy shipping! If you need a white-label build or custom features, feel free to reach out.

