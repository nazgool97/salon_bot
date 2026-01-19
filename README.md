![Python](https://img.shields.io/badge/Python-3.12+-brightgreen?logo=python)
![Aiogram](https://img.shields.io/badge/Aiogram-3.x-blue?logo=telegram)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-teal?logo=fastapi)
![Pydantic](https://img.shields.io/badge/Pydantic-2.x-lightblue?logo=pydantic)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-red?logo=sqlalchemy)
![Alembic](https://img.shields.io/badge/Alembic-migrations-orange)
![PostgreSQL](https://img.shields.io/badge/Postgres-15-blue?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-ready-blue?logo=docker)
![Uvicorn](https://img.shields.io/badge/Uvicorn-ASGI-purple?logo=python)
![React](https://img.shields.io/badge/React-18+-blue?logo=react)
![TypeScript](https://img.shields.io/badge/TypeScript-5+-blue?logo=typescript)
# Telegram Salon Booking Bot (Open Core)

Enterprise-grade CRM bot for salons, barbershops, and studios built with Clean Architecture. Clients self-book, masters manage schedules, owners get analytics — all inside Telegram. Open-core; commercial edition adds support and updates.

👉 Commercial edition: 
- [![Telegram Demo](https://img.shields.io/badge/Telegram-Demo-blue?logo=telegram)](https://t.me/PaywallClubBot)
- [![Gumroad](https://img.shields.io/badge/Gumroad-Buy-orange?logo=gumroad)](https://pentogram.gumroad.com/l/xlbbb)
- [![Payhip](https://img.shields.io/badge/Payhip-Buy-green?logo=paypal)](https://payhip.com/b/8LY2T)
- [![Ko-fi](https://img.shields.io/badge/Ko--fi-Buy-red?logo=kofi)](https://ko-fi.com/s/937c0881d1)

---

## Screenshots

<details>
  <summary><b>CLIENT BOOKING FLOW</b></summary>

  <details>
    <summary>Main menu</summary>
    <img src="screenshots/main_menu.jpg" width="360" />
  </details>

  <details>
    <summary>Service selection</summary>
    <img src="screenshots/service_selection.jpg" width="360" />
  </details>

  <details>
    <summary>Master card</summary>
    <img src="screenshots/master_card.jpg" width="360" />
  </details>

  <details>
    <summary>Date selection</summary>
    <img src="screenshots/date_selection.jpg" width="360" />
  </details>

  <details>
    <summary>Time selection</summary>
    <img src="screenshots/time_selection.jpg" width="360" />
  </details>

  <details>
    <summary>Payment method</summary>
    <img src="screenshots/payment_method.jpg" width="360" />
  </details>

  <details>
    <summary>Online payment</summary>
    <img src="screenshots/online_payment.jpg" width="360" />
  </details>

  <details>
    <summary>Upcoming bookings</summary>
    <img src="screenshots/upcoming_bookings.jpg" width="360" />
  </details>

</details>


<details>
  <summary><b>TELEGRAM MINI APP — CLIENT INTERFACE</b></summary>

  <details>
    <summary>Home</summary>
    <img src="screenshots/webapp_home.jpg" width="360" />
  </details>

  <details>
    <summary>Upcoming visits</summary>
    <img src="screenshots/webapp_upcoming_visits.jpg" width="360" />
  </details>

  <details>
    <summary>Visit history</summary>
    <img src="screenshots/webapp_history_visits.jpg" width="360" />
  </details>

  <details>
    <summary>Choose a master</summary>
    <img src="screenshots/webapp_choose_a_master.jpg" width="360" />
  </details>

  <details>
    <summary>Choose a day</summary>
    <img src="screenshots/webapp_choose_a_day.jpg" width="360" />
  </details>

  <details>
    <summary>Choose a time</summary>
    <img src="screenshots/webapp_choose_a_time.jpg" width="360" />
  </details>

  <details>
    <summary>Check details</summary>
    <img src="screenshots/webapp_check_details.jpg" width="360" />
  </details>

  <details>
    <summary>Booking confirmed</summary>
    <img src="screenshots/webapp_booked.jpg" width="360" />
  </details>

  <details>
    <summary>Booking cart</summary>
    <img src="screenshots/webapp_book_cart.jpg" width="360" />
  </details>

</details>


<details>
  <summary><b>MASTER & ADMIN PANELS</b></summary>

  <details>
    <summary>Master schedule</summary>
    <img src="screenshots/master_schedule.jpg" width="360" />
  </details>

  <details>
    <summary>Booking details (Master)</summary>
    <img src="screenshots/master_booking_detail.jpg" width="360" />
  </details>

  <details>
    <summary>Admin analytics</summary>
    <img src="screenshots/admin_stats.jpg" width="360" />
  </details>

  <details>
    <summary>Admin panel</summary>
    <img src="screenshots/admin_panel.jpg" width="360" />
  </details>

  <details>
    <summary>Admin settings</summary>
    <img src="screenshots/admin_settings.jpg" width="360" />
  </details>

</details>

---

## Tech Stack
- Python 3.11+
- Aiogram 3.x (FSM, Routers)
- SQLAlchemy + Alembic (asyncpg)
- PostgreSQL with advisory locks
- Docker & Docker Compose
- Pydantic for config/validation
- Structured logging
- Telegram Mini App (WebApp API)
- React + TypeScript (Client UI)
- FastAPI facade for WebApp


---

## Project Structure
<details>
<summary>📂 Project Structure</summary>

```text
.
├── bot
│   ├── app
│   │   ├── core       # DB, Config, Logging
│   │   ├── domain     # Models (SQLAlchemy)
│   │   ├── services   # Business Logic (Gap search, analytics)
│   │   └── telegram   # Handlers & Keyboards (Presentation)
│   └── migrations     # Alembic versions
├── docker             # Docker configs
└── docs               # Documentation
```
</details>

## Key Features
- Clean Architecture & DDD-inspired layers (handlers, services, domain, repos, workers).
- Concurrency-safe bookings via `pg_advisory_xact_lock` (no double bookings).
- Smart scheduling: gap search, composite services, per-master durations, UTC-first with local render.
- Professional FSM + navigation stack: real Back behavior with preserved context.
- Payments & policies: holds during pay, cancellation/reschedule windows, Telegram Payments.
- Roles: Owner/Admin, Master, Client with clear permissions.
- Analytics: LTV, retention, no-show rate, revenue (real vs expected), CSV export.
- Background workers: reminders, hold cleanup, payment reconcile, notifications.

---

### Client Web App (Telegram Mini App)
A modern Telegram Mini App for clients:
- Fast booking with a clean UI
- View upcoming & past bookings
- Cancel & reschedule without chat commands
- Works instantly inside Telegram (no installation)

---

## Features
Smart Booking Engine: Automatically finds gaps in schedules, supports multi-service bookings, and respects individual master speeds.

Double-Booking Protection: Uses PostgreSQL Advisory Locks to guarantee data integrity during concurrent requests.

Role-Based Access: Separate interfaces for Clients (booking), Masters (schedule management), and Admins (analytics & config).

Analytics Dashboard: Tracks LTV, Retention Rate, and No-Show statistics in real-time.

FSM & Navigation: Robust state machine with a navigation stack (Back button actually works correctly).

---

## Client Experience

Clients can book and manage visits via a built-in Telegram Mini App:
- No commands, no learning curve
- Beautiful UI inside Telegram
- Booking in under 30 seconds

This drastically increases conversion and reduces admin workload.

---

## Quick Start (2 minutes)

```bash
# 1) Clone
git clone https://github.com/nazgool97/salon_bot.git
cd repo

# 2) Configure
cp .env.example .env

# 3) Run
docker-compose up -d
```

Then talk to the bot with your token; edit `.env` for timezone, currency, policies, payment keys.

---

## Editions

| Capability | Community (Open Core) | Commercial |
| --- | --- | --- |
| Booking engine, FSM, navigation | ✅ | ✅ |
| Admin/Master/Client roles | ✅ | ✅ |
| Analytics (LTV, retention, no-show, revenue) | ✅ | ✅ |
| Payments & holds | ✅ | ✅ |
| Docker + Alembic + PostgreSQL | ✅ | ✅ |
| Support & updates | ✅ | ✅ |
| Production license | — | ✅ |

---

## Documentation
- Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Deployment: [docs/DEPLOY.md](docs/DEPLOY.md)
- Developer Manual (deep dive): [docs/DEVELOPER_MANUAL.md](docs/DEVELOPER_MANUAL.md)

---

## Roadmap (short)
- More payment providers and split payments.
- Multi-location policies.
- Advanced cohort analytics and churn signals.
- Inventory tracking for consumables.

---

## License & Support
- Open-core: free to explore, run locally, and learn.
- Commercial edition: production license, guided setup, updates, and support.
- Contact via the commercial link above for upgrades.

---

Built to be readable, testable, and safe under load: Clean Architecture, advisory locks, UTC everywhere, Alembic migrations, workers, and structured logging out of the box.

---