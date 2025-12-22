![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Aiogram](https://img.shields.io/badge/aiogram-3.x-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg)
![Postgres](https://img.shields.io/badge/postgres-15-blue.svg)
# Telegram Salon Booking Bot (Open Core)

Enterprise-grade CRM bot for salons, barbershops, and studios built with Clean Architecture. Clients self-book, masters manage schedules, owners get analytics — all inside Telegram. Open-core; commercial edition adds support and updates.

👉 Commercial edition: https://ko-fi.com/s/937c0881d1

---

## Screenshots

<details>
  <summary><b><u>Client booking flow</u></b></summary>

  ![Main menu](screenshots/main_menu.jpg)  
  ![Service selection](screenshots/service_selection.jpg)  
  ![master_card](screenshots/master_card.jpg)  
  ![Date selection](screenshots/date_selection.jpg)  
  ![Time selection](screenshots/time_selection.jpg)  
  ![Payment method](screenshots/payment_method.jpg)  
  ![Online payment](screenshots/online_payment.jpg)  
  ![Upcoming bookings](screenshots/upcoming_bookings.jpg)  
</details>

<details>
  <summary>Master & Admin</summary>

  ![Master schedule](screenshots/master_schedule.jpg)  
  ![master_booking_detail](screenshots/master_booking_detail.jpg)  
  ![Admin analytics](screenshots/admin_stats.jpg)  
  ![Admin panel](screenshots/admin_panel.jpg)  
  ![Admin settings](screenshots/admin_settings.jpg)  
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
</details>

---

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

## Features
Smart Booking Engine: Automatically finds gaps in schedules, supports multi-service bookings, and respects individual master speeds.

Double-Booking Protection: Uses PostgreSQL Advisory Locks to guarantee data integrity during concurrent requests.

Role-Based Access: Separate interfaces for Clients (booking), Masters (schedule management), and Admins (analytics & config).

Analytics Dashboard: Tracks LTV, Retention Rate, and No-Show statistics in real-time.

FSM & Navigation: Robust state machine with a navigation stack (Back button actually works correctly).

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