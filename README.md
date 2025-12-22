# Telegram Salon Booking Bot (Open Core)

Enterprise-grade CRM bot for salons, barbershops, and studios built with Clean Architecture. Clients self-book, masters manage schedules, owners get analytics — all inside Telegram. Open-core; commercial edition adds support and updates.

👉 Commercial edition: https://ko-fi.com/s/937c0881d1

---

## Screenshots / GIFs
- Client booking flow (inline calendar, time picker) — _add gif_
- Admin fast-lookup and analytics — _add gif_

## Tech Stack
- Python 3.11+
- Aiogram 3.x (FSM, Routers)
- SQLAlchemy + Alembic (asyncpg)
- PostgreSQL with advisory locks
- Docker & Docker Compose
- Pydantic for config/validation
- Structured logging

## Key Features
- Clean Architecture & DDD-inspired layers (handlers, services, domain, repos, workers).
- Concurrency-safe bookings via `pg_advisory_xact_lock` (no double bookings).
- Smart scheduling: gap search, composite services, per-master durations, UTC-first with local render.
- Professional FSM + navigation stack: real Back behavior with preserved context.
- Payments & policies: holds during pay, cancellation/reschedule windows, Telegram Payments.
- Roles: Owner/Admin, Master, Client with clear permissions.
- Analytics: LTV, retention, no-show rate, revenue (real vs expected), CSV export.
- Background workers: reminders, hold cleanup, payment reconcile, notifications.

## Quick Start (2 minutes)

```bash
# 1) Clone
git clone https://github.com/your/repo.git
cd repo

# 2) Configure
cp .env.example .env

# 3) Run
docker-compose up -d
```

Then talk to the bot with your token; edit `.env` for timezone, currency, policies, payment keys.

## Editions

| Capability | Community (Open Core) | Commercial |
| --- | --- | --- |
| Booking engine, FSM, navigation | ✅ | ✅ |
| Admin/Master/Client roles | ✅ | ✅ |
| Analytics (LTV, retention, no-show, revenue) | ✅ | ✅ |
| Payments & holds | ✅ | ✅ |
| Docker + Alembic + PostgreSQL | ✅ | ✅ |
| Support & updates | — | ✅ |
| Production license | — | ✅ |

## Documentation
- Architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Deployment: [docs/DEPLOY.md](docs/DEPLOY.md)
- Developer Manual (deep dive): [docs/DEVELOPER_MANUAL.md](docs/DEVELOPER_MANUAL.md)

## Roadmap (short)
- More payment providers and split payments.
- Multi-location policies.
- Advanced cohort analytics and churn signals.
- Inventory tracking for consumables.

## License & Support
- Open-core: free to explore, run locally, and learn.
- Commercial edition: production license, guided setup, updates, and support.
- Contact via the commercial link above for upgrades.

---

Built to be readable, testable, and safe under load: Clean Architecture, advisory locks, UTC everywhere, Alembic migrations, workers, and structured logging out of the box.
