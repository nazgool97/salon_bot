# Deployment Guide

## Quick Start (Docker Compose)

```bash
# 1) Clone
git clone https://github.com/your/repo.git
cd repo

# 2) Configure
cp .env.example .env

# 3) Run
docker-compose up -d
```

- Services: bot, PostgreSQL, workers.
- Inspect logs to verify FSM, bookings, reminders.
- Adjust `.env` for timezone, currency, policies, payment keys.

## Environment Configuration
- `BOT_TOKEN`: Telegram bot token.
- `DATABASE_URL`: postgres connection string.
- `BUSINESS_TZ`: canonical timezone (e.g., Europe/Berlin).
- `DEFAULT_CURRENCY`: e.g., EUR.
- `PAYMENT_PROVIDER_KEY`: Telegram Payments provider key.
- `CANCELLATION_LOCK_HOURS`: integer hours.
- `HOLD_TIMEOUT_MINUTES`: integer minutes.

## Migrations
- Alembic manages schema.
- Run before each deploy: `alembic upgrade head` (or via your entrypoint).
- Never skip migrations between versions.

## Production Notes
- Keep all timestamps in UTC internally; render local time in handlers.
- Monitor worker health (reminders, hold cleanup).
- Back up PostgreSQL regularly; test restores.

## Security
- Principle of least privilege for DB user.
- Protect secrets; do not log payment payloads.
- HTTPS for webhooks if used; otherwise long polling is acceptable for dev.

## Scaling
- Handlers are stateless; you can run multiple replicas.
- Workers are idempotent; safe to run multiple replicas.
- Advisory locks keep booking writes safe under concurrency.

## Troubleshooting
- Booking not confirmed: check payment status and hold timeout worker.
- Duplicate reminders: verify idempotency keys and worker logs.
- Wrong time: verify `BUSINESS_TZ` and rendering layer.
- Admin access issues: check role assignment in services.
